use std::{fs::File, io::Read};

use broker_ports::{
    MAX_RAW_FRAME_BYTES, MAX_RAW_FRAME_FINALIZATION_ITEMS, RawCaptureInstanceId, RawFrameCapture,
    RawFrameCaptureAck, RawFrameCaptureKey, RawFrameDisposition, RawFrameFinalization,
    RawFrameFinalizationAck, RawFramePayload, RawFrameSinkError, RawFrameWireEncoding,
};
use market_contracts::{EntitlementState, NumericEncodingV1, UtcTimestamp};
use sha2::{Digest, Sha256};

const RECORD_VERSION: u8 = 1;
const PREDECODE_RECORD: u8 = 1;
const FINALIZATION_RECORD: u8 = 2;
const RECORD_HASH_DOMAIN: &[u8] = b"lqepoch.mdp.raw-frame-spool-record.v1\0";
const MAX_METADATA_STRING_BYTES: usize = 1024;
const RECORD_CHECKSUM_BYTES: usize = 32;

pub(super) struct FinalizedCaptureReader {
    file: File,
    capture_id: RawCaptureInstanceId,
    remaining_bytes: u64,
    last_generation: Option<u64>,
    last_sequence: Option<u64>,
}

impl FinalizedCaptureReader {
    pub(super) fn new(
        mut file: File,
        capture_id: RawCaptureInstanceId,
        snapshot_bytes: u64,
    ) -> Result<Self, RawFrameSinkError> {
        if snapshot_bytes < super::FRAME_LOG_MAGIC.len() as u64 {
            return Err(RawFrameSinkError::Unavailable);
        }
        let mut magic = [0; 8];
        file.read_exact(&mut magic)
            .map_err(|_| RawFrameSinkError::Ambiguous)?;
        if &magic != super::FRAME_LOG_MAGIC {
            return Err(RawFrameSinkError::Unavailable);
        }
        Ok(Self {
            file,
            capture_id,
            remaining_bytes: snapshot_bytes - super::FRAME_LOG_MAGIC.len() as u64,
            last_generation: None,
            last_sequence: None,
        })
    }

    pub(super) fn next_frame(
        &mut self,
    ) -> Result<Option<super::super::SpooledRawFrame>, RawFrameSinkError> {
        let Some(predecode_body) = self.read_record()? else {
            return Ok(None);
        };
        let capture = decode_predecode_record(&predecode_body, self.capture_id)?;
        let Some(finalization_body) = self.read_record()? else {
            return Err(RawFrameSinkError::Ambiguous);
        };
        let (finalization, summary_sha256) =
            decode_finalization_record(&finalization_body, &capture)?;
        let key = capture.capture_key();
        let is_next = match (self.last_generation, self.last_sequence) {
            (None, None) => key.frame_sequence() == 1,
            (Some(generation), Some(sequence)) if key.source_generation() == generation => {
                sequence.checked_add(1) == Some(key.frame_sequence())
            }
            (Some(generation), Some(_)) if key.source_generation() > generation => {
                key.frame_sequence() == 1
            }
            _ => false,
        };
        if !is_next {
            return Err(RawFrameSinkError::Ambiguous);
        }
        self.last_generation = Some(key.source_generation());
        self.last_sequence = Some(key.frame_sequence());
        Ok(Some(super::super::SpooledRawFrame {
            capture,
            finalization,
            finalization_summary_sha256: summary_sha256,
        }))
    }

    fn read_record(&mut self) -> Result<Option<Vec<u8>>, RawFrameSinkError> {
        if self.remaining_bytes == 0 {
            return Ok(None);
        }
        if self.remaining_bytes < 4 {
            return Err(RawFrameSinkError::Ambiguous);
        }
        let mut prefix = [0; 4];
        self.file
            .read_exact(&mut prefix)
            .map_err(|_| RawFrameSinkError::Ambiguous)?;
        self.remaining_bytes -= 4;
        let record_len = usize::try_from(u32::from_be_bytes(prefix))
            .map_err(|_| RawFrameSinkError::CapacityExceeded)?;
        let maximum_record_len = MAX_RAW_FRAME_BYTES
            .checked_add(8 * MAX_METADATA_STRING_BYTES)
            .and_then(|value| value.checked_add(4096))
            .ok_or(RawFrameSinkError::CapacityExceeded)?;
        if record_len < RECORD_CHECKSUM_BYTES + 2 || record_len > maximum_record_len {
            return Err(RawFrameSinkError::CapacityExceeded);
        }
        let record_len_u64 =
            u64::try_from(record_len).map_err(|_| RawFrameSinkError::CapacityExceeded)?;
        if record_len_u64 > self.remaining_bytes {
            return Err(RawFrameSinkError::Ambiguous);
        }
        let mut record = vec![0; record_len];
        self.file
            .read_exact(&mut record)
            .map_err(|_| RawFrameSinkError::Ambiguous)?;
        self.remaining_bytes -= record_len_u64;

        let body_len = record_len - RECORD_CHECKSUM_BYTES;
        let (body, checksum) = record.split_at(body_len);
        let mut hasher = Sha256::new();
        hasher.update(RECORD_HASH_DOMAIN);
        hasher.update(prefix);
        hasher.update(body);
        if checksum != hasher.finalize().as_slice() {
            return Err(RawFrameSinkError::Ambiguous);
        }
        record.truncate(body_len);
        Ok(Some(record))
    }
}

fn decode_predecode_record(
    body: &[u8],
    expected_capture_id: RawCaptureInstanceId,
) -> Result<RawFrameCapture, RawFrameSinkError> {
    let mut cursor = RecordCursor::new(body);
    if cursor.u8()? != RECORD_VERSION || cursor.u8()? != PREDECODE_RECORD {
        return Err(RawFrameSinkError::Unavailable);
    }
    let capture_id = decode_capture_id(cursor.take(16)?)?;
    if capture_id != expected_capture_id {
        return Err(RawFrameSinkError::Unavailable);
    }
    let source_generation = cursor.u64()?;
    let frame_sequence = cursor.u64()?;
    let expected_sha256 = hex::encode(cursor.take(32)?);
    let provider = cursor.string_u16()?.to_owned();
    let feed = cursor.string_u16()?.to_owned();
    let entitlement = decode_entitlement(cursor.u8()?)?;
    let received_timestamp_utc =
        UtcTimestamp::parse(cursor.string_u16()?).map_err(|_| RawFrameSinkError::Unavailable)?;
    let wire_encoding = decode_wire_encoding(cursor.u8()?)?;
    let payload_len =
        usize::try_from(cursor.u32()?).map_err(|_| RawFrameSinkError::CapacityExceeded)?;
    if payload_len == 0 || payload_len > MAX_RAW_FRAME_BYTES {
        return Err(RawFrameSinkError::CapacityExceeded);
    }
    let payload = RawFramePayload::capture(cursor.take(payload_len)?.to_vec())
        .map_err(|_| RawFrameSinkError::CapacityExceeded)?;
    cursor.finish()?;
    let capture = RawFrameCapture::new(
        capture_id,
        provider,
        feed,
        entitlement,
        source_generation,
        frame_sequence,
        received_timestamp_utc,
        wire_encoding,
        payload,
    )
    .map_err(|_| RawFrameSinkError::Unavailable)?;
    if capture.capture_key().frame_sha256() != expected_sha256 {
        return Err(RawFrameSinkError::Ambiguous);
    }
    Ok(capture)
}

fn decode_finalization_record(
    body: &[u8],
    capture: &RawFrameCapture,
) -> Result<(RawFrameFinalization, String), RawFrameSinkError> {
    let mut cursor = RecordCursor::new(body);
    if cursor.u8()? != RECORD_VERSION || cursor.u8()? != FINALIZATION_RECORD {
        return Err(RawFrameSinkError::Unavailable);
    }
    let capture_id = decode_capture_id(cursor.take(16)?)?;
    let source_generation = cursor.u64()?;
    let frame_sequence = cursor.u64()?;
    let frame_sha256 = hex::encode(cursor.take(32)?);
    let summary_sha256 = hex::encode(cursor.take(32)?);
    let expected_key = capture.capture_key();
    if capture_id != expected_key.capture_instance_id()
        || source_generation != expected_key.source_generation()
        || frame_sequence != expected_key.frame_sequence()
        || frame_sha256 != expected_key.frame_sha256()
        || !valid_sha256(&summary_sha256)
    {
        return Err(RawFrameSinkError::Ambiguous);
    }
    let event_count = cursor.u32()?;
    let disposition = decode_disposition(cursor.u8()?)?;
    let numeric_encoding = decode_numeric_encoding(cursor.u8()?)?;
    let symbol_count = usize::from(cursor.u16()?);
    if symbol_count > MAX_RAW_FRAME_FINALIZATION_ITEMS {
        return Err(RawFrameSinkError::CapacityExceeded);
    }
    let mut symbols = Vec::with_capacity(symbol_count);
    for _ in 0..symbol_count {
        symbols.push(cursor.string_u16()?.to_owned());
    }
    cursor.finish()?;
    let finalization =
        RawFrameFinalization::new(event_count, symbols, numeric_encoding, disposition)
            .map_err(|_| RawFrameSinkError::Unavailable)?;
    let predecode_ack = RawFrameCaptureAck::for_capture(capture);
    let finalization_ack = RawFrameFinalizationAck::for_finalization(&predecode_ack, &finalization);
    if finalization_ack.summary_sha256() != summary_sha256 {
        return Err(RawFrameSinkError::Ambiguous);
    }
    Ok((finalization, summary_sha256))
}

struct RecordCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> RecordCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], RawFrameSinkError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(RawFrameSinkError::CapacityExceeded)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(RawFrameSinkError::Ambiguous)?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, RawFrameSinkError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, RawFrameSinkError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| RawFrameSinkError::Ambiguous)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, RawFrameSinkError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| RawFrameSinkError::Ambiguous)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, RawFrameSinkError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| RawFrameSinkError::Ambiguous)?,
        ))
    }

    fn string_u16(&mut self) -> Result<&'a str, RawFrameSinkError> {
        let length = usize::from(self.u16()?);
        if length > MAX_METADATA_STRING_BYTES {
            return Err(RawFrameSinkError::CapacityExceeded);
        }
        std::str::from_utf8(self.take(length)?).map_err(|_| RawFrameSinkError::Unavailable)
    }

    fn finish(self) -> Result<(), RawFrameSinkError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(RawFrameSinkError::Unavailable)
        }
    }
}

fn decode_capture_id(bytes: &[u8]) -> Result<RawCaptureInstanceId, RawFrameSinkError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| RawFrameSinkError::Unavailable)?;
    RawCaptureInstanceId::new(bytes).map_err(|_| RawFrameSinkError::Unavailable)
}

fn decode_entitlement(value: u8) -> Result<EntitlementState, RawFrameSinkError> {
    match value {
        0 => Ok(EntitlementState::Unknown),
        1 => Ok(EntitlementState::Authorized),
        2 => Ok(EntitlementState::Unauthorized),
        _ => Err(RawFrameSinkError::Unavailable),
    }
}

fn decode_wire_encoding(value: u8) -> Result<RawFrameWireEncoding, RawFrameSinkError> {
    match value {
        1 => Ok(RawFrameWireEncoding::Json),
        2 => Ok(RawFrameWireEncoding::MessagePack),
        3 => Ok(RawFrameWireEncoding::Unknown),
        _ => Err(RawFrameSinkError::Unavailable),
    }
}

fn decode_disposition(value: u8) -> Result<RawFrameDisposition, RawFrameSinkError> {
    match value {
        1 => Ok(RawFrameDisposition::DecodedMarketData),
        2 => Ok(RawFrameDisposition::ControlMessage),
        3 => Ok(RawFrameDisposition::UnknownMessage),
        4 => Ok(RawFrameDisposition::ProviderError),
        5 => Ok(RawFrameDisposition::DecodeFailure),
        _ => Err(RawFrameSinkError::Unavailable),
    }
}

fn decode_numeric_encoding(value: u8) -> Result<Option<NumericEncodingV1>, RawFrameSinkError> {
    match value {
        0 => Ok(None),
        1 => Ok(Some(NumericEncodingV1::DecimalToken)),
        2 => Ok(Some(NumericEncodingV1::IntegerToken)),
        3 => Ok(Some(NumericEncodingV1::BinaryFloat64ShortestDecimal)),
        4 => Ok(Some(NumericEncodingV1::BinaryFloat32ShortestDecimal)),
        5 => Ok(Some(NumericEncodingV1::RawMessagePackBytes)),
        6 => Ok(Some(NumericEncodingV1::RawJsonBytes)),
        _ => Err(RawFrameSinkError::Unavailable),
    }
}

pub(super) fn encode_predecode_record(
    capture: &RawFrameCapture,
) -> Result<Vec<u8>, RawFrameSinkError> {
    let key = capture.capture_key();
    let sha = hex::decode(key.frame_sha256()).map_err(|_| RawFrameSinkError::Unavailable)?;
    if sha.len() != 32 || capture.payload().sha256() != key.frame_sha256() {
        return Err(RawFrameSinkError::Unavailable);
    }
    let mut body = Vec::with_capacity(capture.payload().as_bytes().len().saturating_add(512));
    body.extend_from_slice(&[RECORD_VERSION, PREDECODE_RECORD]);
    append_key(&mut body, key, &sha)?;
    append_string_u16(&mut body, capture.provider())?;
    append_string_u16(&mut body, capture.feed())?;
    body.push(entitlement_tag(capture.entitlement()));
    append_string_u16(&mut body, capture.received_timestamp_utc().as_str())?;
    body.push(wire_encoding_tag(capture.wire_encoding())?);
    let payload = capture.payload().as_bytes();
    body.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|_| RawFrameSinkError::CapacityExceeded)?
            .to_be_bytes(),
    );
    body.extend_from_slice(payload);
    frame_record(body)
}

pub(super) fn encode_finalization_record(
    key: &RawFrameCaptureKey,
    summary: &RawFrameFinalization,
    summary_hash: &str,
) -> Result<Vec<u8>, RawFrameSinkError> {
    let sha = hex::decode(key.frame_sha256()).map_err(|_| RawFrameSinkError::Unavailable)?;
    let summary_sha = hex::decode(summary_hash).map_err(|_| RawFrameSinkError::Unavailable)?;
    if sha.len() != 32 || summary_sha.len() != 32 || !valid_sha256(summary_hash) {
        return Err(RawFrameSinkError::Unavailable);
    }
    let mut body = Vec::with_capacity(256 + summary.symbols().len().saturating_mul(16));
    body.extend_from_slice(&[RECORD_VERSION, FINALIZATION_RECORD]);
    append_key(&mut body, key, &sha)?;
    body.extend_from_slice(&summary_sha);
    body.extend_from_slice(&summary.event_count().to_be_bytes());
    body.push(disposition_tag(summary.disposition()));
    body.push(numeric_encoding_tag(summary.numeric_encoding())?);
    body.extend_from_slice(
        &u16::try_from(summary.symbols().len())
            .map_err(|_| RawFrameSinkError::CapacityExceeded)?
            .to_be_bytes(),
    );
    for symbol in summary.symbols() {
        append_string_u16(&mut body, symbol)?;
    }
    frame_record(body)
}

fn append_key(
    body: &mut Vec<u8>,
    key: &RawFrameCaptureKey,
    decoded_sha: &[u8],
) -> Result<(), RawFrameSinkError> {
    body.extend_from_slice(key.capture_instance_id().as_bytes());
    body.extend_from_slice(&key.source_generation().to_be_bytes());
    body.extend_from_slice(&key.frame_sequence().to_be_bytes());
    if decoded_sha.len() != 32 {
        return Err(RawFrameSinkError::Unavailable);
    }
    body.extend_from_slice(decoded_sha);
    Ok(())
}

fn append_string_u16(body: &mut Vec<u8>, value: &str) -> Result<(), RawFrameSinkError> {
    if value.len() > MAX_METADATA_STRING_BYTES {
        return Err(RawFrameSinkError::CapacityExceeded);
    }
    let length = u16::try_from(value.len()).map_err(|_| RawFrameSinkError::CapacityExceeded)?;
    body.extend_from_slice(&length.to_be_bytes());
    body.extend_from_slice(value.as_bytes());
    Ok(())
}

fn frame_record(mut body: Vec<u8>) -> Result<Vec<u8>, RawFrameSinkError> {
    let record_body_length = body
        .len()
        .checked_add(32)
        .ok_or(RawFrameSinkError::CapacityExceeded)?;
    let length =
        u32::try_from(record_body_length).map_err(|_| RawFrameSinkError::CapacityExceeded)?;
    let length_prefix = length.to_be_bytes();
    let mut hasher = Sha256::new();
    hasher.update(RECORD_HASH_DOMAIN);
    hasher.update(length_prefix);
    hasher.update(&body);
    body.extend_from_slice(&hasher.finalize());
    let mut record = Vec::with_capacity(body.len().saturating_add(length_prefix.len()));
    record.extend_from_slice(&length_prefix);
    record.extend_from_slice(&body);
    Ok(record)
}

fn entitlement_tag(value: EntitlementState) -> u8 {
    match value {
        EntitlementState::Unknown => 0,
        EntitlementState::Authorized => 1,
        EntitlementState::Unauthorized => 2,
    }
}

fn wire_encoding_tag(value: RawFrameWireEncoding) -> Result<u8, RawFrameSinkError> {
    match value {
        RawFrameWireEncoding::Json => Ok(1),
        RawFrameWireEncoding::MessagePack => Ok(2),
        RawFrameWireEncoding::Unknown => Ok(3),
        _ => Err(RawFrameSinkError::Unavailable),
    }
}

fn disposition_tag(value: RawFrameDisposition) -> u8 {
    match value {
        RawFrameDisposition::DecodedMarketData => 1,
        RawFrameDisposition::ControlMessage => 2,
        RawFrameDisposition::UnknownMessage => 3,
        RawFrameDisposition::ProviderError => 4,
        RawFrameDisposition::DecodeFailure => 5,
    }
}

fn numeric_encoding_tag(value: Option<NumericEncodingV1>) -> Result<u8, RawFrameSinkError> {
    match value {
        None => Ok(0),
        Some(NumericEncodingV1::DecimalToken) => Ok(1),
        Some(NumericEncodingV1::IntegerToken) => Ok(2),
        Some(NumericEncodingV1::BinaryFloat64ShortestDecimal) => Ok(3),
        Some(NumericEncodingV1::BinaryFloat32ShortestDecimal) => Ok(4),
        Some(NumericEncodingV1::RawMessagePackBytes) => Ok(5),
        Some(NumericEncodingV1::RawJsonBytes) => Ok(6),
        Some(NumericEncodingV1::Unspecified) => Err(RawFrameSinkError::Unavailable),
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use market_contracts::NumericEncodingV1;

    use super::{RECORD_HASH_DOMAIN, decode_numeric_encoding, frame_record, numeric_encoding_tag};

    #[test]
    fn numeric_encoding_tags_preserve_existing_values_and_add_raw_json() {
        let cases = [
            (None, 0),
            (Some(NumericEncodingV1::DecimalToken), 1),
            (Some(NumericEncodingV1::IntegerToken), 2),
            (Some(NumericEncodingV1::BinaryFloat64ShortestDecimal), 3),
            (Some(NumericEncodingV1::BinaryFloat32ShortestDecimal), 4),
            (Some(NumericEncodingV1::RawMessagePackBytes), 5),
            (Some(NumericEncodingV1::RawJsonBytes), 6),
        ];
        for (encoding, expected_tag) in cases {
            assert_eq!(numeric_encoding_tag(encoding), Ok(expected_tag));
            assert_eq!(decode_numeric_encoding(expected_tag), Ok(encoding));
        }
        assert!(numeric_encoding_tag(Some(NumericEncodingV1::Unspecified)).is_err());
        assert!(decode_numeric_encoding(7).is_err());
    }

    #[test]
    fn record_length_and_domain_separated_hash_cover_the_framing_prefix() {
        let record = frame_record(vec![1, 7, 9, 11]).expect("small test record is bounded");
        let declared_len = u32::from_be_bytes(
            record[..4]
                .try_into()
                .expect("record includes the fixed prefix"),
        ) as usize;
        assert_eq!(declared_len, record.len() - 4);

        let hash_start = record.len() - 32;
        let mut hasher = Sha256::new();
        hasher.update(RECORD_HASH_DOMAIN);
        hasher.update(&record[..4]);
        hasher.update(&record[4..hash_start]);
        assert_eq!(&record[hash_start..], &hasher.finalize()[..]);
    }
}
