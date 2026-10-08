use broker_ports::{
    RawFrameCapture, RawFrameCaptureKey, RawFrameDisposition, RawFrameFinalization,
    RawFrameSinkError, RawFrameWireEncoding,
};
use market_contracts::{EntitlementState, NumericEncodingV1};
use sha2::{Digest, Sha256};

const RECORD_VERSION: u8 = 1;
const PREDECODE_RECORD: u8 = 1;
const FINALIZATION_RECORD: u8 = 2;
const RECORD_HASH_DOMAIN: &[u8] = b"lqepoch.mdp.raw-frame-spool-record.v1\0";
const MAX_METADATA_STRING_BYTES: usize = 1024;

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

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::{RECORD_HASH_DOMAIN, frame_record};

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
        Some(NumericEncodingV1::Unspecified) => Err(RawFrameSinkError::Unavailable),
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
