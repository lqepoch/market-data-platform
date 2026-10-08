//! Capture-scoped MessagePack and JSON frame projections under Core-owned V2 schemas.

use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{
    ArrayRef, BinaryArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt32Array,
    UInt64Array,
};
use market_contracts::{
    EntitlementState, NumericEncodingV1, RawFrameCaptureInstanceIdV2, RawFrameDispositionV1,
    RawFrameStorageRecordV2, RawJsonFrameStorageRecordV2, UtcTimestamp,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{
    PARQUET_READ_BATCH_ROWS, ParquetVerification, arrow_schema, validate_arrow_schema,
    validate_decode_budget, validate_no_unexpected_nulls,
};
use crate::{
    MarketDataError, Result,
    protocol::DEFAULT_MAX_JSONL_RECORDS,
    schema::{RAW_FRAME_SCHEMA_V2_ID, RAW_JSON_FRAME_SCHEMA_V2_ID},
};

const MAX_CAPTURE_FRAMES: usize = market_contracts::MAX_RAW_CAPTURE_CHUNK_FRAMES_V2;
const MAX_CAPTURE_BYTES: usize = market_contracts::MAX_RAW_CAPTURE_CHUNK_BYTES_V2;

pub(super) fn write_messagepack_frames(
    path: &Path,
    frames: &[RawFrameStorageRecordV2],
    max_object_bytes: u64,
) -> Result<ParquetVerification> {
    if frames.is_empty() || frames.len() > MAX_CAPTURE_FRAMES {
        return Err(MarketDataError::InputLimit);
    }
    let rows = frames
        .iter()
        .map(|frame| {
            frame.validate().map_err(|_| MarketDataError::Contract)?;
            Ok(RawFrameProjection::from_messagepack(frame))
        })
        .collect::<Result<Vec<_>>>()?;
    validate_frame_chunk(&rows)?;
    write_rows(path, &rows, RAW_FRAME_SCHEMA_V2_ID, max_object_bytes)
}

pub(super) fn write_json_frames(
    path: &Path,
    frames: &[RawJsonFrameStorageRecordV2],
    max_object_bytes: u64,
) -> Result<ParquetVerification> {
    if frames.is_empty() || frames.len() > MAX_CAPTURE_FRAMES {
        return Err(MarketDataError::InputLimit);
    }
    let rows = frames
        .iter()
        .map(|frame| {
            frame.validate().map_err(|_| MarketDataError::Contract)?;
            Ok(RawFrameProjection::from_json(frame))
        })
        .collect::<Result<Vec<_>>>()?;
    validate_frame_chunk(&rows)?;
    write_rows(path, &rows, RAW_JSON_FRAME_SCHEMA_V2_ID, max_object_bytes)
}

pub(super) fn decode_messagepack_batch(
    batch: &RecordBatch,
) -> Result<Vec<RawFrameStorageRecordV2>> {
    decode_common_batch(batch)?
        .into_iter()
        .map(|row| {
            let decoded = RawFrameStorageRecordV2 {
                schema_version: row.schema_version,
                provider: row.provider,
                feed: row.feed,
                entitlement: row.entitlement,
                source_numeric_encoding: row.source_numeric_encoding,
                capture_instance_id: row.capture_instance_id,
                source_generation: row.source_generation,
                source_frame_sequence: row.source_frame_sequence,
                canonical_generation: row.canonical_generation,
                received_timestamp_utc: row.received_timestamp_utc,
                frame_sha256: row.frame_sha256,
                frame_bytes: row.frame_bytes,
                event_count: row.event_count,
                disposition: row.disposition,
                symbols_json: row.symbols_json,
            };
            decoded.validate().map_err(|_| MarketDataError::Contract)?;
            Ok(decoded)
        })
        .collect()
}

pub(super) fn decode_json_batch(batch: &RecordBatch) -> Result<Vec<RawJsonFrameStorageRecordV2>> {
    decode_common_batch(batch)?
        .into_iter()
        .map(|row| {
            let decoded = RawJsonFrameStorageRecordV2 {
                schema_version: row.schema_version,
                provider: row.provider,
                feed: row.feed,
                entitlement: row.entitlement,
                source_numeric_encoding: row.source_numeric_encoding,
                capture_instance_id: row.capture_instance_id,
                source_generation: row.source_generation,
                source_frame_sequence: row.source_frame_sequence,
                canonical_generation: row.canonical_generation,
                received_timestamp_utc: row.received_timestamp_utc,
                frame_sha256: row.frame_sha256,
                frame_bytes: row.frame_bytes,
                event_count: row.event_count,
                disposition: row.disposition,
                symbols_json: row.symbols_json,
            };
            decoded.validate().map_err(|_| MarketDataError::Contract)?;
            Ok(decoded)
        })
        .collect()
}

pub(super) fn read_messagepack_frames(
    path: &Path,
    max_object_bytes: u64,
) -> Result<Vec<RawFrameStorageRecordV2>> {
    let frames = read_all_batches(
        path,
        RAW_FRAME_SCHEMA_V2_ID,
        max_object_bytes,
        decode_messagepack_batch,
    )?;
    let projections = frames
        .iter()
        .map(RawFrameProjection::from_messagepack)
        .collect::<Vec<_>>();
    validate_frame_chunk(&projections)?;
    Ok(frames)
}

pub(super) fn read_json_frames(
    path: &Path,
    max_object_bytes: u64,
) -> Result<Vec<RawJsonFrameStorageRecordV2>> {
    let frames = read_all_batches(
        path,
        RAW_JSON_FRAME_SCHEMA_V2_ID,
        max_object_bytes,
        decode_json_batch,
    )?;
    let projections = frames
        .iter()
        .map(RawFrameProjection::from_json)
        .collect::<Vec<_>>();
    validate_frame_chunk(&projections)?;
    Ok(frames)
}

fn read_all_batches<T>(
    path: &Path,
    schema_id: &str,
    max_object_bytes: u64,
    decode: fn(&RecordBatch) -> Result<Vec<T>>,
) -> Result<Vec<T>> {
    if File::open(path)?.metadata()?.len() > max_object_bytes {
        return Err(MarketDataError::InputLimit);
    }
    let file = File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|_| MarketDataError::Parquet)?;
    validate_decode_budget(builder.metadata().as_ref())?;
    validate_arrow_schema(schema_id, builder.schema().as_ref())?;
    super::validate_optional_schema_metadata(
        schema_id,
        builder.metadata().as_ref(),
        builder.schema(),
    )?;
    let row_count = builder.metadata().file_metadata().num_rows();
    if row_count < 0 || row_count as usize > MAX_CAPTURE_FRAMES {
        return Err(MarketDataError::InputLimit);
    }
    let mut reader = builder
        .with_batch_size(PARQUET_READ_BATCH_ROWS)
        .build()
        .map_err(|_| MarketDataError::Parquet)?;
    let mut rows = Vec::with_capacity(row_count as usize);
    for batch in &mut reader {
        rows.extend(decode(&batch.map_err(|_| MarketDataError::Parquet)?)?);
    }
    Ok(rows)
}

struct RawFrameProjection<'a> {
    schema_version: u32,
    provider: &'a str,
    feed: &'a str,
    entitlement: EntitlementState,
    source_numeric_encoding: Option<NumericEncodingV1>,
    capture_instance_id: &'a str,
    source_generation: u64,
    source_frame_sequence: u64,
    canonical_generation: u64,
    received_timestamp_utc: &'a UtcTimestamp,
    frame_sha256: &'a str,
    frame_bytes: &'a [u8],
    event_count: u32,
    disposition: RawFrameDispositionV1,
    symbols_json: &'a str,
}

impl<'a> RawFrameProjection<'a> {
    fn from_messagepack(frame: &'a RawFrameStorageRecordV2) -> Self {
        Self {
            schema_version: frame.schema_version,
            provider: &frame.provider,
            feed: &frame.feed,
            entitlement: frame.entitlement,
            source_numeric_encoding: frame.source_numeric_encoding,
            capture_instance_id: frame.capture_instance_id.as_str(),
            source_generation: frame.source_generation,
            source_frame_sequence: frame.source_frame_sequence,
            canonical_generation: frame.canonical_generation,
            received_timestamp_utc: &frame.received_timestamp_utc,
            frame_sha256: &frame.frame_sha256,
            frame_bytes: &frame.frame_bytes,
            event_count: frame.event_count,
            disposition: frame.disposition,
            symbols_json: &frame.symbols_json,
        }
    }

    fn from_json(frame: &'a RawJsonFrameStorageRecordV2) -> Self {
        Self {
            schema_version: frame.schema_version,
            provider: &frame.provider,
            feed: &frame.feed,
            entitlement: frame.entitlement,
            source_numeric_encoding: frame.source_numeric_encoding,
            capture_instance_id: frame.capture_instance_id.as_str(),
            source_generation: frame.source_generation,
            source_frame_sequence: frame.source_frame_sequence,
            canonical_generation: frame.canonical_generation,
            received_timestamp_utc: &frame.received_timestamp_utc,
            frame_sha256: &frame.frame_sha256,
            frame_bytes: &frame.frame_bytes,
            event_count: frame.event_count,
            disposition: frame.disposition,
            symbols_json: &frame.symbols_json,
        }
    }
}

fn validate_frame_chunk(rows: &[RawFrameProjection<'_>]) -> Result<()> {
    if rows.is_empty() || rows.len() > MAX_CAPTURE_FRAMES {
        return Err(MarketDataError::InputLimit);
    }
    let mut bytes = 0_usize;
    let first = &rows[0];
    for (index, row) in rows.iter().enumerate() {
        if row.capture_instance_id != first.capture_instance_id
            || row.source_generation != first.source_generation
            || row.provider != first.provider
            || row.feed != first.feed
            || row.entitlement != first.entitlement
            || row.canonical_generation != first.canonical_generation
            || Some(row.source_frame_sequence)
                != first.source_frame_sequence.checked_add(index as u64)
        {
            return Err(MarketDataError::Contract);
        }
        bytes = bytes
            .checked_add(row.frame_bytes.len())
            .ok_or(MarketDataError::InputLimit)?;
        if bytes > MAX_CAPTURE_BYTES {
            return Err(MarketDataError::InputLimit);
        }
    }
    Ok(())
}

fn write_rows(
    path: &Path,
    rows: &[RawFrameProjection<'_>],
    schema_id: &str,
    max_object_bytes: u64,
) -> Result<ParquetVerification> {
    if rows.len() > DEFAULT_MAX_JSONL_RECORDS {
        return Err(MarketDataError::InputLimit);
    }
    let schema = Arc::new(arrow_schema(schema_id)?);
    validate_arrow_schema(schema_id, &schema)?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(
            rows.iter()
                .map(|row| row.schema_version)
                .collect::<Vec<_>>(),
        )),
        super::string_array(rows.iter().map(|row| row.provider)),
        super::string_array(rows.iter().map(|row| row.feed)),
        super::owned_string_array(rows.iter().map(|row| super::enum_string(&row.entitlement))),
        Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.source_numeric_encoding.map(NumericEncodingV1::as_str))
                .collect::<Vec<_>>(),
        )),
        super::string_array(rows.iter().map(|row| row.capture_instance_id)),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.source_generation)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.source_frame_sequence)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            rows.iter()
                .map(|row| row.canonical_generation)
                .collect::<Vec<_>>(),
        )),
        super::timestamp_array(
            rows.iter()
                .map(|row| Some(row.received_timestamp_utc.as_str())),
        )?,
        super::string_array(rows.iter().map(|row| row.frame_sha256)),
        Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|row| row.frame_bytes),
        )),
        Arc::new(UInt32Array::from(
            rows.iter().map(|row| row.event_count).collect::<Vec<_>>(),
        )),
        super::owned_string_array(rows.iter().map(|row| row.disposition.as_str().to_owned())),
        super::string_array(rows.iter().map(|row| row.symbols_json)),
    ];
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|_| MarketDataError::Parquet)?;
    super::write_batch_atomic(path, schema, &batch, max_object_bytes, schema_id)?;
    super::verify_with_limit(path, schema_id, max_object_bytes)
}

struct DecodedRawFrameV2 {
    schema_version: u32,
    provider: String,
    feed: String,
    entitlement: EntitlementState,
    source_numeric_encoding: Option<NumericEncodingV1>,
    capture_instance_id: RawFrameCaptureInstanceIdV2,
    source_generation: u64,
    source_frame_sequence: u64,
    canonical_generation: u64,
    received_timestamp_utc: UtcTimestamp,
    frame_sha256: String,
    frame_bytes: Vec<u8>,
    event_count: u32,
    disposition: RawFrameDispositionV1,
    symbols_json: String,
}

fn decode_common_batch(batch: &RecordBatch) -> Result<Vec<DecodedRawFrameV2>> {
    validate_no_unexpected_nulls(batch)?;
    let col = |name: &str| -> Result<usize> {
        batch
            .schema()
            .index_of(name)
            .map_err(|_| MarketDataError::Parquet)
    };
    let strings = |name: &str| -> Result<&StringArray> { super::string_column(batch, col(name)?) };
    let u32s = |name: &str| -> Result<&UInt32Array> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let u64s = |name: &str| -> Result<&UInt64Array> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let timestamps = |name: &str| -> Result<&TimestampNanosecondArray> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let binary = batch
        .column(col("frame_bytes")?)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or(MarketDataError::Parquet)?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        let source_numeric_encoding =
            match super::rows::optional_string_value(strings("source_numeric_encoding")?, index) {
                Some(value) => Some(super::rows::parse_numeric_encoding(value)?),
                None => None,
            };
        rows.push(DecodedRawFrameV2 {
            schema_version: super::u32_value(u32s("schema_version")?, index)?,
            provider: super::value_string(strings("provider")?, index)?.to_owned(),
            feed: super::value_string(strings("feed")?, index)?.to_owned(),
            entitlement: super::rows::parse_entitlement(super::value_string(
                strings("entitlement")?,
                index,
            )?)?,
            source_numeric_encoding,
            capture_instance_id: RawFrameCaptureInstanceIdV2::parse(
                super::value_string(strings("capture_instance_id")?, index)?.to_owned(),
            )
            .map_err(|_| MarketDataError::Contract)?,
            source_generation: super::u64_value(u64s("source_generation")?, index)?,
            source_frame_sequence: super::u64_value(u64s("source_frame_sequence")?, index)?,
            canonical_generation: super::u64_value(u64s("canonical_generation")?, index)?,
            received_timestamp_utc: super::timestamp_value(
                timestamps("received_timestamp_utc")?,
                index,
            )?,
            frame_sha256: super::value_string(strings("frame_sha256")?, index)?.to_owned(),
            frame_bytes: binary.value(index).to_vec(),
            event_count: super::u32_value(u32s("event_count")?, index)?,
            disposition: parse_disposition(super::value_string(strings("disposition")?, index)?)?,
            symbols_json: super::value_string(strings("symbols_json")?, index)?.to_owned(),
        });
    }
    Ok(rows)
}

fn parse_disposition(value: &str) -> Result<RawFrameDispositionV1> {
    match value {
        "market_data" => Ok(RawFrameDispositionV1::MarketData),
        "control" => Ok(RawFrameDispositionV1::Control),
        "unknown_message" => Ok(RawFrameDispositionV1::UnknownMessage),
        "malformed_message" => Ok(RawFrameDispositionV1::MalformedMessage),
        "provider_error" => Ok(RawFrameDispositionV1::ProviderError),
        _ => Err(MarketDataError::Contract),
    }
}
