//! Exact MessagePack-frame Parquet projection using the core-owned schema.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::Path,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, BinaryArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt32Array,
    UInt64Array,
};
use market_contracts::{
    EntitlementState, NumericEncodingV1, RawFrameDispositionV1, RawFrameStorageRecordV1,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{
    PARQUET_READ_BATCH_ROWS, ParquetVerification, arrow_schema, validate_arrow_schema,
    validate_decode_budget, validate_no_unexpected_nulls, validate_optional_schema_metadata,
};
use crate::{
    MarketDataError, Result, protocol::DEFAULT_MAX_JSONL_RECORDS, schema::RAW_FRAME_SCHEMA_ID,
};

use super::rows;

/// Bound for the exact raw payload bytes held by one immutable capture object.
pub const MAX_RAW_CAPTURE_BYTES: u64 = 16 * 1024 * 1024;
/// Bound for raw frame rows in one capture object.
pub const MAX_RAW_CAPTURE_FRAMES: usize = 1_024;

pub(super) fn write_frames(
    path: &Path,
    frames: &[RawFrameStorageRecordV1],
    max_object_bytes: u64,
) -> Result<ParquetVerification> {
    if frames.is_empty()
        || frames.len() > MAX_RAW_CAPTURE_FRAMES
        || frames.len() > DEFAULT_MAX_JSONL_RECORDS
    {
        return Err(MarketDataError::InputLimit);
    }
    let mut raw_bytes = 0_u64;
    let mut generation = 0_u64;
    let mut frame_sequence = 0_u64;
    for frame in frames {
        validate_raw_frame_source(frame)?;
        raw_bytes = raw_bytes
            .checked_add(
                u64::try_from(frame.frame_bytes.len()).map_err(|_| MarketDataError::InputLimit)?,
            )
            .ok_or(MarketDataError::InputLimit)?;
        if raw_bytes > MAX_RAW_CAPTURE_BYTES {
            return Err(MarketDataError::InputLimit);
        }
        if frame.generation < generation
            || frame.generation == generation && frame.frame_sequence <= frame_sequence
        {
            return Err(MarketDataError::Contract);
        }
        if frame.generation != generation {
            generation = frame.generation;
        }
        frame_sequence = frame.frame_sequence;
    }

    let schema = Arc::new(arrow_schema(RAW_FRAME_SCHEMA_ID)?);
    validate_arrow_schema(RAW_FRAME_SCHEMA_ID, &schema)?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(
            frames
                .iter()
                .map(|row| row.schema_version)
                .collect::<Vec<_>>(),
        )),
        super::string_array(frames.iter().map(|row| row.provider.as_str())),
        super::string_array(frames.iter().map(|row| row.feed.as_str())),
        super::owned_string_array(
            frames
                .iter()
                .map(|row| entitlement_label(row.entitlement).to_owned()),
        ),
        Arc::new(StringArray::from(
            frames
                .iter()
                .map(|row| row.source_numeric_encoding.map(NumericEncodingV1::as_str))
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            frames.iter().map(|row| row.generation).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            frames
                .iter()
                .map(|row| row.frame_sequence)
                .collect::<Vec<_>>(),
        )),
        super::timestamp_array(
            frames
                .iter()
                .map(|row| Some(row.received_timestamp_utc.as_str())),
        )?,
        super::string_array(frames.iter().map(|row| row.frame_sha256.as_str())),
        Arc::new(BinaryArray::from_iter_values(
            frames.iter().map(|row| row.frame_bytes.as_slice()),
        )),
        Arc::new(UInt32Array::from(
            frames.iter().map(|row| row.event_count).collect::<Vec<_>>(),
        )),
        super::owned_string_array(frames.iter().map(|row| row.disposition.as_str().to_owned())),
        super::string_array(frames.iter().map(|row| row.symbols_json.as_str())),
    ];
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|_| MarketDataError::Parquet)?;
    super::write_batch_atomic(path, schema, &batch, max_object_bytes, RAW_FRAME_SCHEMA_ID)?;
    super::verify_with_limit(path, RAW_FRAME_SCHEMA_ID, max_object_bytes)
}

pub(super) fn decode_frame_batch(batch: &RecordBatch) -> Result<Vec<RawFrameStorageRecordV1>> {
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
        let encoding = match rows::optional_string_value(strings("source_numeric_encoding")?, index)
        {
            Some(value) => Some(rows::parse_numeric_encoding(value)?),
            None => None,
        };
        let row = RawFrameStorageRecordV1 {
            schema_version: super::u32_value(u32s("schema_version")?, index)?,
            provider: super::value_string(strings("provider")?, index)?.to_owned(),
            feed: super::value_string(strings("feed")?, index)?.to_owned(),
            entitlement: rows::parse_entitlement(super::value_string(
                strings("entitlement")?,
                index,
            )?)?,
            source_numeric_encoding: encoding,
            generation: super::u64_value(u64s("generation")?, index)?,
            frame_sequence: super::u64_value(u64s("frame_sequence")?, index)?,
            received_timestamp_utc: super::timestamp_value(
                timestamps("received_timestamp_utc")?,
                index,
            )?,
            frame_sha256: super::value_string(strings("frame_sha256")?, index)?.to_owned(),
            frame_bytes: binary.value(index).to_vec(),
            event_count: super::u32_value(u32s("event_count")?, index)?,
            disposition: parse_disposition(super::value_string(strings("disposition")?, index)?)?,
            symbols_json: super::value_string(strings("symbols_json")?, index)?.to_owned(),
        };
        validate_raw_frame_source(&row)?;
        rows.push(row);
    }
    Ok(rows)
}

fn validate_raw_frame_source(frame: &RawFrameStorageRecordV1) -> Result<()> {
    frame.validate().map_err(|_| MarketDataError::Contract)?;
    let accepted = if frame.provider == "synthetic" {
        frame.feed == "synthetic" && frame.entitlement == EntitlementState::Unknown
    } else {
        frame.provider == "alpaca" && frame.feed == "opra"
    };
    if accepted {
        Ok(())
    } else {
        Err(MarketDataError::Contract)
    }
}

/// Read a small producer-owned raw capture object into memory for cross-object ref verification.
pub(super) fn read_capture_frames(
    path: &Path,
    max_object_bytes: u64,
) -> Result<Vec<RawFrameStorageRecordV1>> {
    let file_size = std::fs::metadata(path)?.len();
    if file_size == 0 || file_size > max_object_bytes {
        return Err(MarketDataError::InputLimit);
    }
    let file = File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|_| MarketDataError::Parquet)?;
    validate_decode_budget(builder.metadata().as_ref())?;
    validate_arrow_schema(RAW_FRAME_SCHEMA_ID, builder.schema().as_ref())?;
    validate_optional_schema_metadata(
        RAW_FRAME_SCHEMA_ID,
        builder.metadata().as_ref(),
        builder.schema(),
    )?;
    let rows = builder.metadata().file_metadata().num_rows();
    if rows <= 0 || rows as usize > MAX_RAW_CAPTURE_FRAMES {
        return Err(MarketDataError::InputLimit);
    }
    let mut reader = builder
        .with_batch_size(PARQUET_READ_BATCH_ROWS)
        .build()
        .map_err(|_| MarketDataError::Parquet)?;
    let mut frames = Vec::with_capacity(rows as usize);
    let mut bytes = 0_u64;
    for batch in &mut reader {
        for frame in decode_frame_batch(&batch.map_err(|_| MarketDataError::Parquet)?)? {
            bytes = bytes
                .checked_add(
                    u64::try_from(frame.frame_bytes.len())
                        .map_err(|_| MarketDataError::InputLimit)?,
                )
                .ok_or(MarketDataError::InputLimit)?;
            if bytes > MAX_RAW_CAPTURE_BYTES {
                return Err(MarketDataError::InputLimit);
            }
            frames.push(frame);
        }
    }
    if frames.len() != rows as usize {
        return Err(MarketDataError::Parquet);
    }
    Ok(frames)
}

pub(super) fn verify_event_v2_against_raw(
    raw_path: &Path,
    event_path: &Path,
    max_object_bytes: u64,
) -> Result<super::RawEventCorrelationVerification> {
    let raw_frames = read_capture_frames(raw_path, max_object_bytes)?;
    if raw_frames.is_empty() || raw_frames.len() > MAX_RAW_CAPTURE_FRAMES {
        return Err(MarketDataError::InputLimit);
    }
    let mut frame_indexes = BTreeMap::new();
    for (index, frame) in raw_frames.iter().enumerate() {
        if frame_indexes
            .insert((frame.generation, frame.frame_sequence), index)
            .is_some()
        {
            return Err(MarketDataError::Contract);
        }
    }
    let file_size = std::fs::metadata(event_path)?.len();
    if file_size == 0 || file_size > max_object_bytes {
        return Err(MarketDataError::InputLimit);
    }
    let file = File::open(event_path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|_| MarketDataError::Parquet)?;
    validate_decode_budget(builder.metadata().as_ref())?;
    validate_arrow_schema(super::EVENT_SCHEMA_V2_ID, builder.schema().as_ref())?;
    validate_optional_schema_metadata(
        super::EVENT_SCHEMA_V2_ID,
        builder.metadata().as_ref(),
        builder.schema(),
    )?;
    let footer_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| MarketDataError::Parquet)?;
    if footer_rows == 0 || footer_rows > DEFAULT_MAX_JSONL_RECORDS as u64 {
        return Err(MarketDataError::InputLimit);
    }
    let mut reader = builder
        .with_batch_size(PARQUET_READ_BATCH_ROWS)
        .build()
        .map_err(|_| MarketDataError::Parquet)?;
    let mut seen_ordinals = BTreeMap::<(u64, u64), BTreeSet<u32>>::new();
    let mut event_rows = 0_u64;
    for batch in &mut reader {
        for row in rows::decode_event_v2_batch(&batch.map_err(|_| MarketDataError::Parquet)?)? {
            let reference = row
                .raw_frame_reference
                .ok_or(MarketDataError::IncompleteWindow)?;
            let key = (reference.raw_frame_generation, reference.raw_frame_sequence);
            let frame_index = frame_indexes.get(&key).ok_or(MarketDataError::Contract)?;
            let frame = &raw_frames[*frame_index];
            row.validate_against_frame(frame)
                .map_err(|_| MarketDataError::Contract)?;
            if !seen_ordinals
                .entry(key)
                .or_default()
                .insert(reference.raw_frame_event_ordinal)
            {
                return Err(MarketDataError::Contract);
            }
            event_rows = event_rows
                .checked_add(1)
                .ok_or(MarketDataError::InputLimit)?;
        }
    }
    if event_rows != footer_rows
        || raw_frames.iter().any(|frame| {
            let key = (frame.generation, frame.frame_sequence);
            seen_ordinals.get(&key).map_or(0, BTreeSet::len) != frame.event_count as usize
        })
    {
        return Err(MarketDataError::IncompleteWindow);
    }
    Ok(super::RawEventCorrelationVerification {
        raw_frame_rows: u64::try_from(raw_frames.len()).map_err(|_| MarketDataError::InputLimit)?,
        event_rows,
    })
}

const fn entitlement_label(value: EntitlementState) -> &'static str {
    match value {
        EntitlementState::Unknown => "unknown",
        EntitlementState::Authorized => "authorized",
        EntitlementState::Unauthorized => "unauthorized",
    }
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
