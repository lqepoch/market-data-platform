//! Typed Arrow/Parquet event and completed-minute-bar storage.

use std::{
    fs::{self, File, OpenOptions},
    path::Path,
    sync::{Arc, atomic::AtomicBool},
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::SchemaRef;
use chrono::NaiveDate;
use exact_decimal::ExactDecimal;
use market_contracts::{
    DatasetTimeRangeV1, DecimalString, EntitlementState, EventMetadataV1, MarketDataSourceV1,
    MarketEventEnvelopeV1, MarketEventParquetRowV2, MarketEventParquetRowV3, MarketEventV1,
    NumericEncodingV1, RawFrameStorageRecordV1, RawFrameStorageRecordV2,
    RawJsonFrameStorageRecordV2, UtcTimestamp,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::{Compression, ZstdLevel},
    file::metadata::KeyValue,
};

use crate::{
    MarketDataError, Result,
    aggregate::TradeMinuteBarV1,
    aggregate::validate_stock_symbol,
    queue::CollectionMessage,
    schema::{
        EVENT_SCHEMA_ID, EVENT_SCHEMA_V2_ID, EVENT_SCHEMA_V3_ID, MINUTE_BAR_SCHEMA_ID,
        RAW_FRAME_SCHEMA_ID, RAW_FRAME_SCHEMA_V2_ID, RAW_JSON_FRAME_SCHEMA_V2_ID, arrow_schema,
        fingerprint, validate_arrow_schema,
    },
};

const SCHEMA_DESCRIPTOR_METADATA_KEY: &str = "lqepoch.schema_descriptor.v1";
const SCHEMA_FINGERPRINT_METADATA_KEY: &str = "lqepoch.schema_fingerprint_sha256";

const PARQUET_ROW_GROUP_ROWS: usize = 10_000;
const PARQUET_DATA_PAGE_BYTES: usize = 1024 * 1024;
const PARQUET_READ_BATCH_ROWS: usize = 256;
const MAX_PARQUET_ROW_GROUPS: usize = 1024;
const MAX_PARQUET_ROW_GROUP_UNCOMPRESSED_BYTES: u64 = 32 * 1024 * 1024;
const MAX_PARQUET_TOTAL_UNCOMPRESSED_BYTES: u64 = 512 * 1024 * 1024;

mod events_v3;
mod facts;
mod metadata;
mod projection;
mod raw_frames;
mod raw_frames_v2;
mod rows;
mod write_helpers;
use facts::DatasetFacts;
use metadata::{
    trusted_schema_metadata, validate_decode_budget, validate_optional_schema_metadata,
    writer_properties,
};
use projection::*;
use rows::{decode_bar_batch, decode_event_batch, decode_event_v2_batch, validate_bar_row};
use write_helpers::{
    CappedWriter, TempFileCleanup, create_new_file, parent_dir, parquet_write_error, temporary_path,
};

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ParquetVerification {
    pub schema_id: String,
    pub schema_sha256: String,
    pub footer_rows: u64,
    pub decoded_rows: u64,
    pub size_bytes: u64,
    pub source: MarketDataSourceV1,
    pub symbols: Vec<String>,
    pub time_range: Option<DatasetTimeRangeV1>,
    pub source_timestamp_missing_rows: u64,
}

/// Counts from a verified raw-frame/event-v2 local object pair.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawEventCorrelationVerification {
    pub raw_frame_rows: u64,
    pub event_rows: u64,
}

pub fn write_events(path: &Path, messages: &[CollectionMessage]) -> Result<ParquetVerification> {
    write_events_with_limit(path, messages, crate::archive::DEFAULT_MAX_OBJECT_BYTES)
}

/// Write byte-exact provider MessagePack frames under the core raw-frame schema.
pub fn write_raw_frames_with_limit(
    path: &Path,
    frames: &[RawFrameStorageRecordV1],
    max_object_bytes: u64,
) -> Result<ParquetVerification> {
    raw_frames::write_frames(path, frames, max_object_bytes)
}

/// Write a bounded MessagePack raw/event capture chunk using Core's raw-frame V2 descriptor.
pub fn write_raw_capture_frames_v2_with_limit(
    path: &Path,
    frames: &[RawFrameStorageRecordV2],
    max_object_bytes: u64,
) -> Result<ParquetVerification> {
    raw_frames_v2::write_messagepack_frames(path, frames, max_object_bytes)
}

/// Write a bounded JSON raw/event capture chunk using Core's raw-JSON-frame V2 descriptor.
pub fn write_raw_json_capture_frames_v2_with_limit(
    path: &Path,
    frames: &[RawJsonFrameStorageRecordV2],
    max_object_bytes: u64,
) -> Result<ParquetVerification> {
    raw_frames_v2::write_json_frames(path, frames, max_object_bytes)
}

/// Write normalized events correlated to capture-scoped raw frames under Core's event V3 schema.
pub fn write_event_v3_with_limit(
    path: &Path,
    rows: &[MarketEventParquetRowV3],
    max_object_bytes: u64,
) -> Result<ParquetVerification> {
    events_v3::write_events(path, rows, max_object_bytes)
}

/// Inline decoder for MDP-owned raw-capture files in the configured staging directory only.
/// External Parquet must be verified through the CLI worker boundary.
pub(crate) fn read_capture_raw_frames(
    path: &Path,
    max_object_bytes: u64,
) -> Result<Vec<RawFrameStorageRecordV1>> {
    raw_frames::read_capture_frames(path, max_object_bytes)
}

pub(crate) fn read_capture_raw_frames_v2(
    path: &Path,
    max_object_bytes: u64,
) -> Result<Vec<RawFrameStorageRecordV2>> {
    raw_frames_v2::read_messagepack_frames(path, max_object_bytes)
}

pub(crate) fn read_capture_raw_json_frames_v2(
    path: &Path,
    max_object_bytes: u64,
) -> Result<Vec<RawJsonFrameStorageRecordV2>> {
    raw_frames_v2::read_json_frames(path, max_object_bytes)
}

pub(crate) fn read_capture_event_v3(
    path: &Path,
    max_object_bytes: u64,
) -> Result<Vec<MarketEventParquetRowV3>> {
    events_v3::read_capture_rows(path, max_object_bytes)
}

pub fn write_events_with_limit(
    path: &Path,
    messages: &[CollectionMessage],
    max_bytes: u64,
) -> Result<ParquetVerification> {
    write_events_with_compression(path, messages, max_bytes, default_compression())
}

fn write_events_with_compression(
    path: &Path,
    messages: &[CollectionMessage],
    max_bytes: u64,
    compression: Compression,
) -> Result<ParquetVerification> {
    if messages.len() > crate::protocol::DEFAULT_MAX_JSONL_RECORDS {
        return Err(MarketDataError::InputLimit);
    }
    for message in messages {
        message.validate()?;
        if matches!(message, CollectionMessage::Control(_)) {
            return Err(MarketDataError::UnsupportedEvent);
        }
    }
    let events = messages
        .iter()
        .filter_map(|message| match message {
            CollectionMessage::Market(event) => Some(event),
            CollectionMessage::Control(_) => None,
        })
        .collect::<Vec<_>>();
    if events.is_empty() {
        return Err(MarketDataError::IncompleteWindow);
    }
    let rows = events
        .into_iter()
        .map(|event| MarketEventParquetRowV2 {
            event: (*event).clone(),
            raw_frame_reference: None,
        })
        .collect::<Vec<_>>();
    write_event_rows(path, &rows, EVENT_SCHEMA_ID, max_bytes, compression)
}

/// Write normalized events with their exact source-frame references under the core v2 schema.
pub fn write_event_v2_with_limit(
    path: &Path,
    rows: &[MarketEventParquetRowV2],
    max_bytes: u64,
) -> Result<ParquetVerification> {
    if rows.is_empty() || rows.len() > crate::protocol::DEFAULT_MAX_JSONL_RECORDS {
        return Err(MarketDataError::InputLimit);
    }
    for row in rows {
        row.validate().map_err(|_| MarketDataError::Contract)?;
    }
    write_event_rows(
        path,
        rows,
        EVENT_SCHEMA_V2_ID,
        max_bytes,
        default_compression(),
    )
}

fn write_event_rows(
    path: &Path,
    rows_v2: &[MarketEventParquetRowV2],
    schema_id: &str,
    max_bytes: u64,
    compression: Compression,
) -> Result<ParquetVerification> {
    let events = rows_v2.iter().map(|row| &row.event).collect::<Vec<_>>();
    let schema = Arc::new(arrow_schema(schema_id)?);
    validate_arrow_schema(schema_id, &schema)?;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(UInt32Array::from(
        events
            .iter()
            .map(|event| event.metadata.schema_version)
            .collect::<Vec<_>>(),
    )));
    columns.push(string_array(
        events
            .iter()
            .map(|event| event.metadata.source.provider.as_str()),
    ));
    columns.push(string_array(
        events
            .iter()
            .map(|event| event.metadata.source.feed.as_str()),
    ));
    columns.push(owned_string_array(
        events
            .iter()
            .map(|event| enum_string(&event.metadata.source.entitlement)),
    ));
    columns.push(owned_string_array(
        events
            .iter()
            .map(|event| enum_string(&event.metadata.source.numeric_encoding)),
    ));
    columns.push(Arc::new(StringArray::from(
        events
            .iter()
            .map(|event| event.metadata.source.source_record_id.as_deref())
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(StringArray::from(
        events
            .iter()
            .map(|event| event.metadata.raw_frame_sha256.as_deref())
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt64Array::from(
        events
            .iter()
            .map(|event| event.metadata.generation)
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt64Array::from(
        events
            .iter()
            .map(|event| event.metadata.sequence)
            .collect::<Vec<_>>(),
    )));
    columns.push(timestamp_array(events.iter().map(|event| {
        event
            .metadata
            .source_timestamp
            .as_ref()
            .map(UtcTimestamp::as_str)
    }))?);
    columns.push(timestamp_array(
        events
            .iter()
            .map(|event| Some(event.metadata.received_timestamp.as_str())),
    )?);

    let rows = events
        .iter()
        .map(|event| project_event(event))
        .collect::<Result<Vec<_>>>()?;
    columns.push(string_array(rows.iter().map(|row| row.kind)));
    columns.push(string_array(rows.iter().map(|row| row.symbol)));
    columns.push(nullable_string_array(
        rows.iter().map(|row| row.price.as_deref()),
    ));
    columns.push(nullable_string_array(
        rows.iter().map(|row| row.size.as_deref()),
    ));
    columns.push(nullable_string_array(
        rows.iter().map(|row| row.bid.as_deref()),
    ));
    columns.push(nullable_string_array(
        rows.iter().map(|row| row.ask.as_deref()),
    ));
    columns.push(nullable_string_array(
        rows.iter().map(|row| row.bid_size.as_deref()),
    ));
    columns.push(nullable_string_array(
        rows.iter().map(|row| row.ask_size.as_deref()),
    ));

    if schema_id == EVENT_SCHEMA_V2_ID {
        columns.push(Arc::new(UInt64Array::from(
            rows_v2
                .iter()
                .map(|row| {
                    row.raw_frame_reference
                        .map(|value| value.raw_frame_generation)
                })
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(UInt64Array::from(
            rows_v2
                .iter()
                .map(|row| {
                    row.raw_frame_reference
                        .map(|value| value.raw_frame_sequence)
                })
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(UInt32Array::from(
            rows_v2
                .iter()
                .map(|row| {
                    row.raw_frame_reference
                        .map(|value| value.raw_frame_event_ordinal)
                })
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(UInt32Array::from(
            rows_v2
                .iter()
                .map(|row| {
                    row.raw_frame_reference
                        .map(|value| value.raw_frame_event_count)
                })
                .collect::<Vec<_>>(),
        )));
    } else if schema_id != EVENT_SCHEMA_ID
        || rows_v2.iter().any(|row| row.raw_frame_reference.is_some())
    {
        return Err(MarketDataError::ParquetSchema);
    }

    let batch =
        RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|_| MarketDataError::Parquet)?;
    write_batch_atomic_with_compression(path, schema, &batch, max_bytes, compression, schema_id)?;
    verify_with_limit(path, schema_id, max_bytes)
}

pub fn write_bars(path: &Path, bars: &[TradeMinuteBarV1]) -> Result<ParquetVerification> {
    write_bars_with_limit(path, bars, crate::archive::DEFAULT_MAX_OBJECT_BYTES)
}

pub fn write_bars_with_limit(
    path: &Path,
    bars: &[TradeMinuteBarV1],
    max_bytes: u64,
) -> Result<ParquetVerification> {
    if bars.is_empty() {
        return Err(MarketDataError::IncompleteWindow);
    }
    if bars.len() > crate::aggregate::MAX_AGGREGATION_OUTPUT_ROWS as usize {
        return Err(MarketDataError::InputLimit);
    }
    for bar in bars {
        validate_bar_row(bar)?;
    }
    let mut facts = DatasetFacts::default();
    for bar in bars {
        facts.observe_bar(bar)?;
    }
    facts.finish()?;
    let schema = Arc::new(arrow_schema(MINUTE_BAR_SCHEMA_ID)?);
    validate_arrow_schema(MINUTE_BAR_SCHEMA_ID, &schema)?;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(UInt32Array::from(
        bars.iter()
            .map(|row| row.schema_version)
            .collect::<Vec<_>>(),
    )));
    columns.push(string_array(
        bars.iter().map(|row| row.source_provider.as_str()),
    ));
    columns.push(string_array(
        bars.iter().map(|row| row.source_feed.as_str()),
    ));
    columns.push(string_array(
        bars.iter().map(|row| row.source_entitlement.as_str()),
    ));
    columns.push(string_array(
        bars.iter().map(|row| row.source_numeric_encoding.as_str()),
    ));
    columns.push(string_array(bars.iter().map(|row| row.symbol.as_str())));
    columns.push(timestamp_array(
        bars.iter().map(|row| Some(row.bar_start_utc.as_str())),
    )?);
    columns.push(timestamp_array(
        bars.iter()
            .map(|row| Some(row.bar_end_exclusive_utc.as_str())),
    )?);
    columns.push(timestamp_array(
        bars.iter().map(|row| Some(row.available_at_utc.as_str())),
    )?);
    columns.push(string_array(bars.iter().map(|row| row.trade_date.as_str())));
    columns.push(string_array(bars.iter().map(|row| row.session_id.as_str())));
    columns.push(string_array(
        bars.iter().map(|row| row.session_timezone.as_str()),
    ));
    columns.push(string_array(
        bars.iter().map(|row| row.session_policy_id.as_str()),
    ));
    columns.push(string_array(
        bars.iter().map(|row| row.session_policy_sha256.as_str()),
    ));
    columns.push(timestamp_array(
        bars.iter().map(|row| Some(row.session_start_utc.as_str())),
    )?);
    columns.push(timestamp_array(
        bars.iter()
            .map(|row| Some(row.session_end_exclusive_utc.as_str())),
    )?);
    columns.push(timestamp_array(
        bars.iter().map(|row| Some(row.window_start_utc.as_str())),
    )?);
    columns.push(timestamp_array(
        bars.iter()
            .map(|row| Some(row.window_end_exclusive_utc.as_str())),
    )?);
    columns.push(string_array(bars.iter().map(|row| row.open.as_str())));
    columns.push(string_array(bars.iter().map(|row| row.high.as_str())));
    columns.push(string_array(bars.iter().map(|row| row.low.as_str())));
    columns.push(string_array(bars.iter().map(|row| row.close.as_str())));
    columns.push(string_array(bars.iter().map(|row| row.volume.as_str())));
    columns.push(Arc::new(UInt64Array::from(
        bars.iter().map(|row| row.trade_count).collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt64Array::from(
        bars.iter()
            .map(|row| row.quote_events_excluded)
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt64Array::from(
        bars.iter()
            .map(|row| row.source_timestamp_missing_rows)
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt64Array::from(
        bars.iter()
            .map(|row| row.sequence_gap_count)
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt64Array::from(
        bars.iter()
            .map(|row| row.late_event_count)
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt64Array::from(
        bars.iter()
            .map(|row| row.window_expected_minutes)
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt64Array::from(
        bars.iter()
            .map(|row| row.window_empty_trade_minutes)
            .collect::<Vec<_>>(),
    )));
    columns.push(timestamp_array(
        bars.iter().map(|row| Some(row.source_start_utc.as_str())),
    )?);
    columns.push(timestamp_array(
        bars.iter()
            .map(|row| Some(row.source_end_exclusive_utc.as_str())),
    )?);
    columns.push(Arc::new(BooleanArray::from(
        bars.iter()
            .map(|row| row.window_input_eof)
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(BooleanArray::from(
        bars.iter()
            .map(|row| row.source_pages_exhausted)
            .collect::<Vec<_>>(),
    )));
    columns.push(string_array(
        bars.iter().map(|row| row.completion_mode.as_str()),
    ));
    columns.push(string_array(
        bars.iter().map(|row| row.nbbo_input_status.as_str()),
    ));

    let batch =
        RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|_| MarketDataError::Parquet)?;
    write_batch_atomic(path, schema, &batch, max_bytes, MINUTE_BAR_SCHEMA_ID)?;
    verify_with_limit(path, MINUTE_BAR_SCHEMA_ID, max_bytes)
}

pub fn verify(path: &Path, schema_id: &str) -> Result<ParquetVerification> {
    verify_with_limit(path, schema_id, crate::archive::DEFAULT_MAX_OBJECT_BYTES)
}

pub fn verify_with_limit(
    path: &Path,
    schema_id: &str,
    max_bytes: u64,
) -> Result<ParquetVerification> {
    if fs::metadata(path)?.len() > max_bytes {
        return Err(MarketDataError::InputLimit);
    }
    let file = File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|_| MarketDataError::Parquet)?;
    validate_decode_budget(builder.metadata().as_ref())?;
    validate_arrow_schema(schema_id, builder.schema().as_ref())?;
    validate_optional_schema_metadata(schema_id, builder.metadata().as_ref(), builder.schema())?;
    let schema_hash = fingerprint(schema_id, builder.schema().as_ref())?;
    let footer_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| MarketDataError::Parquet)?;
    if footer_rows > crate::protocol::DEFAULT_MAX_JSONL_RECORDS as u64 {
        return Err(MarketDataError::InputLimit);
    }
    let mut reader = builder
        .with_batch_size(PARQUET_READ_BATCH_ROWS)
        .build()
        .map_err(|_| MarketDataError::Parquet)?;
    let mut decoded_rows = 0_u64;
    let mut facts = DatasetFacts::default();
    for batch in &mut reader {
        let batch = batch.map_err(|_| MarketDataError::Parquet)?;
        validate_no_unexpected_nulls(&batch)?;
        match schema_id {
            EVENT_SCHEMA_ID => {
                for row in decode_event_batch(&batch)? {
                    facts.observe_event(&row)?;
                }
            }
            EVENT_SCHEMA_V2_ID => {
                for row in decode_event_v2_batch(&batch)? {
                    facts.observe_event(&row.event)?;
                }
            }
            EVENT_SCHEMA_V3_ID => {
                for row in events_v3::decode_batch(&batch)? {
                    facts.observe_event(&row.event)?;
                }
            }
            MINUTE_BAR_SCHEMA_ID => {
                for row in decode_bar_batch(&batch)? {
                    facts.observe_bar(&row)?;
                }
            }
            RAW_FRAME_SCHEMA_ID => {
                for row in raw_frames::decode_frame_batch(&batch)? {
                    facts.observe_raw_frame(&row)?;
                }
            }
            RAW_FRAME_SCHEMA_V2_ID => {
                for row in raw_frames_v2::decode_messagepack_batch(&batch)? {
                    facts.observe_raw_frame_v2_messagepack(&row)?;
                }
            }
            RAW_JSON_FRAME_SCHEMA_V2_ID => {
                for row in raw_frames_v2::decode_json_batch(&batch)? {
                    facts.observe_raw_json_frame_v2(&row)?;
                }
            }
            _ => return Err(MarketDataError::ParquetSchema),
        }
        decoded_rows = decoded_rows
            .checked_add(u64::try_from(batch.num_rows()).map_err(|_| MarketDataError::Parquet)?)
            .ok_or(MarketDataError::Parquet)?;
    }
    let size_bytes = fs::metadata(path)?.len();
    if footer_rows == 0 || footer_rows != decoded_rows || size_bytes == 0 {
        return Err(MarketDataError::Parquet);
    }
    let (source, symbols, time_range, source_timestamp_missing_rows) = facts.finish()?;
    Ok(ParquetVerification {
        schema_id: schema_id.to_owned(),
        schema_sha256: schema_hash,
        footer_rows,
        decoded_rows,
        size_bytes,
        source,
        symbols,
        time_range,
        source_timestamp_missing_rows,
    })
}

/// Verify every event-v2 row against its byte-exact raw frame and require complete per-frame
/// ordinal coverage. The caller must also bind both immutable manifests in the capture receipt.
/// Inline pair correlation for producer-owned local Parquet files only.
///
/// Do not use this function on arbitrary or remote Parquet inputs. External inputs must be
/// decoded through the CLI worker boundary; a future CLI capture-pair operation must perform this
/// correlation in that worker rather than exposing this inline helper.
pub(crate) fn verify_event_v2_against_raw(
    raw_path: &Path,
    event_path: &Path,
    max_object_bytes: u64,
) -> Result<RawEventCorrelationVerification> {
    raw_frames::verify_event_v2_against_raw(raw_path, event_path, max_object_bytes)
}

/// Verify capture-scoped V2 raw rows against all V3 event references using the shared Core
/// validator. This helper is for producer-owned local Parquet files only.
pub(crate) fn verify_capture_pair_v2(
    raw_path: &Path,
    raw_schema_id: &str,
    event_path: &Path,
    max_object_bytes: u64,
) -> Result<RawEventCorrelationVerification> {
    let events = events_v3::read_capture_rows(event_path, max_object_bytes)?;
    let (raw_count, event_count) = match raw_schema_id {
        RAW_FRAME_SCHEMA_V2_ID => {
            let frames = raw_frames_v2::read_messagepack_frames(raw_path, max_object_bytes)?;
            market_contracts::validate_messagepack_capture_chunk_v2(&frames, &events)
                .map_err(|_| MarketDataError::IncompleteWindow)?;
            (frames.len(), events.len())
        }
        RAW_JSON_FRAME_SCHEMA_V2_ID => {
            let frames = raw_frames_v2::read_json_frames(raw_path, max_object_bytes)?;
            market_contracts::validate_json_capture_chunk_v2(&frames, &events)
                .map_err(|_| MarketDataError::IncompleteWindow)?;
            (frames.len(), events.len())
        }
        _ => return Err(MarketDataError::ParquetSchema),
    };
    Ok(RawEventCorrelationVerification {
        raw_frame_rows: u64::try_from(raw_count).map_err(|_| MarketDataError::InputLimit)?,
        event_rows: u64::try_from(event_count).map_err(|_| MarketDataError::InputLimit)?,
    })
}

pub fn query_bars(path: &Path, symbol_filter: Option<&str>) -> Result<Vec<TradeMinuteBarV1>> {
    query_bars_with_limits(
        path,
        symbol_filter,
        crate::aggregate::MAX_AGGREGATION_OUTPUT_ROWS as usize,
        crate::remote_query::DEFAULT_MAX_QUERY_RESULT_BYTES,
    )
}

pub fn query_bars_with_limits(
    path: &Path,
    symbol_filter: Option<&str>,
    max_rows: usize,
    max_result_bytes: u64,
) -> Result<Vec<TradeMinuteBarV1>> {
    if fs::metadata(path)?.len() > crate::archive::DEFAULT_MAX_OBJECT_BYTES {
        return Err(MarketDataError::InputLimit);
    }
    if max_rows == 0
        || max_rows > crate::aggregate::MAX_AGGREGATION_OUTPUT_ROWS as usize
        || max_result_bytes == 0
    {
        return Err(MarketDataError::InvalidInput);
    }
    let file = File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|_| MarketDataError::Parquet)?;
    validate_decode_budget(builder.metadata().as_ref())?;
    validate_arrow_schema(MINUTE_BAR_SCHEMA_ID, builder.schema().as_ref())?;
    validate_optional_schema_metadata(
        MINUTE_BAR_SCHEMA_ID,
        builder.metadata().as_ref(),
        builder.schema(),
    )?;
    if builder.metadata().file_metadata().num_rows()
        > crate::protocol::DEFAULT_MAX_JSONL_RECORDS as i64
    {
        return Err(MarketDataError::InputLimit);
    }
    let mut reader = builder
        .with_batch_size(PARQUET_READ_BATCH_ROWS)
        .build()
        .map_err(|_| MarketDataError::Parquet)?;
    let mut rows = Vec::new();
    let mut estimated_bytes = 0_u64;
    for batch in &mut reader {
        let batch = batch.map_err(|_| MarketDataError::Parquet)?;
        for row in decode_bar_batch(&batch)? {
            if symbol_filter.is_none_or(|filter| filter == row.symbol) {
                estimated_bytes = estimated_bytes
                    .checked_add(estimated_bar_row_bytes(&row)?)
                    .ok_or(MarketDataError::InputLimit)?;
                if estimated_bytes > max_result_bytes || rows.len() >= max_rows {
                    return Err(MarketDataError::InputLimit);
                }
                rows.push(row);
            }
        }
    }
    let mut facts = DatasetFacts::default();
    // Validate whole-dataset coverage independently of any symbol query filter.
    let file = File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|_| MarketDataError::Parquet)?;
    validate_decode_budget(builder.metadata().as_ref())?;
    let mut full_reader = builder
        .with_batch_size(PARQUET_READ_BATCH_ROWS)
        .build()
        .map_err(|_| MarketDataError::Parquet)?;
    for batch in &mut full_reader {
        let batch = batch.map_err(|_| MarketDataError::Parquet)?;
        for row in decode_bar_batch(&batch)? {
            facts.observe_bar(&row)?;
        }
    }
    let _ = facts.finish()?;
    Ok(rows)
}

fn estimated_bar_row_bytes(row: &TradeMinuteBarV1) -> Result<u64> {
    let string_bytes = [
        row.source_provider.as_str(),
        row.source_feed.as_str(),
        row.source_entitlement.as_str(),
        row.source_numeric_encoding.as_str(),
        row.symbol.as_str(),
        row.trade_date.as_str(),
        row.session_id.as_str(),
        row.session_timezone.as_str(),
        row.session_policy_id.as_str(),
        row.session_policy_sha256.as_str(),
        row.open.as_str(),
        row.high.as_str(),
        row.low.as_str(),
        row.close.as_str(),
        row.volume.as_str(),
        row.completion_mode.as_str(),
        row.nbbo_input_status.as_str(),
    ]
    .into_iter()
    .try_fold(0_u64, |sum, value| {
        sum.checked_add(u64::try_from(value.len()).map_err(|_| MarketDataError::InputLimit)?)
            .ok_or(MarketDataError::InputLimit)
    })?;
    string_bytes
        .checked_add(512)
        .ok_or(MarketDataError::InputLimit)
}

pub fn export_bars_jsonl(
    path: &Path,
    destination: &Path,
    symbol_filter: Option<&str>,
) -> Result<usize> {
    let bars = query_bars(path, symbol_filter)?;
    if destination.exists() {
        return Err(MarketDataError::Conflict);
    }
    let mut file = create_new_file(destination)?;
    for row in &bars {
        serde_json::to_writer(&mut file, row)?;
        use std::io::Write;
        file.write_all(b"\n")?;
    }
    file.sync_all()?;
    Ok(bars.len())
}

fn write_batch_atomic(
    path: &Path,
    schema: SchemaRef,
    batch: &RecordBatch,
    max_bytes: u64,
    schema_id: &str,
) -> Result<()> {
    write_batch_atomic_with_compression(
        path,
        schema,
        batch,
        max_bytes,
        default_compression(),
        schema_id,
    )
}

fn write_batch_atomic_with_compression(
    path: &Path,
    schema: SchemaRef,
    batch: &RecordBatch,
    max_bytes: u64,
    compression: Compression,
    schema_id: &str,
) -> Result<()> {
    if max_bytes == 0 {
        return Err(MarketDataError::InvalidInput);
    }
    validate_no_unexpected_nulls(batch)?;
    if path.exists() {
        return Err(MarketDataError::Conflict);
    }
    let parent = parent_dir(path);
    fs::create_dir_all(parent)?;
    let temp = temporary_path(path);
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    let _cleanup = TempFileCleanup(temp.clone());
    let over_limit = Arc::new(AtomicBool::new(false));
    let capped_file = CappedWriter {
        file,
        max_bytes,
        written_bytes: 0,
        over_limit: Arc::clone(&over_limit),
    };
    let schema_metadata = trusted_schema_metadata(schema_id)?;
    let writer_schema = Arc::new(
        schema
            .as_ref()
            .clone()
            .with_metadata(schema_metadata.clone()),
    );
    let annotated_batch =
        RecordBatch::try_new(Arc::clone(&writer_schema), batch.columns().to_vec())
            .map_err(|_| MarketDataError::Parquet)?;
    let mut flat_metadata = schema_metadata
        .iter()
        .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
        .collect::<Vec<_>>();
    flat_metadata.sort_by(|left, right| left.key.cmp(&right.key));
    let mut writer = ArrowWriter::try_new(
        capped_file,
        writer_schema,
        Some(writer_properties(compression, flat_metadata)),
    )
    .map_err(|_| parquet_write_error(&over_limit))?;
    writer
        .write(&annotated_batch)
        .map_err(|_| parquet_write_error(&over_limit))?;
    writer
        .close()
        .map_err(|_| parquet_write_error(&over_limit))?;
    File::open(&temp)?.sync_all()?;
    match fs::hard_link(&temp, path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&temp);
            return Err(MarketDataError::Conflict);
        }
        Err(_) => {
            let _ = fs::remove_file(&temp);
            return Err(MarketDataError::Io(std::io::Error::other(
                "atomic parquet publish failed",
            )));
        }
    }
    fs::remove_file(&temp)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn default_compression() -> Compression {
    Compression::ZSTD(ZstdLevel::try_new(1).expect("Zstandard level 1 is valid"))
}

#[cfg(test)]
pub(crate) mod tests;
