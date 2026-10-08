//! Typed Arrow/Parquet event and completed-minute-bar storage.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt32Array,
    UInt64Array,
};
use arrow_schema::SchemaRef;
use chrono::{DateTime, NaiveDate, Utc};
use exact_decimal::ExactDecimal;
use market_contracts::{
    DatasetTimeRangeV1, DecimalString, EntitlementState, EventMetadataV1, MarketDataSourceV1,
    MarketEventEnvelopeV1, MarketEventV1, NumericEncodingV1, UtcTimestamp,
};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};

use crate::{
    MarketDataError, Result,
    aggregate::TradeMinuteBarV1,
    aggregate::validate_stock_symbol,
    queue::CollectionMessage,
    schema::{
        EVENT_SCHEMA_ID, MINUTE_BAR_SCHEMA_ID, arrow_schema, fingerprint, validate_arrow_schema,
    },
};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

mod facts;
mod rows;
use facts::DatasetFacts;
use rows::{decode_bar_batch, decode_event_batch, validate_bar_row};

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
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

pub fn write_events(path: &Path, messages: &[CollectionMessage]) -> Result<ParquetVerification> {
    write_events_with_limit(path, messages, crate::archive::DEFAULT_MAX_OBJECT_BYTES)
}

pub fn write_events_with_limit(
    path: &Path,
    messages: &[CollectionMessage],
    max_bytes: u64,
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
    let schema = Arc::new(arrow_schema(EVENT_SCHEMA_ID)?);
    validate_arrow_schema(EVENT_SCHEMA_ID, &schema)?;
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

    let batch =
        RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|_| MarketDataError::Parquet)?;
    write_batch_atomic(path, schema, &batch, max_bytes)?;
    verify_with_limit(path, EVENT_SCHEMA_ID, max_bytes)
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
    write_batch_atomic(path, schema, &batch, max_bytes)?;
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
    validate_arrow_schema(schema_id, builder.schema().as_ref())?;
    let schema_hash = fingerprint(schema_id, builder.schema().as_ref())?;
    let footer_rows = u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| MarketDataError::Parquet)?;
    if footer_rows > crate::protocol::DEFAULT_MAX_JSONL_RECORDS as u64 {
        return Err(MarketDataError::InputLimit);
    }
    let mut reader = builder.build().map_err(|_| MarketDataError::Parquet)?;
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
            MINUTE_BAR_SCHEMA_ID => {
                for row in decode_bar_batch(&batch)? {
                    facts.observe_bar(&row)?;
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

pub fn query_bars(path: &Path, symbol_filter: Option<&str>) -> Result<Vec<TradeMinuteBarV1>> {
    if fs::metadata(path)?.len() > crate::archive::DEFAULT_MAX_OBJECT_BYTES {
        return Err(MarketDataError::InputLimit);
    }
    let file = File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|_| MarketDataError::Parquet)?;
    validate_arrow_schema(MINUTE_BAR_SCHEMA_ID, builder.schema().as_ref())?;
    if builder.metadata().file_metadata().num_rows()
        > crate::protocol::DEFAULT_MAX_JSONL_RECORDS as i64
    {
        return Err(MarketDataError::InputLimit);
    }
    let mut reader = builder.build().map_err(|_| MarketDataError::Parquet)?;
    let mut rows = Vec::new();
    for batch in &mut reader {
        let batch = batch.map_err(|_| MarketDataError::Parquet)?;
        for row in decode_bar_batch(&batch)? {
            if symbol_filter.is_none_or(|filter| filter == row.symbol) {
                rows.push(row);
            }
        }
    }
    if rows.len() > crate::aggregate::MAX_AGGREGATION_OUTPUT_ROWS as usize {
        return Err(MarketDataError::InputLimit);
    }
    let mut facts = DatasetFacts::default();
    // Validate whole-dataset coverage independently of any symbol query filter.
    let file = File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|_| MarketDataError::Parquet)?;
    let mut full_reader = builder.build().map_err(|_| MarketDataError::Parquet)?;
    for batch in &mut full_reader {
        let batch = batch.map_err(|_| MarketDataError::Parquet)?;
        for row in decode_bar_batch(&batch)? {
            facts.observe_bar(&row)?;
        }
    }
    let _ = facts.finish()?;
    Ok(rows)
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
    let mut writer = ArrowWriter::try_new(
        capped_file,
        Arc::clone(&schema),
        Some(WriterProperties::builder().build()),
    )
    .map_err(|_| parquet_write_error(&over_limit))?;
    writer
        .write(batch)
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

struct CappedWriter {
    file: File,
    max_bytes: u64,
    written_bytes: u64,
    over_limit: Arc<AtomicBool>,
}

impl Write for CappedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let requested = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if self.written_bytes.saturating_add(requested) > self.max_bytes {
            self.over_limit.store(true, Ordering::Release);
            return Err(std::io::Error::other("parquet byte limit exceeded"));
        }
        let written = self.file.write(bytes)?;
        self.written_bytes = self
            .written_bytes
            .checked_add(u64::try_from(written).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("parquet byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

fn parquet_write_error(over_limit: &AtomicBool) -> MarketDataError {
    if over_limit.load(Ordering::Acquire) {
        MarketDataError::InputLimit
    } else {
        MarketDataError::Parquet
    }
}

struct TempFileCleanup(PathBuf);

impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn create_new_file(path: &Path) -> Result<File> {
    fs::create_dir_all(parent_dir(path))?;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                MarketDataError::Conflict
            } else {
                MarketDataError::Io(error)
            }
        })
}

fn temporary_path(path: &Path) -> PathBuf {
    let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("dataset.parquet");
    parent_dir(path).join(format!(".{name}.tmp-{}-{sequence}", std::process::id()))
}

fn parent_dir(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn validate_no_unexpected_nulls(batch: &RecordBatch) -> Result<()> {
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if !field.is_nullable() && column.null_count() != 0 {
            return Err(MarketDataError::ParquetSchema);
        }
    }
    Ok(())
}

fn string_array<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn nullable_string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>()))
}

fn owned_string_array(values: impl Iterator<Item = String>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn timestamp_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> Result<ArrayRef> {
    let values = values
        .map(|value| value.map(timestamp_to_ns).transpose())
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(
        TimestampNanosecondArray::from(values).with_timezone("UTC"),
    ))
}

fn timestamp_to_ns(value: &str) -> Result<i64> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|_| MarketDataError::Contract)?
        .timestamp_nanos_opt()
        .ok_or(MarketDataError::InvalidInput)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_metadata_label(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn timestamp_from_ns(value: i64) -> Result<UtcTimestamp> {
    let seconds = value.div_euclid(1_000_000_000);
    let nanos = value.rem_euclid(1_000_000_000) as u32;
    let timestamp =
        DateTime::<Utc>::from_timestamp(seconds, nanos).ok_or(MarketDataError::InvalidInput)?;
    UtcTimestamp::parse(&timestamp.to_rfc3339()).map_err(|_| MarketDataError::Contract)
}

struct ProjectedEvent<'a> {
    kind: &'static str,
    symbol: &'a str,
    price: Option<String>,
    size: Option<String>,
    bid: Option<String>,
    ask: Option<String>,
    bid_size: Option<String>,
    ask_size: Option<String>,
}

fn project_event(event: &MarketEventEnvelopeV1) -> Result<ProjectedEvent<'_>> {
    Ok(match &event.event {
        MarketEventV1::StockQuote {
            symbol,
            bid,
            ask,
            bid_size,
            ask_size,
        } => ProjectedEvent {
            kind: "stock_quote",
            symbol,
            price: None,
            size: None,
            bid: bid.as_ref().map(ToString::to_string),
            ask: ask.as_ref().map(ToString::to_string),
            bid_size: bid_size.as_ref().map(ToString::to_string),
            ask_size: ask_size.as_ref().map(ToString::to_string),
        },
        MarketEventV1::StockTrade {
            symbol,
            price,
            size,
        } => ProjectedEvent {
            kind: "stock_trade",
            symbol,
            price: Some(price.as_str().to_owned()),
            size: Some(size.as_str().to_owned()),
            bid: None,
            ask: None,
            bid_size: None,
            ask_size: None,
        },
        MarketEventV1::OptionQuote {
            symbol,
            bid,
            ask,
            bid_size,
            ask_size,
        } => ProjectedEvent {
            kind: "option_quote",
            symbol,
            price: None,
            size: None,
            bid: bid.as_ref().map(ToString::to_string),
            ask: ask.as_ref().map(ToString::to_string),
            bid_size: bid_size.as_ref().map(ToString::to_string),
            ask_size: ask_size.as_ref().map(ToString::to_string),
        },
        MarketEventV1::OptionTrade {
            symbol,
            price,
            size,
        } => ProjectedEvent {
            kind: "option_trade",
            symbol,
            price: Some(price.as_str().to_owned()),
            size: Some(size.as_str().to_owned()),
            bid: None,
            ask: None,
            bid_size: None,
            ask_size: None,
        },
    })
}

fn enum_string<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn string_column(batch: &RecordBatch, index: usize) -> Result<&StringArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or(MarketDataError::Parquet)
}

fn value_string(array: &StringArray, index: usize) -> Result<&str> {
    if array.is_null(index) {
        return Err(MarketDataError::ParquetSchema);
    }
    Ok(array.value(index))
}

fn timestamp_value(array: &TimestampNanosecondArray, index: usize) -> Result<UtcTimestamp> {
    if array.is_null(index) {
        return Err(MarketDataError::ParquetSchema);
    }
    timestamp_from_ns(array.value(index))
}

fn u64_value(array: &UInt64Array, index: usize) -> Result<u64> {
    if array.is_null(index) {
        return Err(MarketDataError::ParquetSchema);
    }
    Ok(array.value(index))
}

fn u32_value(array: &UInt32Array, index: usize) -> Result<u32> {
    if array.is_null(index) {
        return Err(MarketDataError::ParquetSchema);
    }
    Ok(array.value(index))
}

fn bool_value(array: &BooleanArray, index: usize) -> Result<bool> {
    if array.is_null(index) {
        return Err(MarketDataError::ParquetSchema);
    }
    Ok(array.value(index))
}

#[cfg(test)]
mod tests;
