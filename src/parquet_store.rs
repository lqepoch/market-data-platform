//! Typed Arrow/Parquet event and completed-minute-bar storage.

use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
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

#[derive(Default)]
struct DatasetFacts {
    source: Option<MarketDataSourceV1>,
    symbols: BTreeSet<String>,
    minimum_source_ns: Option<i64>,
    maximum_source_end_ns: Option<i64>,
    source_timestamp_missing_rows: u64,
    bar_window: Option<BarWindowIdentity>,
    bar_keys: BTreeSet<(String, i64)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BarWindowIdentity {
    provider: String,
    feed: String,
    entitlement: String,
    numeric_encoding: String,
    trade_date: String,
    session_id: String,
    timezone: String,
    policy_id: String,
    policy_sha256: String,
    session_start: String,
    session_end_exclusive: String,
    window_start: String,
    window_end_exclusive: String,
    available_at: String,
    expected_minutes: u64,
    input_eof: bool,
    source_pages_exhausted: Option<bool>,
    completion_mode: String,
    nbbo_input_status: String,
}

impl BarWindowIdentity {
    fn from_row(row: &TradeMinuteBarV1) -> Self {
        Self {
            provider: row.source_provider.clone(),
            feed: row.source_feed.clone(),
            entitlement: row.source_entitlement.clone(),
            numeric_encoding: row.source_numeric_encoding.clone(),
            trade_date: row.trade_date.clone(),
            session_id: row.session_id.clone(),
            timezone: row.session_timezone.clone(),
            policy_id: row.session_policy_id.clone(),
            policy_sha256: row.session_policy_sha256.clone(),
            session_start: row.session_start_utc.as_str().to_owned(),
            session_end_exclusive: row.session_end_exclusive_utc.as_str().to_owned(),
            window_start: row.window_start_utc.as_str().to_owned(),
            window_end_exclusive: row.window_end_exclusive_utc.as_str().to_owned(),
            available_at: row.available_at_utc.as_str().to_owned(),
            expected_minutes: row.window_expected_minutes,
            input_eof: row.window_input_eof,
            source_pages_exhausted: row.source_pages_exhausted,
            completion_mode: row.completion_mode.clone(),
            nbbo_input_status: row.nbbo_input_status.clone(),
        }
    }
}

impl DatasetFacts {
    fn observe_source(&mut self, mut source: MarketDataSourceV1) -> Result<()> {
        source.source_record_id = None;
        if self
            .source
            .as_ref()
            .is_some_and(|existing| existing != &source)
        {
            return Err(MarketDataError::MixedProvenance);
        }
        self.source.get_or_insert(source);
        Ok(())
    }

    fn observe_source_interval(&mut self, start_ns: i64, end_exclusive_ns: i64) -> Result<()> {
        if start_ns >= end_exclusive_ns {
            return Err(MarketDataError::Contract);
        }
        self.minimum_source_ns = Some(
            self.minimum_source_ns
                .map_or(start_ns, |current| current.min(start_ns)),
        );
        self.maximum_source_end_ns = Some(
            self.maximum_source_end_ns
                .map_or(end_exclusive_ns, |current| current.max(end_exclusive_ns)),
        );
        Ok(())
    }

    fn observe_event(&mut self, row: &MarketEventEnvelopeV1) -> Result<()> {
        self.observe_source(row.metadata.source.clone())?;
        let symbol = match &row.event {
            MarketEventV1::StockQuote { symbol, .. }
            | MarketEventV1::StockTrade { symbol, .. }
            | MarketEventV1::OptionQuote { symbol, .. }
            | MarketEventV1::OptionTrade { symbol, .. } => symbol,
        };
        self.symbols.insert(symbol.clone());
        if let Some(timestamp) = &row.metadata.source_timestamp {
            let start_ns = timestamp_to_ns(timestamp.as_str())?;
            self.observe_source_interval(
                start_ns,
                start_ns
                    .checked_add(1)
                    .ok_or(MarketDataError::InvalidInput)?,
            )?;
        } else {
            self.source_timestamp_missing_rows = self
                .source_timestamp_missing_rows
                .checked_add(1)
                .ok_or(MarketDataError::InputLimit)?;
        }
        Ok(())
    }

    fn observe_bar(&mut self, row: &TradeMinuteBarV1) -> Result<()> {
        let source = source_from_bar(row)?;
        self.observe_source(source)?;
        self.symbols.insert(row.symbol.clone());
        let start_ns = timestamp_to_ns(row.source_start_utc.as_str())?;
        let end_ns = timestamp_to_ns(row.source_end_exclusive_utc.as_str())?;
        self.observe_source_interval(start_ns, end_ns)?;

        let identity = BarWindowIdentity::from_row(row);
        if self
            .bar_window
            .as_ref()
            .is_some_and(|existing| existing != &identity)
        {
            return Err(MarketDataError::Contract);
        }
        self.bar_window.get_or_insert(identity);
        let bar_start_ns = timestamp_to_ns(row.bar_start_utc.as_str())?;
        if !self.bar_keys.insert((row.symbol.clone(), bar_start_ns)) {
            return Err(MarketDataError::Contract);
        }
        Ok(())
    }

    fn validate_complete_bar_grid(&self) -> Result<()> {
        let Some(identity) = &self.bar_window else {
            return Ok(());
        };
        let start_ns = timestamp_to_ns(&identity.window_start)?;
        let mut expected_rows = 0_u64;
        for symbol in &self.symbols {
            for offset in 0..identity.expected_minutes {
                let offset_ns = i64::try_from(offset)
                    .map_err(|_| MarketDataError::InputLimit)?
                    .checked_mul(60_000_000_000)
                    .ok_or(MarketDataError::InputLimit)?;
                let bar_start_ns = start_ns
                    .checked_add(offset_ns)
                    .ok_or(MarketDataError::InputLimit)?;
                if !self.bar_keys.contains(&(symbol.clone(), bar_start_ns)) {
                    return Err(MarketDataError::IncompleteWindow);
                }
                expected_rows = expected_rows
                    .checked_add(1)
                    .ok_or(MarketDataError::InputLimit)?;
            }
        }
        if expected_rows != self.bar_keys.len() as u64 {
            return Err(MarketDataError::IncompleteWindow);
        }
        Ok(())
    }

    fn finish(
        self,
    ) -> Result<(
        MarketDataSourceV1,
        Vec<String>,
        Option<DatasetTimeRangeV1>,
        u64,
    )> {
        self.validate_complete_bar_grid()?;
        let source = self.source.ok_or(MarketDataError::IncompleteWindow)?;
        let time_range = match (self.minimum_source_ns, self.maximum_source_end_ns) {
            (Some(start), Some(end)) => Some(DatasetTimeRangeV1 {
                start_inclusive: timestamp_from_ns(start)?,
                end_exclusive: timestamp_from_ns(end)?,
            }),
            (None, None) => None,
            _ => return Err(MarketDataError::Contract),
        };
        Ok((
            source,
            self.symbols.into_iter().collect(),
            time_range,
            self.source_timestamp_missing_rows,
        ))
    }
}

pub fn write_events(path: &Path, messages: &[CollectionMessage]) -> Result<ParquetVerification> {
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
    write_batch_atomic(path, schema, &batch)?;
    verify(path, EVENT_SCHEMA_ID)
}

pub fn write_bars(path: &Path, bars: &[TradeMinuteBarV1]) -> Result<ParquetVerification> {
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
    write_batch_atomic(path, schema, &batch)?;
    verify(path, MINUTE_BAR_SCHEMA_ID)
}

pub fn verify(path: &Path, schema_id: &str) -> Result<ParquetVerification> {
    if fs::metadata(path)?.len() > crate::archive::DEFAULT_MAX_OBJECT_BYTES {
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

fn decode_event_batch(batch: &RecordBatch) -> Result<Vec<MarketEventEnvelopeV1>> {
    validate_no_unexpected_nulls(batch)?;
    let col = |name: &str| -> Result<usize> {
        batch
            .schema()
            .index_of(name)
            .map_err(|_| MarketDataError::Parquet)
    };
    let strings = |name: &str| -> Result<&StringArray> { string_column(batch, col(name)?) };
    let timestamps = |name: &str| -> Result<&TimestampNanosecondArray> {
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
    let u32s = |name: &str| -> Result<&UInt32Array> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let mut rows = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        let source = MarketDataSourceV1::new(
            value_string(strings("provider")?, index)?.to_owned(),
            value_string(strings("feed")?, index)?.to_owned(),
            parse_entitlement(value_string(strings("entitlement")?, index)?)?,
            parse_numeric_encoding(value_string(strings("numeric_encoding")?, index)?)?,
            optional_string_value(strings("source_record_id")?, index).map(str::to_owned),
        )
        .map_err(|_| MarketDataError::Contract)?;
        let metadata = EventMetadataV1 {
            schema_version: u32_value(u32s("schema_version")?, index)?,
            source,
            generation: u64_value(u64s("generation")?, index)?,
            sequence: u64_value(u64s("sequence")?, index)?,
            raw_frame_sha256: optional_string_value(strings("raw_frame_sha256")?, index)
                .map(str::to_owned),
            source_timestamp: optional_timestamp_value(timestamps("source_timestamp")?, index)?,
            received_timestamp: timestamp_value(timestamps("received_timestamp")?, index)?,
        };
        let kind = value_string(strings("event_kind")?, index)?;
        let symbol = value_string(strings("symbol")?, index)?.to_owned();
        let price = optional_decimal_value(strings("price")?, index)?;
        let size = optional_decimal_value(strings("size")?, index)?;
        let bid = optional_decimal_value(strings("bid")?, index)?;
        let ask = optional_decimal_value(strings("ask")?, index)?;
        let bid_size = optional_decimal_value(strings("bid_size")?, index)?;
        let ask_size = optional_decimal_value(strings("ask_size")?, index)?;
        let event = match kind {
            "stock_quote" => {
                if price.is_some() || size.is_some() {
                    return Err(MarketDataError::Contract);
                }
                MarketEventV1::StockQuote {
                    symbol,
                    bid,
                    ask,
                    bid_size,
                    ask_size,
                }
            }
            "stock_trade" => {
                if bid.is_some() || ask.is_some() || bid_size.is_some() || ask_size.is_some() {
                    return Err(MarketDataError::Contract);
                }
                MarketEventV1::StockTrade {
                    symbol,
                    price: price.ok_or(MarketDataError::Contract)?,
                    size: size.ok_or(MarketDataError::Contract)?,
                }
            }
            "option_quote" => {
                if price.is_some() || size.is_some() {
                    return Err(MarketDataError::Contract);
                }
                MarketEventV1::OptionQuote {
                    symbol,
                    bid,
                    ask,
                    bid_size,
                    ask_size,
                }
            }
            "option_trade" => {
                if bid.is_some() || ask.is_some() || bid_size.is_some() || ask_size.is_some() {
                    return Err(MarketDataError::Contract);
                }
                MarketEventV1::OptionTrade {
                    symbol,
                    price: price.ok_or(MarketDataError::Contract)?,
                    size: size.ok_or(MarketDataError::Contract)?,
                }
            }
            _ => return Err(MarketDataError::Contract),
        };
        let row = MarketEventEnvelopeV1 { metadata, event };
        row.validate().map_err(|_| MarketDataError::Contract)?;
        rows.push(row);
    }
    Ok(rows)
}

fn decode_bar_batch(batch: &RecordBatch) -> Result<Vec<TradeMinuteBarV1>> {
    validate_no_unexpected_nulls(batch)?;
    let col = |name: &str| -> Result<usize> {
        batch
            .schema()
            .index_of(name)
            .map_err(|_| MarketDataError::Parquet)
    };
    let strings = |name: &str| -> Result<&StringArray> { string_column(batch, col(name)?) };
    let timestamps = |name: &str| -> Result<&TimestampNanosecondArray> {
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
    let u32s = |name: &str| -> Result<&UInt32Array> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let bools = |name: &str| -> Result<&BooleanArray> {
        batch
            .column(col(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let mut rows = Vec::with_capacity(batch.num_rows());
    for index in 0..batch.num_rows() {
        let row = TradeMinuteBarV1 {
            schema_version: u32_value(u32s("schema_version")?, index)?,
            source_provider: value_string(strings("source_provider")?, index)?.to_owned(),
            source_feed: value_string(strings("source_feed")?, index)?.to_owned(),
            source_entitlement: value_string(strings("source_entitlement")?, index)?.to_owned(),
            source_numeric_encoding: value_string(strings("source_numeric_encoding")?, index)?
                .to_owned(),
            symbol: value_string(strings("symbol")?, index)?.to_owned(),
            bar_start_utc: timestamp_value(timestamps("bar_start_utc")?, index)?,
            bar_end_exclusive_utc: timestamp_value(timestamps("bar_end_exclusive_utc")?, index)?,
            available_at_utc: timestamp_value(timestamps("available_at_utc")?, index)?,
            trade_date: value_string(strings("trade_date")?, index)?.to_owned(),
            session_id: value_string(strings("session_id")?, index)?.to_owned(),
            session_timezone: value_string(strings("session_timezone")?, index)?.to_owned(),
            session_policy_id: value_string(strings("session_policy_id")?, index)?.to_owned(),
            session_policy_sha256: value_string(strings("session_policy_sha256")?, index)?
                .to_owned(),
            session_start_utc: timestamp_value(timestamps("session_start_utc")?, index)?,
            session_end_exclusive_utc: timestamp_value(
                timestamps("session_end_exclusive_utc")?,
                index,
            )?,
            window_start_utc: timestamp_value(timestamps("window_start_utc")?, index)?,
            window_end_exclusive_utc: timestamp_value(
                timestamps("window_end_exclusive_utc")?,
                index,
            )?,
            open: value_string(strings("open")?, index)?.to_owned(),
            high: value_string(strings("high")?, index)?.to_owned(),
            low: value_string(strings("low")?, index)?.to_owned(),
            close: value_string(strings("close")?, index)?.to_owned(),
            volume: value_string(strings("volume")?, index)?.to_owned(),
            trade_count: u64_value(u64s("trade_count")?, index)?,
            quote_events_excluded: u64_value(u64s("quote_events_excluded")?, index)?,
            source_timestamp_missing_rows: u64_value(
                u64s("source_timestamp_missing_rows")?,
                index,
            )?,
            sequence_gap_count: u64_value(u64s("sequence_gap_count")?, index)?,
            late_event_count: u64_value(u64s("late_event_count")?, index)?,
            window_expected_minutes: u64_value(u64s("window_expected_minutes")?, index)?,
            window_empty_trade_minutes: u64_value(u64s("window_empty_trade_minutes")?, index)?,
            source_start_utc: timestamp_value(timestamps("source_start_utc")?, index)?,
            source_end_exclusive_utc: timestamp_value(
                timestamps("source_end_exclusive_utc")?,
                index,
            )?,
            window_input_eof: bool_value(bools("window_input_eof")?, index)?,
            source_pages_exhausted: if bools("source_pages_exhausted")?.is_null(index) {
                None
            } else {
                Some(bool_value(bools("source_pages_exhausted")?, index)?)
            },
            completion_mode: value_string(strings("completion_mode")?, index)?.to_owned(),
            nbbo_input_status: value_string(strings("nbbo_input_status")?, index)?.to_owned(),
        };
        validate_bar_row(&row)?;
        rows.push(row);
    }
    Ok(rows)
}

fn validate_bar_row(row: &TradeMinuteBarV1) -> Result<()> {
    let entitlement = parse_entitlement(&row.source_entitlement)?;
    let encoding = parse_numeric_encoding(&row.source_numeric_encoding)?;
    MarketDataSourceV1::new(
        row.source_provider.clone(),
        row.source_feed.clone(),
        entitlement,
        encoding,
        None,
    )
    .map_err(|_| MarketDataError::Contract)?;
    if row.schema_version != 1
        || !valid_metadata_label(&row.session_id, 256)
        || !valid_metadata_label(&row.session_timezone, 128)
        || !valid_metadata_label(&row.session_policy_id, 256)
        || !valid_sha256(&row.session_policy_sha256)
        || row.completion_mode.is_empty()
        || row.nbbo_input_status != "excluded"
        || !row.window_input_eof
        || row.source_timestamp_missing_rows != 0
        || row.sequence_gap_count != 0
        || row.late_event_count != 0
        || row.window_empty_trade_minutes != 0
        || row.trade_count == 0
        || !NaiveDate::parse_from_str(&row.trade_date, "%Y-%m-%d")
            .is_ok_and(|date| date.to_string() == row.trade_date)
    {
        return Err(MarketDataError::Contract);
    }
    validate_stock_symbol(&row.symbol)?;

    let session_start = timestamp_to_ns(row.session_start_utc.as_str())?;
    let session_end = timestamp_to_ns(row.session_end_exclusive_utc.as_str())?;
    let window_start = timestamp_to_ns(row.window_start_utc.as_str())?;
    let window_end = timestamp_to_ns(row.window_end_exclusive_utc.as_str())?;
    let bar_start = timestamp_to_ns(row.bar_start_utc.as_str())?;
    let bar_end = timestamp_to_ns(row.bar_end_exclusive_utc.as_str())?;
    let available_at = timestamp_to_ns(row.available_at_utc.as_str())?;
    let source_start = timestamp_to_ns(row.source_start_utc.as_str())?;
    let source_end = timestamp_to_ns(row.source_end_exclusive_utc.as_str())?;
    let window_duration = window_end
        .checked_sub(window_start)
        .ok_or(MarketDataError::Contract)?;
    let session_duration = session_end
        .checked_sub(session_start)
        .ok_or(MarketDataError::Contract)?;
    let offset = window_start
        .checked_sub(session_start)
        .ok_or(MarketDataError::Contract)?;
    let expected_minutes =
        u64::try_from(window_duration / 60_000_000_000).map_err(|_| MarketDataError::Contract)?;
    if session_start > window_start
        || window_start >= window_end
        || window_end > session_end
        || session_duration <= 0
        || window_duration <= 0
        || offset % 60_000_000_000 != 0
        || session_duration % 60_000_000_000 != 0
        || window_duration % 60_000_000_000 != 0
        || expected_minutes == 0
        || expected_minutes > 390
        || row.window_expected_minutes != expected_minutes
        || bar_start % 60_000_000_000 != 0
        || bar_start < window_start
        || bar_end
            != bar_start
                .checked_add(60_000_000_000)
                .ok_or(MarketDataError::Contract)?
        || bar_end > window_end
        || available_at < window_end
        || source_start < bar_start
        || source_start >= source_end
        || source_end > bar_end
    {
        return Err(MarketDataError::Contract);
    }

    match row.completion_mode.as_str() {
        "synthetic_eof"
            if row.source_provider == "synthetic"
                && row.source_feed == "synthetic"
                && entitlement == EntitlementState::Unknown
                && encoding == NumericEncodingV1::DecimalToken
                && row.source_pages_exhausted.is_none() => {}
        "historical_eof_paged"
            if row.source_provider != "synthetic" && row.source_pages_exhausted == Some(true) => {}
        "historical_eof_nonpaged"
            if row.source_provider != "synthetic" && row.source_pages_exhausted.is_none() => {}
        _ => return Err(MarketDataError::Contract),
    }

    let open = positive_decimal(&row.open)?;
    let high = positive_decimal(&row.high)?;
    let low = positive_decimal(&row.low)?;
    let close = positive_decimal(&row.close)?;
    let volume = nonnegative_decimal(&row.volume)?;
    if volume <= ExactDecimal::ZERO
        || low > high
        || high < open
        || high < close
        || low > open
        || low > close
    {
        return Err(MarketDataError::Contract);
    }
    Ok(())
}

fn source_from_bar(row: &TradeMinuteBarV1) -> Result<MarketDataSourceV1> {
    MarketDataSourceV1::new(
        row.source_provider.clone(),
        row.source_feed.clone(),
        parse_entitlement(&row.source_entitlement)?,
        parse_numeric_encoding(&row.source_numeric_encoding)?,
        None,
    )
    .map_err(|_| MarketDataError::Contract)
}

fn positive_decimal(value: &str) -> Result<ExactDecimal> {
    DecimalString::new(value.to_owned()).map_err(|_| MarketDataError::Contract)?;
    let value = ExactDecimal::parse_json_number(value).map_err(|_| MarketDataError::Contract)?;
    if value <= ExactDecimal::ZERO {
        return Err(MarketDataError::Contract);
    }
    Ok(value)
}

fn nonnegative_decimal(value: &str) -> Result<ExactDecimal> {
    DecimalString::new(value.to_owned()).map_err(|_| MarketDataError::Contract)?;
    let value = ExactDecimal::parse_json_number(value).map_err(|_| MarketDataError::Contract)?;
    if value < ExactDecimal::ZERO {
        return Err(MarketDataError::Contract);
    }
    Ok(value)
}

fn parse_entitlement(value: &str) -> Result<EntitlementState> {
    match value {
        "unknown" => Ok(EntitlementState::Unknown),
        "authorized" => Ok(EntitlementState::Authorized),
        "unauthorized" => Ok(EntitlementState::Unauthorized),
        _ => Err(MarketDataError::Contract),
    }
}

fn parse_numeric_encoding(value: &str) -> Result<NumericEncodingV1> {
    match value {
        "decimal_token" => Ok(NumericEncodingV1::DecimalToken),
        "integer_token" => Ok(NumericEncodingV1::IntegerToken),
        "binary_float64_shortest_decimal" => Ok(NumericEncodingV1::BinaryFloat64ShortestDecimal),
        "binary_float32_shortest_decimal" => Ok(NumericEncodingV1::BinaryFloat32ShortestDecimal),
        "unspecified" => Err(MarketDataError::Contract),
        _ => Err(MarketDataError::Contract),
    }
}

fn optional_decimal_value(array: &StringArray, index: usize) -> Result<Option<DecimalString>> {
    optional_string_value(array, index)
        .map(|value| DecimalString::new(value.to_owned()).map_err(|_| MarketDataError::Contract))
        .transpose()
}

fn optional_string_value(array: &StringArray, index: usize) -> Option<&str> {
    (!array.is_null(index)).then(|| array.value(index))
}

fn optional_timestamp_value(
    array: &TimestampNanosecondArray,
    index: usize,
) -> Result<Option<UtcTimestamp>> {
    if array.is_null(index) {
        Ok(None)
    } else {
        timestamp_value(array, index).map(Some)
    }
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

fn write_batch_atomic(path: &Path, schema: SchemaRef, batch: &RecordBatch) -> Result<()> {
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
    let mut writer = ArrowWriter::try_new(
        file,
        Arc::clone(&schema),
        Some(WriterProperties::builder().build()),
    )
    .map_err(|_| MarketDataError::Parquet)?;
    writer.write(batch).map_err(|_| MarketDataError::Parquet)?;
    writer.close().map_err(|_| MarketDataError::Parquet)?;
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
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn valid_bar() -> TradeMinuteBarV1 {
        TradeMinuteBarV1 {
            schema_version: 1,
            source_provider: "synthetic".into(),
            source_feed: "synthetic".into(),
            source_entitlement: "unknown".into(),
            source_numeric_encoding: "decimal_token".into(),
            symbol: "QQQ".into(),
            bar_start_utc: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
            bar_end_exclusive_utc: UtcTimestamp::parse("2026-10-08T13:31:00Z").unwrap(),
            available_at_utc: UtcTimestamp::parse("2026-10-08T13:31:00Z").unwrap(),
            trade_date: "2026-10-08".into(),
            session_id: "test-regular-session".into(),
            session_timezone: "America/New_York".into(),
            session_policy_id: "test-session-policy-v1".into(),
            session_policy_sha256: "a".repeat(64),
            session_start_utc: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
            session_end_exclusive_utc: UtcTimestamp::parse("2026-10-08T20:00:00Z").unwrap(),
            window_start_utc: UtcTimestamp::parse("2026-10-08T13:30:00Z").unwrap(),
            window_end_exclusive_utc: UtcTimestamp::parse("2026-10-08T13:31:00Z").unwrap(),
            open: "600.25".into(),
            high: "600.25".into(),
            low: "600.25".into(),
            close: "600.25".into(),
            volume: "2".into(),
            trade_count: 1,
            quote_events_excluded: 0,
            source_timestamp_missing_rows: 0,
            sequence_gap_count: 0,
            late_event_count: 0,
            window_expected_minutes: 1,
            window_empty_trade_minutes: 0,
            source_start_utc: UtcTimestamp::parse("2026-10-08T13:30:15Z").unwrap(),
            source_end_exclusive_utc: UtcTimestamp::parse("2026-10-08T13:30:15.000000001Z")
                .unwrap(),
            window_input_eof: true,
            source_pages_exhausted: None,
            completion_mode: "synthetic_eof".into(),
            nbbo_input_status: "excluded".into(),
        }
    }

    #[test]
    fn write_verify_and_query_share_semantic_bar_validation() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("bars.parquet");
        let verification = write_bars(&path, &[valid_bar()]).unwrap();
        assert_eq!(verification.footer_rows, 1);
        assert_eq!(verification.source.provider, "synthetic");
        assert_eq!(verification.symbols, ["QQQ"]);
        assert_eq!(query_bars(&path, Some("QQQ")).unwrap(), [valid_bar()]);
        assert!(query_bars(&path, Some("SPY")).unwrap().is_empty());

        let mut bad_decimal = valid_bar();
        bad_decimal.open = "600.2oops".into();
        let mut bad_date = valid_bar();
        bad_date.trade_date = "2026-02-30".into();
        let mut bad_version = valid_bar();
        bad_version.schema_version = 2;
        let mut bad_range = valid_bar();
        bad_range.source_end_exclusive_utc = UtcTimestamp::parse("2026-10-08T13:30:14Z").unwrap();
        let mut bad_missing_count = valid_bar();
        bad_missing_count.source_timestamp_missing_rows = 1;
        let mut bad_symbol = valid_bar();
        bad_symbol.symbol = "QQQ\nspoof".into();
        let mut bad_session_id = valid_bar();
        bad_session_id.session_id = "s".repeat(257);
        let mut bad_policy_id = valid_bar();
        bad_policy_id.session_policy_id = "policy\nspoof".into();

        for (index, row) in [
            bad_decimal,
            bad_date,
            bad_version,
            bad_range,
            bad_missing_count,
            bad_symbol,
            bad_session_id,
            bad_policy_id,
        ]
        .into_iter()
        .enumerate()
        {
            let rejected_path = temp.path().join(format!("invalid-{index}.parquet"));
            assert!(matches!(
                write_bars(&rejected_path, &[row]),
                Err(MarketDataError::Contract)
            ));
            assert!(!rejected_path.exists());
        }
    }

    #[test]
    fn nonpaged_historical_bar_roundtrips_without_fabricated_page_evidence() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("historical-nonpaged.parquet");
        let mut bar = valid_bar();
        bar.source_provider = "fixture-provider".into();
        bar.source_feed = "sip".into();
        bar.completion_mode = "historical_eof_nonpaged".into();

        let verification = write_bars(&path, &[bar.clone()]).unwrap();
        assert_eq!(verification.footer_rows, 1);
        assert_eq!(query_bars(&path, None).unwrap(), [bar]);

        let paged_path = temp.path().join("historical-paged.parquet");
        let mut paged_bar = valid_bar();
        paged_bar.source_provider = "fixture-provider".into();
        paged_bar.source_feed = "sip".into();
        paged_bar.source_pages_exhausted = Some(true);
        paged_bar.completion_mode = "historical_eof_paged".into();
        write_bars(&paged_path, &[paged_bar.clone()]).unwrap();
        assert_eq!(query_bars(&paged_path, None).unwrap(), [paged_bar]);

        let mut legacy_ambiguous = valid_bar();
        legacy_ambiguous.source_provider = "fixture-provider".into();
        legacy_ambiguous.source_feed = "sip".into();
        legacy_ambiguous.completion_mode = "historical_eof".into();
        assert!(validate_bar_row(&legacy_ambiguous).is_err());
    }
}
