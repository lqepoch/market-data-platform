use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt32Array,
    UInt64Array,
};
use chrono::{DateTime, Utc};
use market_contracts::{MarketEventEnvelopeV1, MarketEventV1, UtcTimestamp};

use crate::{MarketDataError, Result};

pub(super) fn validate_no_unexpected_nulls(batch: &RecordBatch) -> Result<()> {
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if !field.is_nullable() && column.null_count() != 0 {
            return Err(MarketDataError::ParquetSchema);
        }
    }
    Ok(())
}

pub(super) fn string_array<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

pub(super) fn nullable_string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>()))
}

pub(super) fn owned_string_array(values: impl Iterator<Item = String>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

pub(super) fn timestamp_array<'a>(
    values: impl Iterator<Item = Option<&'a str>>,
) -> Result<ArrayRef> {
    let values = values
        .map(|value| value.map(timestamp_to_ns).transpose())
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(
        TimestampNanosecondArray::from(values).with_timezone("UTC"),
    ))
}

pub(super) fn timestamp_to_ns(value: &str) -> Result<i64> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|_| MarketDataError::Contract)?
        .timestamp_nanos_opt()
        .ok_or(MarketDataError::InvalidInput)
}

pub(super) fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn valid_metadata_label(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

pub(super) fn timestamp_from_ns(value: i64) -> Result<UtcTimestamp> {
    let seconds = value.div_euclid(1_000_000_000);
    let nanos = value.rem_euclid(1_000_000_000) as u32;
    let timestamp =
        DateTime::<Utc>::from_timestamp(seconds, nanos).ok_or(MarketDataError::InvalidInput)?;
    UtcTimestamp::parse(&timestamp.to_rfc3339()).map_err(|_| MarketDataError::Contract)
}

pub(super) struct ProjectedEvent<'a> {
    pub(super) kind: &'static str,
    pub(super) symbol: &'a str,
    pub(super) price: Option<String>,
    pub(super) size: Option<String>,
    pub(super) bid: Option<String>,
    pub(super) ask: Option<String>,
    pub(super) bid_size: Option<String>,
    pub(super) ask_size: Option<String>,
}

pub(super) fn project_event(event: &MarketEventEnvelopeV1) -> Result<ProjectedEvent<'_>> {
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

pub(super) fn enum_string<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

pub(super) fn string_column(batch: &RecordBatch, index: usize) -> Result<&StringArray> {
    batch
        .column(index)
        .as_any()
        .downcast_ref()
        .ok_or(MarketDataError::Parquet)
}

pub(super) fn value_string(array: &StringArray, index: usize) -> Result<&str> {
    if array.is_null(index) {
        return Err(MarketDataError::ParquetSchema);
    }
    Ok(array.value(index))
}

pub(super) fn timestamp_value(
    array: &TimestampNanosecondArray,
    index: usize,
) -> Result<UtcTimestamp> {
    if array.is_null(index) {
        return Err(MarketDataError::ParquetSchema);
    }
    timestamp_from_ns(array.value(index))
}

pub(super) fn u64_value(array: &UInt64Array, index: usize) -> Result<u64> {
    if array.is_null(index) {
        return Err(MarketDataError::ParquetSchema);
    }
    Ok(array.value(index))
}

pub(super) fn u32_value(array: &UInt32Array, index: usize) -> Result<u32> {
    if array.is_null(index) {
        return Err(MarketDataError::ParquetSchema);
    }
    Ok(array.value(index))
}

pub(super) fn bool_value(array: &BooleanArray, index: usize) -> Result<bool> {
    if array.is_null(index) {
        return Err(MarketDataError::ParquetSchema);
    }
    Ok(array.value(index))
}
