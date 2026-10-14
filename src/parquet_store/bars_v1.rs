//! Core-owned market-bars V1 rows in their trusted Parquet schema.

use std::{fs::File, sync::Arc};

use arrow_array::{
    Array, ArrayRef, RecordBatch, StringArray, TimestampNanosecondArray, UInt32Array,
};
use market_contracts::{
    BARS_SCHEMA_VERSION_V1, BarRowV1, BarsResponseV1, DecimalString, ProtoTimestampV2,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::de::DeserializeOwned;

use crate::{MarketDataError, Result};

use super::{
    arrow_schema,
    metadata::{
        bars_v1_source_from_metadata, bars_v1_source_metadata, validate_decode_budget,
        validate_optional_schema_metadata,
    },
    projection::{string_array, timestamp_array, timestamp_value, u32_value, value_string},
    validate_arrow_schema, validate_no_unexpected_nulls, write_batch_atomic_with_extra_metadata,
};

pub(super) fn write_response_object_with_limit(
    path: &std::path::Path,
    response: &BarsResponseV1,
    max_bytes: u64,
) -> Result<()> {
    response.validate().map_err(|_| MarketDataError::Contract)?;
    let rows = &response.rows;
    if rows.is_empty() || rows.len() > 1000 || max_bytes == 0 {
        return Err(MarketDataError::InvalidInput);
    }
    let schema_id = crate::schema::MARKET_BARS_SCHEMA_V1_ID;
    let schema = Arc::new(arrow_schema(schema_id)?);
    validate_arrow_schema(schema_id, &schema)?;

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(vec![BARS_SCHEMA_VERSION_V1; rows.len()])),
        string_array(rows.iter().map(|row| row.symbol.as_str())),
        enum_array(rows.iter().map(|row| &row.asset_class))?,
        enum_array(rows.iter().map(|row| &row.resolution))?,
        enum_array(rows.iter().map(|row| &row.adjustment))?,
        timestamp_array(rows.iter().map(|row| Some(row.bar_start_utc.as_str())))?,
        timestamp_array(
            rows.iter()
                .map(|row| Some(row.bar_end_exclusive_utc.as_str())),
        )?,
        timestamp_array(rows.iter().map(|row| Some(row.received_at_utc.as_str())))?,
        timestamp_array(rows.iter().map(|row| Some(row.available_at_utc.as_str())))?,
        string_array(rows.iter().map(|row| row.open.as_str())),
        string_array(rows.iter().map(|row| row.high.as_str())),
        string_array(rows.iter().map(|row| row.low.as_str())),
        string_array(rows.iter().map(|row| row.close.as_str())),
        string_array(rows.iter().map(|row| row.volume.as_str())),
        string_array(rows.iter().map(|row| row.provenance_id.as_str())),
        string_array(rows.iter().map(|row| row.session_instance_id.as_str())),
        enum_array(rows.iter().map(|row| &row.validation_state))?,
    ];
    let batch =
        RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|_| MarketDataError::Parquet)?;
    write_batch_atomic_with_extra_metadata(
        path,
        schema,
        &batch,
        max_bytes,
        super::default_compression(),
        schema_id,
        bars_v1_source_metadata(response)?,
    )
}

/// Decode typed rows after the caller has placed this work inside the bounded Parquet worker.
pub(super) fn read_rows_with_limit(
    path: &std::path::Path,
    max_bytes: u64,
    max_rows: usize,
    max_result_bytes: u64,
) -> Result<Vec<BarRowV1>> {
    if max_bytes == 0 || max_rows == 0 || max_rows > 1000 || max_result_bytes == 0 {
        return Err(MarketDataError::InvalidInput);
    }
    if std::fs::metadata(path)?.len() > max_bytes {
        return Err(MarketDataError::InputLimit);
    }
    let schema_id = crate::schema::MARKET_BARS_SCHEMA_V1_ID;
    let file = File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|_| MarketDataError::Parquet)?;
    validate_decode_budget(builder.metadata().as_ref())?;
    validate_arrow_schema(schema_id, builder.schema().as_ref())?;
    validate_optional_schema_metadata(schema_id, builder.metadata().as_ref(), builder.schema())?;
    let footer_rows = usize::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| MarketDataError::InputLimit)?;
    if footer_rows == 0 || footer_rows > max_rows {
        return Err(MarketDataError::InputLimit);
    }
    let mut reader = builder
        .with_batch_size(super::PARQUET_READ_BATCH_ROWS)
        .build()
        .map_err(|_| MarketDataError::Parquet)?;
    let mut rows = Vec::with_capacity(footer_rows);
    for batch in &mut reader {
        let batch = batch.map_err(|_| MarketDataError::Parquet)?;
        rows.extend(decode_batch(&batch)?);
        if rows.len() > max_rows {
            return Err(MarketDataError::InputLimit);
        }
    }
    if rows.len() != footer_rows {
        return Err(MarketDataError::Parquet);
    }
    let encoded_bytes = serde_json::to_vec(&rows)?.len();
    if u64::try_from(encoded_bytes).map_err(|_| MarketDataError::InputLimit)? > max_result_bytes {
        return Err(MarketDataError::InputLimit);
    }
    Ok(rows)
}

pub(super) fn decode_batch(batch: &RecordBatch) -> Result<Vec<BarRowV1>> {
    validate_no_unexpected_nulls(batch)?;
    let index = |name: &str| {
        batch
            .schema()
            .index_of(name)
            .map_err(|_| MarketDataError::Parquet)
    };
    let strings = |name: &str| super::string_column(batch, index(name)?);
    let timestamps = |name: &str| -> Result<&TimestampNanosecondArray> {
        batch
            .column(index(name)?)
            .as_any()
            .downcast_ref()
            .ok_or(MarketDataError::Parquet)
    };
    let schema_versions = batch
        .column(index("schema_version")?)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or(MarketDataError::Parquet)?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        if u32_value(schema_versions, row_index)? != BARS_SCHEMA_VERSION_V1 {
            return Err(MarketDataError::ParquetSchema);
        }
        rows.push(BarRowV1 {
            symbol: value_string(strings("symbol")?, row_index)?.to_owned(),
            asset_class: parse_enum(value_string(strings("asset_class")?, row_index)?)?,
            resolution: parse_enum(value_string(strings("resolution")?, row_index)?)?,
            adjustment: parse_enum(value_string(strings("adjustment")?, row_index)?)?,
            bar_start_utc: proto_timestamp(
                timestamp_value(timestamps("bar_start_utc")?, row_index)?.as_str(),
            )?,
            bar_end_exclusive_utc: proto_timestamp(
                timestamp_value(timestamps("bar_end_exclusive_utc")?, row_index)?.as_str(),
            )?,
            received_at_utc: proto_timestamp(
                timestamp_value(timestamps("received_at_utc")?, row_index)?.as_str(),
            )?,
            available_at_utc: proto_timestamp(
                timestamp_value(timestamps("available_at_utc")?, row_index)?.as_str(),
            )?,
            open: decimal_value(strings("open")?, row_index)?,
            high: decimal_value(strings("high")?, row_index)?,
            low: decimal_value(strings("low")?, row_index)?,
            close: decimal_value(strings("close")?, row_index)?,
            volume: decimal_value(strings("volume")?, row_index)?,
            provenance_id: value_string(strings("provenance_id")?, row_index)?.to_owned(),
            session_instance_id: value_string(strings("session_instance_id")?, row_index)?
                .to_owned(),
            validation_state: parse_enum(value_string(strings("validation_state")?, row_index)?)?,
        });
    }
    Ok(rows)
}

fn enum_array<'a, T: serde::Serialize + 'a>(
    values: impl Iterator<Item = &'a T>,
) -> Result<ArrayRef> {
    let values = values
        .map(|value| {
            serde_json::to_value(value)
                .map_err(MarketDataError::from)?
                .as_str()
                .map(str::to_owned)
                .ok_or(MarketDataError::Contract)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StringArray::from_iter_values(values)))
}

fn parse_enum<T: DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|_| MarketDataError::Contract)
}

pub(super) fn source_from_footer(
    values: Option<&Vec<parquet::file::metadata::KeyValue>>,
) -> Result<market_contracts::MarketDataSourceV1> {
    bars_v1_source_from_metadata(values)
}

fn decimal_value(array: &StringArray, index: usize) -> Result<DecimalString> {
    DecimalString::new(value_string(array, index)?.to_owned())
        .map_err(|_| MarketDataError::Contract)
}

fn proto_timestamp(value: &str) -> Result<ProtoTimestampV2> {
    ProtoTimestampV2::parse(value).map_err(|_| MarketDataError::Contract)
}

#[cfg(test)]
mod tests {
    use super::*;
    use market_contracts::{
        AdjustmentV1, AssetClassV1, BarCoverageSegmentV1, BarResolutionV1, BarSourceProvenanceV1,
        BarsCacheStatusV1, BarsCoverageStatusV1, BarsEntitlementStatusV1, BarsValidationStateV1,
        NumericEncodingV1,
    };

    fn response() -> BarsResponseV1 {
        let timestamp = |value| ProtoTimestampV2::parse(value).unwrap();
        BarsResponseV1 {
            schema_version: BARS_SCHEMA_VERSION_V1,
            provider: "alpaca".to_owned(),
            feed: "sip".to_owned(),
            entitlement_status: BarsEntitlementStatusV1::Authorized,
            requested_resolution: BarResolutionV1::OneMinute,
            effective_resolution: BarResolutionV1::OneMinute,
            adjustment: AdjustmentV1::Unadjusted,
            rows: vec![BarRowV1 {
                symbol: "QQQ".to_owned(),
                asset_class: AssetClassV1::Etf,
                resolution: BarResolutionV1::OneMinute,
                adjustment: AdjustmentV1::Unadjusted,
                bar_start_utc: timestamp("2026-10-08T13:30:00.123456789Z"),
                bar_end_exclusive_utc: timestamp("2026-10-08T13:31:00Z"),
                received_at_utc: timestamp("2026-10-08T13:31:00Z"),
                available_at_utc: timestamp("2026-10-08T13:31:00.000000001Z"),
                open: DecimalString::new("600.250000000000000001").unwrap(),
                high: DecimalString::new("601.00").unwrap(),
                low: DecimalString::new("599.50").unwrap(),
                close: DecimalString::new("600.75").unwrap(),
                volume: DecimalString::new("12345678901234567890").unwrap(),
                provenance_id: "source-1".to_owned(),
                session_instance_id: "2026-10-08-regular".to_owned(),
                validation_state: BarsValidationStateV1::Validated,
            }],
            coverage_status: BarsCoverageStatusV1::Complete,
            segments: vec![BarCoverageSegmentV1 {
                start_utc: timestamp("2026-10-08T13:30:00Z"),
                end_exclusive_utc: timestamp("2026-10-08T13:31:00Z"),
                coverage_status: BarsCoverageStatusV1::Complete,
                provenance_id: Some("source-1".to_owned()),
                session_instance_id: "2026-10-08-regular".to_owned(),
            }],
            gaps: Vec::new(),
            source_provenance: vec![BarSourceProvenanceV1 {
                provenance_id: "source-1".to_owned(),
                provider: "alpaca".to_owned(),
                feed: "sip".to_owned(),
                source: "Alpaca SIP".to_owned(),
                entitlement_status: BarsEntitlementStatusV1::Authorized,
                numeric_encoding: NumericEncodingV1::DecimalToken,
            }],
            as_of: None,
            fetched_at: timestamp("2026-10-08T13:31:01Z"),
            verified_at: None,
            cache_status: BarsCacheStatusV1::Miss,
            market_calendar_version: Some("XNYS-2026a".to_owned()),
            continuation_cursor: None,
            truncated: false,
        }
    }

    #[test]
    fn rows_keep_nanosecond_timestamps_and_exact_decimal_tokens() {
        let response = response();
        response.validate().unwrap();
        let schema_id = crate::schema::MARKET_BARS_SCHEMA_V1_ID;
        let schema = super::super::arrow_schema(schema_id).unwrap();
        assert_eq!(schema.fields().len(), 17);
        let row = &response.rows[0];
        assert_eq!(row.open.as_str(), "600.250000000000000001");
        assert_eq!(row.volume.as_str(), "12345678901234567890");
        assert_eq!(row.bar_start_utc.as_str(), "2026-10-08T13:30:00.123456789Z");
    }

    #[test]
    fn unresolved_source_references_are_rejected_by_core_response_validation() {
        let mut value = response();
        value.rows[0].provenance_id = "missing-source".to_owned();
        assert!(value.validate().is_err());
    }
}
