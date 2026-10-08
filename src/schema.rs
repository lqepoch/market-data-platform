//! Arrow-to-core Parquet schema contract validation.
//!
//! `trading-core::market-contracts` owns descriptors and fingerprints. This module only maps the
//! trusted logical types to Arrow types and rejects physical schema drift before write/read.

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use market_contracts::parquet_schema::{
    MARKET_EVENT_PARQUET_SCHEMA_ID, MARKET_EVENT_PARQUET_SCHEMA_V2_ID,
    MARKET_EVENT_PARQUET_SCHEMA_V3_ID, MARKET_RAW_FRAME_PARQUET_SCHEMA_ID,
    MARKET_RAW_FRAME_PARQUET_SCHEMA_V2_ID, MARKET_RAW_JSON_FRAME_PARQUET_SCHEMA_V2_ID,
    ParquetSchemaDescriptorV1, ParquetSchemaError, US_EQUITY_TRADE_BAR_1M_SCHEMA_ID,
    trusted_parquet_schema, trusted_schema_fingerprint,
};

use crate::{MarketDataError, Result};

pub const EVENT_SCHEMA_ID: &str = MARKET_EVENT_PARQUET_SCHEMA_ID;
pub const EVENT_SCHEMA_V2_ID: &str = MARKET_EVENT_PARQUET_SCHEMA_V2_ID;
pub const EVENT_SCHEMA_V3_ID: &str = MARKET_EVENT_PARQUET_SCHEMA_V3_ID;
pub const RAW_FRAME_SCHEMA_ID: &str = MARKET_RAW_FRAME_PARQUET_SCHEMA_ID;
pub const RAW_FRAME_SCHEMA_V2_ID: &str = MARKET_RAW_FRAME_PARQUET_SCHEMA_V2_ID;
pub const RAW_JSON_FRAME_SCHEMA_V2_ID: &str = MARKET_RAW_JSON_FRAME_PARQUET_SCHEMA_V2_ID;
pub const MINUTE_BAR_SCHEMA_ID: &str = US_EQUITY_TRADE_BAR_1M_SCHEMA_ID;

pub fn descriptor(schema_id: &str) -> Result<ParquetSchemaDescriptorV1> {
    trusted_parquet_schema(schema_id).map_err(map_schema_error)
}

pub fn fingerprint(schema_id: &str, schema: &Schema) -> Result<String> {
    validate_arrow_schema(schema_id, schema)?;
    trusted_schema_fingerprint(schema_id).map_err(map_schema_error)
}

pub fn arrow_schema(schema_id: &str) -> Result<Schema> {
    let descriptor = descriptor(schema_id)?;
    let fields = descriptor
        .fields
        .iter()
        .map(|field| {
            Ok(Field::new(
                &field.name,
                arrow_type(&field.logical_type)?,
                field.nullable,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

pub fn validate_arrow_schema(schema_id: &str, schema: &Schema) -> Result<()> {
    let descriptor = descriptor(schema_id)?;
    if schema.fields().len() != descriptor.fields.len() {
        return Err(MarketDataError::ParquetSchema);
    }
    for (actual, expected) in schema.fields().iter().zip(&descriptor.fields) {
        if actual.name() != &expected.name
            || actual.is_nullable() != expected.nullable
            || actual.data_type() != &arrow_type(&expected.logical_type)?
        {
            return Err(MarketDataError::ParquetSchema);
        }
    }
    Ok(())
}

fn arrow_type(logical: &str) -> Result<DataType> {
    match logical {
        "utf8" | "decimal_string" | "date_iso8601" | "sha256_hex" => Ok(DataType::Utf8),
        "binary" => Ok(DataType::Binary),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "bool" => Ok(DataType::Boolean),
        "timestamp_ns_utc" => Ok(DataType::Timestamp(
            TimeUnit::Nanosecond,
            Some("UTC".into()),
        )),
        _ => Err(MarketDataError::ParquetSchema),
    }
}

fn map_schema_error(error: ParquetSchemaError) -> MarketDataError {
    match error {
        ParquetSchemaError::UnknownSchemaId | ParquetSchemaError::TrustedSchemaMismatch => {
            MarketDataError::ParquetSchema
        }
        _ => MarketDataError::Contract,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_registered_schema_golden_hashes_are_consumed() {
        let events = arrow_schema(EVENT_SCHEMA_ID).unwrap();
        let bars = arrow_schema(MINUTE_BAR_SCHEMA_ID).unwrap();
        assert_eq!(
            fingerprint(EVENT_SCHEMA_ID, &events).unwrap(),
            "d02712364494c7c60e60e8e6e2e05fb4d63fd5944f67e7e4af0f45f7408875f6"
        );
        assert_eq!(
            fingerprint(MINUTE_BAR_SCHEMA_ID, &bars).unwrap(),
            "5e761a91d880e0002aeafe6dc2083b7c8a0ff2ba486d5d93582fbb4479146cb0"
        );
    }

    #[test]
    fn physical_arrow_schema_must_match_trusted_descriptor() {
        let mut fields = arrow_schema(MINUTE_BAR_SCHEMA_ID)
            .unwrap()
            .fields()
            .to_vec();
        fields.swap(0, 1);
        assert!(validate_arrow_schema(MINUTE_BAR_SCHEMA_ID, &Schema::new(fields)).is_err());
    }
}
