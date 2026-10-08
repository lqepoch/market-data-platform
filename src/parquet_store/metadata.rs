use std::collections::HashMap;

use arrow_schema::Schema;
use parquet::{
    basic::Compression,
    file::{
        metadata::{KeyValue, ParquetMetaData},
        properties::WriterProperties,
    },
};

use crate::{MarketDataError, Result};

use super::{
    MAX_PARQUET_ROW_GROUP_UNCOMPRESSED_BYTES, MAX_PARQUET_ROW_GROUPS,
    MAX_PARQUET_TOTAL_UNCOMPRESSED_BYTES, PARQUET_DATA_PAGE_BYTES, PARQUET_ROW_GROUP_ROWS,
    SCHEMA_DESCRIPTOR_METADATA_KEY, SCHEMA_FINGERPRINT_METADATA_KEY,
};

pub(super) fn trusted_schema_metadata(schema_id: &str) -> Result<HashMap<String, String>> {
    let descriptor = market_contracts::parquet_schema::trusted_parquet_schema(schema_id)
        .map_err(|_| MarketDataError::Contract)?;
    let canonical_json = descriptor
        .canonical_json()
        .map_err(|_| MarketDataError::Contract)?;
    let fingerprint = descriptor
        .fingerprint_sha256()
        .map_err(|_| MarketDataError::Contract)?;
    Ok(HashMap::from([
        (SCHEMA_DESCRIPTOR_METADATA_KEY.to_owned(), canonical_json),
        (SCHEMA_FINGERPRINT_METADATA_KEY.to_owned(), fingerprint),
    ]))
}

pub(super) fn validate_optional_schema_metadata(
    schema_id: &str,
    parquet_metadata: &ParquetMetaData,
    arrow_schema: &Schema,
) -> Result<()> {
    let expected = trusted_schema_metadata(schema_id)?;
    let validate_map = |actual: &HashMap<String, String>| -> Result<()> {
        let descriptor = actual.get(SCHEMA_DESCRIPTOR_METADATA_KEY);
        let fingerprint = actual.get(SCHEMA_FINGERPRINT_METADATA_KEY);
        match (descriptor, fingerprint) {
            (None, None) => Ok(()),
            (Some(actual_descriptor), Some(actual_fingerprint))
                if expected.get(SCHEMA_DESCRIPTOR_METADATA_KEY) == Some(actual_descriptor)
                    && expected.get(SCHEMA_FINGERPRINT_METADATA_KEY)
                        == Some(actual_fingerprint) =>
            {
                Ok(())
            }
            _ => Err(MarketDataError::ParquetSchema),
        }
    };

    let arrow_metadata = arrow_schema
        .metadata()
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    validate_map(&arrow_metadata)?;
    let mut footer_metadata = HashMap::new();
    if let Some(entries) = parquet_metadata.file_metadata().key_value_metadata() {
        for entry in entries {
            if entry.key == "ARROW:schema" {
                continue;
            }
            if entry.key == SCHEMA_DESCRIPTOR_METADATA_KEY
                || entry.key == SCHEMA_FINGERPRINT_METADATA_KEY
            {
                let value = entry.value.as_ref().ok_or(MarketDataError::ParquetSchema)?;
                if footer_metadata
                    .insert(entry.key.as_str(), value.as_str())
                    .is_some()
                {
                    return Err(MarketDataError::ParquetSchema);
                }
            }
        }
    }
    let footer_map = footer_metadata
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect::<HashMap<_, _>>();
    validate_map(&footer_map)
}

pub(super) fn writer_properties(
    compression: Compression,
    metadata: Vec<KeyValue>,
) -> WriterProperties {
    WriterProperties::builder()
        .set_key_value_metadata(Some(metadata))
        .set_compression(compression)
        .set_max_row_group_row_count(Some(PARQUET_ROW_GROUP_ROWS))
        .set_data_page_size_limit(PARQUET_DATA_PAGE_BYTES)
        .set_dictionary_page_size_limit(PARQUET_DATA_PAGE_BYTES)
        .build()
}

pub(super) fn validate_decode_budget(metadata: &ParquetMetaData) -> Result<()> {
    if metadata.num_row_groups() == 0 || metadata.num_row_groups() > MAX_PARQUET_ROW_GROUPS {
        return Err(MarketDataError::InputLimit);
    }
    let mut total_uncompressed_bytes = 0_u64;
    for row_group in metadata.row_groups() {
        let advertised_group_bytes =
            u64::try_from(row_group.total_byte_size()).map_err(|_| MarketDataError::Parquet)?;
        if advertised_group_bytes == 0
            || advertised_group_bytes > MAX_PARQUET_ROW_GROUP_UNCOMPRESSED_BYTES
        {
            return Err(MarketDataError::InputLimit);
        }
        let column_bytes = row_group.columns().iter().try_fold(0_u64, |sum, column| {
            let bytes =
                u64::try_from(column.uncompressed_size()).map_err(|_| MarketDataError::Parquet)?;
            sum.checked_add(bytes).ok_or(MarketDataError::InputLimit)
        })?;
        if column_bytes != advertised_group_bytes {
            return Err(MarketDataError::Parquet);
        }
        total_uncompressed_bytes = total_uncompressed_bytes
            .checked_add(advertised_group_bytes)
            .ok_or(MarketDataError::InputLimit)?;
        if total_uncompressed_bytes > MAX_PARQUET_TOTAL_UNCOMPRESSED_BYTES {
            return Err(MarketDataError::InputLimit);
        }
    }
    Ok(())
}
