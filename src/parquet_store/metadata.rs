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

const BARS_V1_SOURCE_PROVIDER_KEY: &str = "lqepoch.bars_v1.source.provider";
const BARS_V1_SOURCE_FEED_KEY: &str = "lqepoch.bars_v1.source.feed";
const BARS_V1_SOURCE_ENTITLEMENT_KEY: &str = "lqepoch.bars_v1.source.entitlement";
const BARS_V1_SOURCE_ENCODING_KEY: &str = "lqepoch.bars_v1.source.numeric_encoding";

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

pub(super) fn bars_v1_source_metadata(
    response: &market_contracts::BarsResponseV1,
) -> Result<Vec<KeyValue>> {
    use market_contracts::BarsEntitlementStatusV1;

    let first = response
        .source_provenance
        .first()
        .ok_or(MarketDataError::Contract)?;
    if response.source_provenance.iter().any(|source| {
        source.provider != first.provider
            || source.feed != first.feed
            || source.entitlement_status != first.entitlement_status
            || source.numeric_encoding != first.numeric_encoding
    }) {
        return Err(MarketDataError::MixedProvenance);
    }
    let entitlement = match first.entitlement_status {
        BarsEntitlementStatusV1::Authorized => "authorized",
        BarsEntitlementStatusV1::Unauthorized => "unauthorized",
        BarsEntitlementStatusV1::Unknown => "unknown",
        BarsEntitlementStatusV1::Mixed => return Err(MarketDataError::MixedProvenance),
    };
    Ok(vec![
        KeyValue::new(
            BARS_V1_SOURCE_PROVIDER_KEY.to_owned(),
            Some(first.provider.clone()),
        ),
        KeyValue::new(BARS_V1_SOURCE_FEED_KEY.to_owned(), Some(first.feed.clone())),
        KeyValue::new(
            BARS_V1_SOURCE_ENTITLEMENT_KEY.to_owned(),
            Some(entitlement.to_owned()),
        ),
        KeyValue::new(
            BARS_V1_SOURCE_ENCODING_KEY.to_owned(),
            Some(first.numeric_encoding.as_str().to_owned()),
        ),
    ])
}

pub(super) fn bars_v1_source_from_metadata(
    values: Option<&Vec<KeyValue>>,
) -> Result<market_contracts::MarketDataSourceV1> {
    use market_contracts::{EntitlementState, NumericEncodingV1};

    let mut source = HashMap::new();
    if let Some(values) = values {
        for entry in values {
            if entry.key.starts_with("lqepoch.bars_v1.source.") {
                let value = entry
                    .value
                    .as_deref()
                    .ok_or(MarketDataError::ParquetSchema)?;
                if source.insert(entry.key.as_str(), value).is_some() {
                    return Err(MarketDataError::ParquetSchema);
                }
            }
        }
    }
    if source.len() != 4 {
        return Err(MarketDataError::ParquetSchema);
    }
    let get = |key| {
        source
            .get(key)
            .copied()
            .ok_or(MarketDataError::ParquetSchema)
    };
    let entitlement = match get(BARS_V1_SOURCE_ENTITLEMENT_KEY)? {
        "authorized" => EntitlementState::Authorized,
        "unauthorized" => EntitlementState::Unauthorized,
        "unknown" => EntitlementState::Unknown,
        _ => return Err(MarketDataError::ParquetSchema),
    };
    let numeric_encoding: NumericEncodingV1 = serde_json::from_value(serde_json::Value::String(
        get(BARS_V1_SOURCE_ENCODING_KEY)?.to_owned(),
    ))
    .map_err(|_| MarketDataError::ParquetSchema)?;
    market_contracts::MarketDataSourceV1::new(
        get(BARS_V1_SOURCE_PROVIDER_KEY)?.to_owned(),
        get(BARS_V1_SOURCE_FEED_KEY)?.to_owned(),
        entitlement,
        numeric_encoding,
        None,
    )
    .map_err(|_| MarketDataError::ParquetSchema)
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
