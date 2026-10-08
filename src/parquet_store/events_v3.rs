//! Normalized event rows with capture UUID and source-generation provenance.

use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray, UInt32Array, UInt64Array};
use market_contracts::{MarketEventParquetRowV3, RawFrameCaptureInstanceIdV2, RawFrameReferenceV3};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::{
    PARQUET_READ_BATCH_ROWS, ParquetVerification, arrow_schema, default_compression,
    validate_arrow_schema, validate_decode_budget, validate_no_unexpected_nulls,
};
use crate::{MarketDataError, Result, schema::EVENT_SCHEMA_V3_ID};

pub(super) fn write_events(
    path: &Path,
    rows: &[MarketEventParquetRowV3],
    max_object_bytes: u64,
) -> Result<ParquetVerification> {
    if rows.is_empty() || rows.len() > crate::protocol::DEFAULT_MAX_JSONL_RECORDS {
        return Err(MarketDataError::InputLimit);
    }
    for row in rows {
        row.validate().map_err(|_| MarketDataError::Contract)?;
        if row.raw_frame_reference.is_none() {
            return Err(MarketDataError::Contract);
        }
    }

    let schema = std::sync::Arc::new(arrow_schema(EVENT_SCHEMA_V3_ID)?);
    validate_arrow_schema(EVENT_SCHEMA_V3_ID, &schema)?;
    let events = rows.iter().map(|row| &row.event).collect::<Vec<_>>();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(UInt32Array::from(
        events
            .iter()
            .map(|event| event.metadata.schema_version)
            .collect::<Vec<_>>(),
    )));
    columns.push(super::string_array(
        events
            .iter()
            .map(|event| event.metadata.source.provider.as_str()),
    ));
    columns.push(super::string_array(
        events
            .iter()
            .map(|event| event.metadata.source.feed.as_str()),
    ));
    columns.push(super::owned_string_array(
        events
            .iter()
            .map(|event| super::enum_string(&event.metadata.source.entitlement)),
    ));
    columns.push(super::owned_string_array(events.iter().map(|event| {
        super::enum_string(&event.metadata.source.numeric_encoding)
    })));
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
    columns.push(super::timestamp_array(events.iter().map(|event| {
        event
            .metadata
            .source_timestamp
            .as_ref()
            .map(market_contracts::UtcTimestamp::as_str)
    }))?);
    columns.push(super::timestamp_array(
        events
            .iter()
            .map(|event| Some(event.metadata.received_timestamp.as_str())),
    )?);

    let projected = events
        .iter()
        .map(|event| super::project_event(event))
        .collect::<Result<Vec<_>>>()?;
    columns.push(super::string_array(projected.iter().map(|row| row.kind)));
    columns.push(super::string_array(projected.iter().map(|row| row.symbol)));
    columns.push(super::nullable_string_array(
        projected.iter().map(|row| row.price.as_deref()),
    ));
    columns.push(super::nullable_string_array(
        projected.iter().map(|row| row.size.as_deref()),
    ));
    columns.push(super::nullable_string_array(
        projected.iter().map(|row| row.bid.as_deref()),
    ));
    columns.push(super::nullable_string_array(
        projected.iter().map(|row| row.ask.as_deref()),
    ));
    columns.push(super::nullable_string_array(
        projected.iter().map(|row| row.bid_size.as_deref()),
    ));
    columns.push(super::nullable_string_array(
        projected.iter().map(|row| row.ask_size.as_deref()),
    ));
    columns.push(Arc::new(UInt64Array::from(
        rows.iter()
            .map(|row| {
                row.raw_frame_reference
                    .as_ref()
                    .map(|reference| reference.raw_frame_generation)
            })
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt64Array::from(
        rows.iter()
            .map(|row| {
                row.raw_frame_reference
                    .as_ref()
                    .map(|reference| reference.raw_frame_sequence)
            })
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt32Array::from(
        rows.iter()
            .map(|row| {
                row.raw_frame_reference
                    .as_ref()
                    .map(|reference| reference.raw_frame_event_ordinal)
            })
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt32Array::from(
        rows.iter()
            .map(|row| {
                row.raw_frame_reference
                    .as_ref()
                    .map(|reference| reference.raw_frame_event_count)
            })
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(StringArray::from(
        rows.iter()
            .map(|row| {
                row.raw_frame_reference
                    .as_ref()
                    .map(|reference| reference.raw_frame_capture_instance_id.as_str())
            })
            .collect::<Vec<_>>(),
    )));
    columns.push(Arc::new(UInt64Array::from(
        rows.iter()
            .map(|row| {
                row.raw_frame_reference
                    .as_ref()
                    .map(|reference| reference.raw_frame_source_generation)
            })
            .collect::<Vec<_>>(),
    )));

    let batch =
        RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|_| MarketDataError::Parquet)?;
    super::write_batch_atomic_with_compression(
        path,
        schema,
        &batch,
        max_object_bytes,
        default_compression(),
        EVENT_SCHEMA_V3_ID,
    )?;
    super::verify_with_limit(path, EVENT_SCHEMA_V3_ID, max_object_bytes)
}

pub(super) fn decode_batch(batch: &RecordBatch) -> Result<Vec<MarketEventParquetRowV3>> {
    let events = super::rows::decode_event_batch(batch)?;
    let col = |name: &str| -> Result<usize> {
        batch
            .schema()
            .index_of(name)
            .map_err(|_| MarketDataError::Parquet)
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
    let strings = |name: &str| -> Result<&StringArray> { super::string_column(batch, col(name)?) };
    let canonical_generation = u64s("raw_frame_generation")?;
    let source_sequence = u64s("raw_frame_sequence")?;
    let event_ordinal = u32s("raw_frame_event_ordinal")?;
    let event_count = u32s("raw_frame_event_count")?;
    let capture_id = strings("raw_frame_capture_instance_id")?;
    let source_generation = u64s("raw_frame_source_generation")?;
    let mut rows = Vec::with_capacity(events.len());
    for (index, event) in events.into_iter().enumerate() {
        let present = [
            !canonical_generation.is_null(index),
            !source_sequence.is_null(index),
            !event_ordinal.is_null(index),
            !event_count.is_null(index),
            !capture_id.is_null(index),
            !source_generation.is_null(index),
        ];
        let raw_frame_reference = match present {
            [false, false, false, false, false, false] => None,
            [true, true, true, true, true, true] => Some(RawFrameReferenceV3 {
                raw_frame_capture_instance_id: RawFrameCaptureInstanceIdV2::parse(
                    capture_id.value(index).to_owned(),
                )
                .map_err(|_| MarketDataError::Contract)?,
                raw_frame_source_generation: source_generation.value(index),
                raw_frame_generation: canonical_generation.value(index),
                raw_frame_sequence: source_sequence.value(index),
                raw_frame_event_ordinal: event_ordinal.value(index),
                raw_frame_event_count: event_count.value(index),
            }),
            _ => return Err(MarketDataError::Contract),
        };
        let row = MarketEventParquetRowV3 {
            event,
            raw_frame_reference,
        };
        row.validate().map_err(|_| MarketDataError::Contract)?;
        rows.push(row);
    }
    Ok(rows)
}

pub(super) fn read_capture_rows(
    path: &Path,
    max_object_bytes: u64,
) -> Result<Vec<MarketEventParquetRowV3>> {
    if File::open(path)?.metadata()?.len() > max_object_bytes {
        return Err(MarketDataError::InputLimit);
    }
    let file = File::open(path)?;
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).map_err(|_| MarketDataError::Parquet)?;
    validate_decode_budget(builder.metadata().as_ref())?;
    validate_arrow_schema(EVENT_SCHEMA_V3_ID, builder.schema().as_ref())?;
    super::validate_optional_schema_metadata(
        EVENT_SCHEMA_V3_ID,
        builder.metadata().as_ref(),
        builder.schema(),
    )?;
    let row_count = builder.metadata().file_metadata().num_rows();
    if row_count <= 0 || row_count as u64 > crate::protocol::DEFAULT_MAX_JSONL_RECORDS as u64 {
        return Err(MarketDataError::InputLimit);
    }
    let mut reader = builder
        .with_batch_size(PARQUET_READ_BATCH_ROWS)
        .build()
        .map_err(|_| MarketDataError::Parquet)?;
    let mut rows = Vec::with_capacity(row_count as usize);
    for batch in &mut reader {
        let batch = batch.map_err(|_| MarketDataError::Parquet)?;
        validate_no_unexpected_nulls(&batch)?;
        rows.extend(decode_batch(&batch)?);
    }
    Ok(rows)
}
