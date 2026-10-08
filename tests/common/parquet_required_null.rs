//! File-level null-definition fixture for a footer that declares a required field.

use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, RecordBatch, StringArray, TimestampNanosecondArray,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use market_contracts::parquet_schema::trusted_parquet_schema_metadata;
use market_data_platform::schema::{self, MINUTE_BAR_SCHEMA_ID};
use parquet::{
    arrow::{ARROW_SCHEMA_META_KEY, ArrowSchemaConverter, ArrowWriter, encode_arrow_schema},
    file::metadata::{
        FileMetaData, KeyValue, ParquetMetaDataBuilder, ParquetMetaDataReader,
        ParquetMetaDataWriter,
    },
    file::properties::WriterProperties,
};

const NULL_FIELD: &str = "symbol";

/// Write a real Parquet data page containing a null definition level, then rewrite only its
/// footer schema so that the same physical column is declared required. The prefix containing
/// every page byte is asserted unchanged after rewriting.
pub(crate) fn write_required_schema_with_null_definition_level(path: &Path) {
    let nullable_schema = schema_with_null_field(true);
    let symbol_index = nullable_schema.index_of(NULL_FIELD).unwrap();
    let arrays = nullable_schema
        .fields()
        .iter()
        .map(|field| column(field, field.name() == NULL_FIELD))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(nullable_schema), arrays).unwrap();
    assert_eq!(batch.column(symbol_index).null_count(), 1);

    let trusted_metadata = trusted_parquet_schema_metadata(MINUTE_BAR_SCHEMA_ID).unwrap();
    let footer_metadata = trusted_metadata
        .iter()
        .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
        .collect();
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(footer_metadata))
        .build();
    let writer = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(writer, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let original_bytes = std::fs::read(path).unwrap();
    let original_footer_start = footer_start(&original_bytes);
    let original_metadata = read_metadata(path);
    let original_symbol_column = column_index(original_metadata.file_metadata(), NULL_FIELD);
    assert_eq!(
        original_metadata
            .file_metadata()
            .schema_descr()
            .column(original_symbol_column)
            .max_def_level(),
        1,
        "the source Parquet column must encode optional definition levels"
    );
    let original_reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).unwrap(),
    )
    .unwrap();
    assert!(original_reader.schema().field(symbol_index).is_nullable());
    let original_batch = original_reader.build().unwrap().next().unwrap().unwrap();
    assert_eq!(original_batch.column(symbol_index).null_count(), 1);

    let required_schema = schema_with_null_field(false);
    let mut file_metadata = original_metadata.file_metadata().clone();
    let mut key_values = file_metadata
        .key_value_metadata()
        .cloned()
        .unwrap_or_default();
    let arrow_schema = key_values
        .iter_mut()
        .find(|entry| entry.key == ARROW_SCHEMA_META_KEY)
        .expect("ArrowWriter writes an Arrow schema hint");
    arrow_schema.value = Some(encode_arrow_schema(&required_schema));
    let required_parquet_schema = Arc::new(
        ArrowSchemaConverter::new()
            .convert(&required_schema)
            .unwrap(),
    );
    file_metadata = FileMetaData::new(
        file_metadata.version(),
        file_metadata.num_rows(),
        file_metadata.created_by().map(str::to_owned),
        Some(key_values),
        required_parquet_schema,
        file_metadata.column_orders().cloned(),
    );
    let malformed_metadata = ParquetMetaDataBuilder::new(file_metadata)
        .set_row_groups(original_metadata.row_groups().to_vec())
        .build();

    let mut malformed_bytes = original_bytes[..original_footer_start].to_vec();
    ParquetMetaDataWriter::new(&mut malformed_bytes, &malformed_metadata)
        .finish()
        .unwrap();
    std::fs::write(path, &malformed_bytes).unwrap();

    let malformed_footer_start = footer_start(&malformed_bytes);
    assert_eq!(malformed_footer_start, original_footer_start);
    assert_eq!(
        &malformed_bytes[..malformed_footer_start],
        &original_bytes[..original_footer_start],
        "page bytes, including the encoded null, must remain unchanged"
    );
    let rewritten_metadata = read_metadata(path);
    let rewritten_symbol_column = column_index(rewritten_metadata.file_metadata(), NULL_FIELD);
    assert_eq!(
        rewritten_metadata
            .file_metadata()
            .schema_descr()
            .column(rewritten_symbol_column)
            .max_def_level(),
        0,
        "the rewritten Parquet footer must declare the column required"
    );
    let rewritten_reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        File::open(path).unwrap(),
    )
    .unwrap();
    assert!(!rewritten_reader.schema().field(symbol_index).is_nullable());
    schema::validate_arrow_schema(MINUTE_BAR_SCHEMA_ID, rewritten_reader.schema().as_ref())
        .expect("the malformed file must still match the trusted logical schema");
    let trusted_metadata = trusted_parquet_schema_metadata(MINUTE_BAR_SCHEMA_ID).unwrap();
    for (key, value) in &trusted_metadata {
        assert_eq!(rewritten_reader.schema().metadata().get(key), Some(value));
        assert!(
            rewritten_metadata
                .file_metadata()
                .key_value_metadata()
                .unwrap()
                .iter()
                .any(|entry| entry.key == *key && entry.value.as_ref() == Some(value))
        );
    }
}

fn schema_with_null_field(nullable: bool) -> Schema {
    let base = schema::arrow_schema(MINUTE_BAR_SCHEMA_ID).unwrap();
    let fields = base
        .fields()
        .iter()
        .map(|field| {
            if field.name() == NULL_FIELD {
                Arc::new(field.as_ref().clone().with_nullable(nullable))
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    let metadata = trusted_parquet_schema_metadata(MINUTE_BAR_SCHEMA_ID).unwrap();
    Schema::new_with_metadata(fields, metadata)
}

fn column(field: &Field, null_symbol: bool) -> ArrayRef {
    match field.data_type() {
        DataType::Utf8 => {
            if null_symbol {
                Arc::new(StringArray::from(vec![None::<&str>]))
            } else {
                Arc::new(StringArray::from(vec![Some(value_for(field.name()))]))
            }
        }
        DataType::Binary => Arc::new(BinaryArray::from(vec![Some(b"synthetic".as_slice())])),
        DataType::UInt32 => Arc::new(UInt32Array::from(vec![Some(1)])),
        DataType::UInt64 => Arc::new(UInt64Array::from(vec![Some(1)])),
        DataType::Boolean => Arc::new(BooleanArray::from(vec![Some(true)])),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)) => Arc::new(
            TimestampNanosecondArray::from(vec![Some(1_791_382_200_000_000_000_i64)])
                .with_timezone(Arc::clone(timezone)),
        ),
        other => panic!(
            "unhandled registered schema type for {}: {other:?}",
            field.name()
        ),
    }
}

fn value_for(name: &str) -> &'static str {
    match name {
        "source_provider" | "source_feed" => "synthetic",
        "source_entitlement" => "unknown",
        "source_numeric_encoding" => "integer_token",
        "trade_date" => "2026-10-07",
        "session_timezone" => "UTC",
        "session_policy_sha256" => {
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }
        "open" | "high" | "low" | "close" | "volume" => "1",
        "completion_mode" => "synthetic_eof",
        "nbbo_input_status" => "excluded",
        _ => "synthetic",
    }
}

fn read_metadata(path: &Path) -> parquet::file::metadata::ParquetMetaData {
    ParquetMetaDataReader::new()
        .parse_and_finish(&File::open(path).unwrap())
        .unwrap()
}

fn column_index(metadata: &FileMetaData, name: &str) -> usize {
    metadata
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.name() == name)
        .unwrap()
}

fn footer_start(bytes: &[u8]) -> usize {
    assert!(bytes.len() >= 8);
    assert_eq!(&bytes[bytes.len() - 4..], b"PAR1");
    let footer_len =
        u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
    bytes
        .len()
        .checked_sub(8 + usize::try_from(footer_len).unwrap())
        .unwrap()
}
