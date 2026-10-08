//! File-level null-definition fixture for a footer that declares a required field.

use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::Schema;
use market_contracts::parquet_schema::trusted_parquet_schema_metadata;
use market_data_platform::schema;
use parquet::{
    arrow::{
        ARROW_SCHEMA_META_KEY, ArrowSchemaConverter, ArrowWriter,
        arrow_reader::ParquetRecordBatchReaderBuilder, encode_arrow_schema,
    },
    file::metadata::{
        FileMetaData, KeyValue, ParquetMetaDataBuilder, ParquetMetaDataReader,
        ParquetMetaDataWriter,
    },
    file::properties::WriterProperties,
};

const NULL_FIELD: &str = "symbol";
const READ_BATCH_ROWS: usize = 8192;

/// Copy a production-written valid bar dataset, changing only its first symbol to null. Then
/// rewrite only the footer schema so that the same data page declares `symbol` as required.
pub(crate) fn write_required_schema_with_null_definition_level(source: &Path, path: &Path) {
    write_valid_rows_with_nullable_null_symbol(source, path);
    assert_identical_except_first_symbol(source, path);

    let original_bytes = std::fs::read(path).unwrap();
    let original_footer_start = footer_start(&original_bytes);
    let original_metadata = read_metadata(path);
    let symbol_index = schema::arrow_schema(schema::MINUTE_BAR_SCHEMA_ID)
        .unwrap()
        .index_of(NULL_FIELD)
        .unwrap();
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
    let original_reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    assert!(original_reader.schema().field(symbol_index).is_nullable());
    let required_schema = schema_with_null_field(original_reader.schema().as_ref(), false);
    let original_batch = original_reader.build().unwrap().next().unwrap().unwrap();
    assert_eq!(original_batch.num_rows(), 390);
    assert_eq!(original_batch.column(symbol_index).null_count(), 1);

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
    let rewritten_reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    assert!(!rewritten_reader.schema().field(symbol_index).is_nullable());
    schema::validate_arrow_schema(
        schema::MINUTE_BAR_SCHEMA_ID,
        rewritten_reader.schema().as_ref(),
    )
    .expect("the malformed file must still match the trusted logical schema");
    let trusted_metadata = trusted_parquet_schema_metadata(schema::MINUTE_BAR_SCHEMA_ID).unwrap();
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
    let mut decoded_rows = rewritten_reader.build().unwrap();
    assert!(
        decoded_rows.next().unwrap().is_err(),
        "the Parquet decoder must reject the null definition level under a required footer schema"
    );
}

fn write_valid_rows_with_nullable_null_symbol(source: &Path, path: &Path) {
    let source_reader = ParquetRecordBatchReaderBuilder::try_new(File::open(source).unwrap())
        .unwrap()
        .with_batch_size(READ_BATCH_ROWS)
        .build()
        .unwrap();
    let source_schema = source_reader.schema();
    schema::validate_arrow_schema(schema::MINUTE_BAR_SCHEMA_ID, source_schema.as_ref()).unwrap();
    let nullable_schema = Arc::new(schema_with_null_field(source_schema.as_ref(), true));
    let symbol_index = nullable_schema.index_of(NULL_FIELD).unwrap();
    let trusted_metadata = trusted_parquet_schema_metadata(schema::MINUTE_BAR_SCHEMA_ID).unwrap();
    let footer_metadata = trusted_metadata
        .iter()
        .map(|(key, value)| KeyValue::new(key.clone(), value.clone()))
        .collect();
    let properties = WriterProperties::builder()
        .set_key_value_metadata(Some(footer_metadata))
        .build();
    let writer = File::create(path).unwrap();
    let mut writer =
        ArrowWriter::try_new(writer, Arc::clone(&nullable_schema), Some(properties)).unwrap();
    let mut rows_seen = 0_usize;
    for source_batch in source_reader {
        let source_batch = source_batch.unwrap();
        let source_symbols = source_batch
            .column(symbol_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(source_symbols.null_count(), 0);
        let mut columns: Vec<ArrayRef> = source_batch.columns().to_vec();
        let symbols = (0..source_symbols.len())
            .map(|index| {
                if rows_seen + index == 0 {
                    None
                } else {
                    Some(source_symbols.value(index))
                }
            })
            .collect::<Vec<_>>();
        columns[symbol_index] = Arc::new(StringArray::from(symbols));
        let batch = RecordBatch::try_new(Arc::clone(&nullable_schema), columns).unwrap();
        rows_seen = rows_seen.checked_add(batch.num_rows()).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    assert_eq!(
        rows_seen, 390,
        "fixture must derive from a full valid session"
    );
}

fn assert_identical_except_first_symbol(source: &Path, candidate: &Path) {
    let source_reader = ParquetRecordBatchReaderBuilder::try_new(File::open(source).unwrap())
        .unwrap()
        .with_batch_size(READ_BATCH_ROWS)
        .build()
        .unwrap();
    let candidate_reader = ParquetRecordBatchReaderBuilder::try_new(File::open(candidate).unwrap())
        .unwrap()
        .with_batch_size(READ_BATCH_ROWS)
        .build()
        .unwrap();
    let symbol_index = source_reader.schema().index_of(NULL_FIELD).unwrap();
    let mut source_batches = source_reader;
    let mut candidate_batches = candidate_reader;
    let mut row_offset = 0;
    loop {
        match (source_batches.next(), candidate_batches.next()) {
            (Some(Ok(source)), Some(Ok(candidate))) => {
                assert_eq!(source.num_rows(), candidate.num_rows());
                for column in 0..source.num_columns() {
                    if column == symbol_index {
                        let source_values = source
                            .column(column)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        let candidate_values = candidate
                            .column(column)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap();
                        for index in 0..source.num_rows() {
                            if row_offset + index == 0 {
                                assert!(candidate_values.is_null(index));
                            } else {
                                assert_eq!(
                                    candidate_values.value(index),
                                    source_values.value(index)
                                );
                            }
                        }
                    } else {
                        assert_eq!(
                            source.column(column).to_data(),
                            candidate.column(column).to_data(),
                            "only the symbol column may differ"
                        );
                    }
                }
                row_offset += source.num_rows();
            }
            (None, None) => break,
            _ => panic!("source and malformed fixture have different row groups"),
        }
    }
    assert_eq!(row_offset, 390);
}

fn schema_with_null_field(schema: &Schema, nullable: bool) -> Schema {
    let fields = schema
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
    Schema::new_with_metadata(fields, schema.metadata().clone())
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
