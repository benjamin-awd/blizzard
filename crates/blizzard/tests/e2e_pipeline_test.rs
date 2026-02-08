//! End-to-end pipeline tests: NDJSON → parse → Parquet.
//!
//! These tests exercise the real data path through the reader and writer,
//! verifying that NDJSON input produces correct Parquet output.

use std::io::Write;
use std::ops::ControlFlow;
use std::sync::Arc;

use bytes::Bytes;
use deltalake::arrow::array::{Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use deltalake::parquet::file::reader::{FileReader as ParquetFileReader, SerializedFileReader};
use tempfile::TempDir;

use blizzard::config::CompressionFormat;
use blizzard::parquet::{ParquetWriter, ParquetWriterConfig};
use blizzard::source::{FileReader, NdjsonReader, NdjsonReaderConfig};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
        Field::new("category", DataType::Utf8, true),
    ]))
}

fn generate_ndjson(num_rows: usize) -> String {
    (0..num_rows)
        .map(|i| {
            format!(
                r#"{{"id":"row-{}","value":{},"category":"cat-{}"}}"#,
                i,
                i * 10,
                i % 3
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Collect all batches from the reader into a Vec.
fn collect_batches(
    reader: &NdjsonReader,
    data: Bytes,
    path: &str,
) -> Result<Vec<RecordBatch>, blizzard::error::ReaderError> {
    let mut batches = Vec::new();
    reader.read_batches(data, path, &mut |batch| {
        batches.push(batch);
        ControlFlow::Continue(())
    })?;
    Ok(batches)
}

#[test]
fn test_ndjson_to_parquet_pipeline() {
    let _temp_dir = TempDir::new().unwrap();
    let schema = test_schema();
    let num_rows = 100;

    // Generate NDJSON content
    let ndjson = generate_ndjson(num_rows);
    let data = Bytes::from(ndjson);

    // Read with NdjsonReader (uncompressed)
    let reader_config = NdjsonReaderConfig::new(1000, CompressionFormat::None);
    let reader = NdjsonReader::new(schema.clone(), reader_config, "e2e-test".to_string());

    let batches = collect_batches(&reader, data, "test.ndjson").unwrap();
    assert!(!batches.is_empty(), "should produce at least one batch");

    let total_read: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_read, num_rows, "should read all rows");

    // Write batches with ParquetWriter
    let writer_config = ParquetWriterConfig::default().with_file_size_mb(10);
    let mut writer = ParquetWriter::new(schema, writer_config, "e2e-test".to_string()).unwrap();

    for batch in &batches {
        writer.write_batch(batch).unwrap();
    }

    let finished_files = writer.close().unwrap();

    // Verify FinishedFile metadata
    assert!(
        !finished_files.is_empty(),
        "should produce at least one file"
    );

    let total_records: usize = finished_files.iter().map(|f| f.record_count).sum();
    assert_eq!(
        total_records, num_rows,
        "total record count should match input"
    );

    // Verify each parquet file is readable and correct
    for file in &finished_files {
        assert!(file.size > 0, "file should have non-zero size");
        assert!(file.bytes.is_some(), "file should have parquet bytes");

        let bytes = file.bytes.as_ref().unwrap();

        // Verify with SerializedFileReader (metadata check)
        let serialized_reader = SerializedFileReader::new(bytes.clone()).unwrap();
        let metadata = serialized_reader.metadata();
        let parquet_row_count: usize = metadata
            .row_groups()
            .iter()
            .map(|rg| usize::try_from(rg.num_rows()).expect("row count should fit in usize"))
            .sum();
        assert_eq!(
            parquet_row_count, file.record_count,
            "parquet metadata row count should match"
        );

        // Verify schema has 3 fields
        let parquet_schema = metadata.file_metadata().schema_descr();
        assert_eq!(
            parquet_schema.num_columns(),
            3,
            "parquet should have 3 columns"
        );

        // Verify with ArrowReader (data check)
        let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .unwrap()
            .build()
            .unwrap();

        let mut arrow_rows = 0;
        for batch_result in arrow_reader {
            let batch = batch_result.unwrap();
            arrow_rows += batch.num_rows();

            // Verify schema
            assert_eq!(batch.schema().field(0).name(), "id");
            assert_eq!(batch.schema().field(1).name(), "value");
            assert_eq!(batch.schema().field(2).name(), "category");
        }
        assert_eq!(
            arrow_rows, file.record_count,
            "arrow reader row count should match"
        );
    }

    // Spot-check column values from the first file
    let first_bytes = finished_files[0].bytes.as_ref().unwrap();
    let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(first_bytes.clone())
        .unwrap()
        .build()
        .unwrap();

    let first_batch = arrow_reader.into_iter().next().unwrap().unwrap();

    let id_col = first_batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(id_col.value(0), "row-0");

    let value_col = first_batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(value_col.value(0), 0);
    assert_eq!(value_col.value(1), 10);

    let category_col = first_batch
        .column_by_name("category")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(category_col.value(0), "cat-0");
    assert_eq!(category_col.value(1), "cat-1");
    assert_eq!(category_col.value(2), "cat-2");
}

#[test]
fn test_ndjson_gz_to_parquet_pipeline() {
    let _temp_dir = TempDir::new().unwrap();
    let schema = test_schema();
    let num_rows = 50;

    // Generate gzip-compressed NDJSON
    let ndjson = generate_ndjson(num_rows);
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(ndjson.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();
    let data = Bytes::from(compressed);

    // Read with NdjsonReader (gzip)
    let reader_config = NdjsonReaderConfig::new(1000, CompressionFormat::Gzip);
    let reader = NdjsonReader::new(schema.clone(), reader_config, "e2e-gz-test".to_string());

    let batches = collect_batches(&reader, data, "test.ndjson.gz").unwrap();
    assert!(!batches.is_empty(), "should produce at least one batch");

    let total_read: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_read, num_rows, "should read all rows from gzip");

    // Write to parquet
    let writer_config = ParquetWriterConfig::default().with_file_size_mb(10);
    let mut writer = ParquetWriter::new(schema, writer_config, "e2e-gz-test".to_string()).unwrap();

    for batch in &batches {
        writer.write_batch(batch).unwrap();
    }

    let finished_files = writer.close().unwrap();

    // Verify
    assert!(!finished_files.is_empty());
    let total_records: usize = finished_files.iter().map(|f| f.record_count).sum();
    assert_eq!(total_records, num_rows);

    // Verify parquet is readable
    let bytes = finished_files[0].bytes.as_ref().unwrap();
    let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .unwrap()
        .build()
        .unwrap();

    let mut arrow_rows = 0;
    for batch_result in arrow_reader {
        let batch = batch_result.unwrap();
        arrow_rows += batch.num_rows();
    }
    assert_eq!(arrow_rows, finished_files[0].record_count);
}
