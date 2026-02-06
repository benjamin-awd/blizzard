//! Benchmark utilities for generating test data.

#![allow(clippy::cast_possible_wrap)]

use deltalake::arrow::array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use rand::Rng;
use std::io::Write;
use std::sync::Arc;

/// Returns a realistic schema with mixed types for benchmarking.
pub fn benchmark_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("category", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, true),
    ]))
}

/// Generate NDJSON lines for parsing benchmarks.
pub fn generate_json_lines(count: usize) -> Vec<String> {
    let mut rng = rand::rng();
    let categories = ["incoming", "outgoing"];

    (0..count)
        .map(|i| {
            let category = categories[rng.random_range(0..2)];
            let value: f64 = rng.random_range(100.0..10000.0);
            let amount: f64 = rng.random_range(0.01..100.0);
            let timestamp: i64 = 1700000000000 + (i as i64);
            let active: bool = rng.random_bool(0.9);

            format!(
                r#"{{"id":"record_{i}","timestamp":{timestamp},"value":{value:.2},"amount":{amount:.4},"category":"{category}","active":{active}}}"#,
            )
        })
        .collect()
}

/// Generate a gzip-compressed NDJSON file. Returns the temporary file handle.
pub fn generate_ndjson_gz_file(count: usize) -> tempfile::NamedTempFile {
    use flate2::Compression;
    use flate2::write::GzEncoder;

    let lines = generate_json_lines(count);
    let file = tempfile::NamedTempFile::new().expect("Failed to create temp file");

    let mut encoder = GzEncoder::new(
        std::fs::File::create(file.path()).expect("Failed to create file"),
        Compression::fast(),
    );

    for line in &lines {
        writeln!(encoder, "{line}").expect("Failed to write line");
    }

    encoder.finish().expect("Failed to finish compression");
    file
}

/// Generate Arrow RecordBatches for Parquet writing benchmarks.
pub fn generate_record_batches(batch_size: usize, num_batches: usize) -> Vec<RecordBatch> {
    let schema = benchmark_schema();
    let mut rng = rand::rng();

    (0..num_batches)
        .map(|batch_idx| {
            let base_idx = batch_idx * batch_size;

            let ids: Vec<String> = (0..batch_size)
                .map(|i| format!("record_{}", base_idx + i))
                .collect();

            let timestamps: Vec<i64> = (0..batch_size)
                .map(|i| 1700000000000 + ((base_idx + i) as i64))
                .collect();

            let values: Vec<Option<f64>> = (0..batch_size)
                .map(|_| {
                    if rng.random_bool(0.95) {
                        Some(rng.random_range(100.0..10000.0))
                    } else {
                        None
                    }
                })
                .collect();

            let amounts: Vec<Option<f64>> = (0..batch_size)
                .map(|_| {
                    if rng.random_bool(0.95) {
                        Some(rng.random_range(0.01..100.0))
                    } else {
                        None
                    }
                })
                .collect();

            let categories: Vec<Option<&str>> = (0..batch_size)
                .map(|_| {
                    if rng.random_bool(0.5) {
                        Some("incoming")
                    } else {
                        Some("outgoing")
                    }
                })
                .collect();

            let active: Vec<Option<bool>> = (0..batch_size)
                .map(|_| {
                    if rng.random_bool(0.98) {
                        Some(rng.random_bool(0.9))
                    } else {
                        None
                    }
                })
                .collect();

            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(ids)),
                    Arc::new(Int64Array::from(timestamps)),
                    Arc::new(Float64Array::from(values)),
                    Arc::new(Float64Array::from(amounts)),
                    Arc::new(StringArray::from(categories)),
                    Arc::new(BooleanArray::from(active)),
                ],
            )
            .expect("Failed to create RecordBatch")
        })
        .collect()
}
