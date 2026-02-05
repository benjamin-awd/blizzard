//! Benchmark utilities for generating test data.

use deltalake::arrow::array::{BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use rand::Rng;
use std::io::Write;
use std::sync::Arc;

/// Returns a realistic schema with mixed types for benchmarking.
///
/// Matches a typical event-style schema:
/// - id: String (non-nullable)
/// - timestamp: Int64 (non-nullable)
/// - value: Float64 (nullable)
/// - amount: Float64 (nullable)
/// - category: String (nullable)
/// - active: Boolean (nullable)
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
///
/// Each line is a valid JSON object matching the benchmark schema.
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
                r#"{{"id":"record_{}","timestamp":{},"value":{:.2},"amount":{:.4},"category":"{}","active":{}}}"#,
                i, timestamp, value, amount, category, active
            )
        })
        .collect()
}

/// Generate a gzip-compressed NDJSON file for I/O benchmarks.
/// Returns the path to the temporary file.
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
        writeln!(encoder, "{}", line).expect("Failed to write line");
    }

    encoder.finish().expect("Failed to finish compression");
    file
}

/// Generate Arrow RecordBatches for Parquet benchmarks.
///
/// Creates batches with the benchmark schema containing realistic data.
pub fn generate_record_batches(batch_size: usize, num_batches: usize) -> Vec<RecordBatch> {
    let schema = benchmark_schema();
    let mut rng = rand::rng();

    (0..num_batches)
        .map(|batch_idx| {
            let base_idx = batch_idx * batch_size;

            // Generate ids
            let ids: Vec<String> = (0..batch_size)
                .map(|i| format!("record_{}", base_idx + i))
                .collect();

            // Generate timestamps
            let timestamps: Vec<i64> = (0..batch_size)
                .map(|i| 1700000000000 + ((base_idx + i) as i64))
                .collect();

            // Generate values (some nulls)
            let values: Vec<Option<f64>> = (0..batch_size)
                .map(|_| {
                    if rng.random_bool(0.95) {
                        Some(rng.random_range(100.0..10000.0))
                    } else {
                        None
                    }
                })
                .collect();

            // Generate amounts (some nulls)
            let amounts: Vec<Option<f64>> = (0..batch_size)
                .map(|_| {
                    if rng.random_bool(0.95) {
                        Some(rng.random_range(0.01..100.0))
                    } else {
                        None
                    }
                })
                .collect();

            // Generate categories
            let categories: Vec<Option<&str>> = (0..batch_size)
                .map(|_| {
                    if rng.random_bool(0.5) {
                        Some("incoming")
                    } else {
                        Some("outgoing")
                    }
                })
                .collect();

            // Generate active flags
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
