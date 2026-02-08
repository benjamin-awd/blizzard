//! End-to-end pipeline throughput benchmarks.
//!
//! Measures single-core throughput for the full blizzard data path:
//! gzip NDJSON → parse → Parquet write.
//!
//! Baseline target: ~100k records/sec per vCPU.

use std::ops::ControlFlow;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use blizzard::config::CompressionFormat;
use blizzard::parquet::{ParquetWriter, ParquetWriterConfig};
use blizzard::source::{FileReader, NdjsonReader, NdjsonReaderConfig};
use deltalake::arrow::json::ReaderBuilder;

mod bench_utils;

/// End-to-end benchmark: gzip decompress → NDJSON parse → Parquet write.
///
/// This measures the full single-threaded data path and is the primary
/// benchmark for validating per-vCPU throughput.
fn pipeline_end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_e2e");

    for record_count in [10_000, 100_000, 500_000] {
        let temp_file = bench_utils::generate_ndjson_gz_file(record_count);
        let compressed = Bytes::from(std::fs::read(temp_file.path()).unwrap());
        let schema = bench_utils::benchmark_schema();

        group.throughput(Throughput::Elements(record_count as u64));
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::new("gzip_ndjson_to_parquet", record_count),
            &compressed,
            |b, data| {
                b.iter(|| {
                    let reader_config = NdjsonReaderConfig::new(8192, CompressionFormat::Gzip);
                    let reader = NdjsonReader::new(schema.clone(), reader_config, "bench".into());
                    let writer_config = ParquetWriterConfig::default();
                    let mut writer =
                        ParquetWriter::new(schema.clone(), writer_config, "bench".into()).unwrap();

                    let mut total = 0usize;
                    reader
                        .read_batches(data.clone(), "bench.ndjson.gz", &mut |batch| {
                            total += batch.num_rows();
                            writer.write_batch(&batch).unwrap();
                            ControlFlow::Continue(())
                        })
                        .unwrap();

                    writer.close().unwrap();
                    total
                });
            },
        );
    }

    group.finish();
}

/// JSON parsing throughput (decompress + parse only, no Parquet write).
///
/// Isolates the reader side to identify whether parsing or writing
/// is the bottleneck.
fn json_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");

    for size in [1_000, 10_000, 100_000] {
        let lines = bench_utils::generate_json_lines(size);
        let schema = bench_utils::benchmark_schema();

        group.throughput(Throughput::Elements(size as u64));

        // Bulk decode: concatenate lines and parse in one shot
        let bulk_data: String = lines.join("\n");
        group.bench_with_input(
            BenchmarkId::new("bulk_decode", size),
            &bulk_data,
            |b, data| {
                b.iter(|| {
                    let mut decoder = ReaderBuilder::new(schema.clone())
                        .with_batch_size(8192)
                        .with_strict_mode(false)
                        .build_decoder()
                        .unwrap();
                    decoder.decode(data.as_bytes()).unwrap();
                    decoder.flush().unwrap()
                });
            },
        );
    }

    group.finish();
}

/// Parquet writing throughput (write pre-built RecordBatches, no parsing).
///
/// Isolates the writer side to identify whether parsing or writing
/// is the bottleneck.
fn parquet_writing(c: &mut Criterion) {
    let mut group = c.benchmark_group("parquet_writing");

    for batch_size in [1_000, 8_192, 32_768] {
        let batches = bench_utils::generate_record_batches(batch_size, 10);
        let total_records = batch_size * 10;

        group.throughput(Throughput::Elements(total_records as u64));
        group.bench_with_input(
            BenchmarkId::new("write_batches", batch_size),
            &batches,
            |b, batches| {
                b.iter(|| {
                    let schema = bench_utils::benchmark_schema();
                    let config = ParquetWriterConfig::default();
                    let mut writer = ParquetWriter::new(schema, config, "bench".into()).unwrap();
                    for batch in batches {
                        writer.write_batch(batch).unwrap();
                    }
                    writer.close().unwrap();
                });
            },
        );
    }

    group.finish();
}

/// Gzip decompression + NDJSON parse (no Parquet write).
///
/// Tests the full reader path including decompression to isolate
/// read-side throughput from write-side costs.
fn decompression_and_parse(c: &mut Criterion) {
    let mut group = c.benchmark_group("decompress_and_parse");

    for record_count in [10_000, 100_000] {
        let temp_file = bench_utils::generate_ndjson_gz_file(record_count);
        let compressed = Bytes::from(std::fs::read(temp_file.path()).unwrap());
        let schema = bench_utils::benchmark_schema();

        group.throughput(Throughput::Elements(record_count as u64));
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::new("gzip_ndjson_parse", record_count),
            &compressed,
            |b, data| {
                b.iter(|| {
                    let config = NdjsonReaderConfig::new(8192, CompressionFormat::Gzip);
                    let reader = NdjsonReader::new(schema.clone(), config, "bench".into());

                    let mut total = 0usize;
                    reader
                        .read_batches(data.clone(), "bench.ndjson.gz", &mut |batch| {
                            total += batch.num_rows();
                            ControlFlow::Continue(())
                        })
                        .unwrap();
                    total
                });
            },
        );
    }

    group.finish();
}

/// Coercion mode benchmark: measures the cost of object-to-string coercion.
///
/// Compares standard Arrow reader vs coercion mode on data where Utf8 fields
/// sometimes contain JSON objects/arrays. Standard mode would fail on this
/// data, so this benchmarks the coercion path that makes it work.
fn coercion_mode(c: &mut Criterion) {
    let mut group = c.benchmark_group("coercion_mode");

    for record_count in [10_000, 100_000] {
        let temp_file = bench_utils::generate_ndjson_gz_file_with_objects(record_count);
        let compressed = Bytes::from(std::fs::read(temp_file.path()).unwrap());
        let schema = bench_utils::coercion_schema();

        // Also generate clean data (no objects in Utf8 fields) for baseline comparison
        let clean_temp_file = bench_utils::generate_ndjson_gz_file(record_count);
        let clean_compressed = Bytes::from(std::fs::read(clean_temp_file.path()).unwrap());
        let clean_schema = bench_utils::benchmark_schema();

        group.throughput(Throughput::Elements(record_count as u64));
        group.sample_size(10);

        // Baseline: standard mode on clean data
        group.bench_with_input(
            BenchmarkId::new("standard", record_count),
            &clean_compressed,
            |b, data| {
                b.iter(|| {
                    let config = NdjsonReaderConfig::new(8192, CompressionFormat::Gzip);
                    let reader = NdjsonReader::new(clean_schema.clone(), config, "bench".into());

                    let mut total = 0usize;
                    reader
                        .read_batches(data.clone(), "bench.ndjson.gz", &mut |batch| {
                            total += batch.num_rows();
                            ControlFlow::Continue(())
                        })
                        .unwrap();
                    total
                });
            },
        );

        // Coercion mode on mixed data (objects in Utf8 fields)
        group.bench_with_input(
            BenchmarkId::new("coercing", record_count),
            &compressed,
            |b, data| {
                b.iter(|| {
                    let config = NdjsonReaderConfig::new(8192, CompressionFormat::Gzip)
                        .coerce_objects_to_strings();
                    let reader = NdjsonReader::new(schema.clone(), config, "bench".into());

                    let mut total = 0usize;
                    reader
                        .read_batches(data.clone(), "bench.ndjson.gz", &mut |batch| {
                            total += batch.num_rows();
                            ControlFlow::Continue(())
                        })
                        .unwrap();
                    total
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    pipeline_end_to_end,
    json_parsing,
    parquet_writing,
    decompression_and_parse,
    coercion_mode,
);
criterion_main!(benches);
