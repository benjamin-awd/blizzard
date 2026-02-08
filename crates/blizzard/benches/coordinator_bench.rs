//! Pipeline coordinator scaling benchmarks.
//!
//! Measures how sink parallelism affects end-to-end throughput by simulating
//! the full pipeline coordination pattern: discovery → download → parse → N sinks.

use std::ops::ControlFlow;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use deltalake::arrow::array::RecordBatch;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use blizzard::config::CompressionFormat;
use blizzard::parquet::{ParquetWriter, ParquetWriterConfig};
use blizzard::source::{FileReader, NdjsonReader, NdjsonReaderConfig};

mod bench_utils;

const DOWNLOAD_LATENCY: Duration = Duration::from_millis(8);
const MAX_CONCURRENT_DOWNLOADS: usize = 4;
const RECORDS_PER_FILE: usize = 50_000;
const BATCH_SIZE: usize = 8192;
const BATCH_CHANNEL_CAPACITY: usize = 4;

fn generate_test_files(num_files: usize) -> Vec<Bytes> {
    (0..num_files)
        .map(|_| {
            let temp_file = bench_utils::generate_ndjson_gz_file(RECORDS_PER_FILE);
            Bytes::from(std::fs::read(temp_file.path()).unwrap())
        })
        .collect()
}

struct ProcessedFile {
    batch_rx: mpsc::Receiver<RecordBatch>,
}

async fn run_pipeline(files: &[Bytes], sink_workers: usize) -> usize {
    let schema = bench_utils::benchmark_schema();
    let num_files = files.len();
    let max_in_flight = sink_workers + 2;

    let (discovery_tx, mut discovery_rx) = mpsc::channel::<usize>(64);
    tokio::spawn(async move {
        for i in 0..num_files {
            if discovery_tx.send(i).await.is_err() {
                break;
            }
        }
    });

    let (download_tx, mut download_rx) = mpsc::channel::<(usize, Bytes)>(8);
    let files_for_download: Vec<Bytes> = files.to_vec();
    tokio::spawn(async move {
        let mut in_flight = FuturesUnordered::new();
        let mut discovery_done = false;

        loop {
            tokio::select! {
                biased;

                Some((idx, data)) = in_flight.next(), if !in_flight.is_empty() => {
                    if download_tx.send((idx, data)).await.is_err() {
                        break;
                    }
                }

                result = discovery_rx.recv(),
                    if !discovery_done && in_flight.len() < MAX_CONCURRENT_DOWNLOADS =>
                {
                    match result {
                        Some(idx) => {
                            let data = files_for_download[idx].clone();
                            in_flight.push(async move {
                                tokio::time::sleep(DOWNLOAD_LATENCY).await;
                                (idx, data)
                            });
                        }
                        None => {
                            discovery_done = true;
                            if in_flight.is_empty() {
                                break;
                            }
                        }
                    }
                }

                else => break,
            }
        }
    });

    let (completion_tx, mut completion_rx) = mpsc::unbounded_channel::<usize>();
    let mut worker_txs: Vec<mpsc::Sender<ProcessedFile>> = Vec::with_capacity(sink_workers);
    let mut worker_handles = Vec::with_capacity(sink_workers);

    for _ in 0..sink_workers {
        let (file_tx, mut file_rx) = mpsc::channel::<ProcessedFile>(1);
        let worker_completion_tx = completion_tx.clone();
        let worker_schema = schema.clone();

        let handle = tokio::spawn(async move {
            let writer_config = ParquetWriterConfig::default();
            let mut writer =
                ParquetWriter::new(worker_schema, writer_config, "bench".into()).unwrap();

            while let Some(processed) = file_rx.recv().await {
                let mut batch_rx = processed.batch_rx;
                let mut file_records = 0usize;

                while let Some(batch) = batch_rx.recv().await {
                    file_records += batch.num_rows();
                    writer.write_batch(&batch).unwrap();
                }

                let _ = worker_completion_tx.send(file_records);
            }

            writer.close().unwrap();
        });

        worker_txs.push(file_tx);
        worker_handles.push(handle);
    }
    drop(completion_tx);

    let mut next_worker: usize = 0;
    let mut files_dispatched: usize = 0;
    let mut files_completed: usize = 0;
    let mut total_records: usize = 0;
    let mut downloads_done = false;

    loop {
        if files_completed == num_files {
            break;
        }

        tokio::select! {
            biased;

            Some(records) = completion_rx.recv() => {
                files_completed += 1;
                total_records += records;
            }

            result = download_rx.recv(),
                if !downloads_done
                   && (files_dispatched - files_completed) < max_in_flight =>
            {
                match result {
                    Some((_idx, data)) => {
                        let reader_schema = schema.clone();
                        let (batch_tx, batch_rx) = mpsc::channel(BATCH_CHANNEL_CAPACITY);

                        // v0.4.0 API: read_batches() with streaming callback
                        tokio::task::spawn_blocking(move || {
                            let config =
                                NdjsonReaderConfig::new(BATCH_SIZE, CompressionFormat::Gzip);
                            let reader =
                                NdjsonReader::new(reader_schema, config, "bench".into());
                            reader
                                .read_batches(data, "bench.ndjson.gz", &mut |batch| {
                                    if batch_tx.blocking_send(batch).is_err() {
                                        return ControlFlow::Break(());
                                    }
                                    ControlFlow::Continue(())
                                })
                                .unwrap();
                        });

                        let processed = ProcessedFile { batch_rx };
                        worker_txs[next_worker].send(processed).await.unwrap();
                        next_worker = (next_worker + 1) % sink_workers;
                        files_dispatched += 1;
                    }
                    None => {
                        downloads_done = true;
                    }
                }
            }
        }
    }

    drop(worker_txs);
    for handle in worker_handles {
        handle.await.unwrap();
    }

    total_records
}

fn coordinator_scaling(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("coordinator_scaling");

    for num_files in [16, 32] {
        let files = generate_test_files(num_files);
        let total_records = (num_files * RECORDS_PER_FILE) as u64;

        group.throughput(Throughput::Elements(total_records));
        group.sample_size(10);

        for sink_workers in [1, 4] {
            group.bench_with_input(
                BenchmarkId::new(format!("sink_workers_{sink_workers}"), num_files),
                &files,
                |b, files| {
                    b.to_async(&rt)
                        .iter(|| async { run_pipeline(files, sink_workers).await });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, coordinator_scaling);
criterion_main!(benches);
