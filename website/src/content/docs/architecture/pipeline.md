---
title: Pipeline Architecture
description: How Blizzard's processing pipeline connects source, sink, and staging components
---

Blizzard's pipeline connects sources and sinks into a streaming data flow with backpressure and graceful shutdown. It uses a producer-consumer pattern to maximize parallelism by separating I/O-bound work from CPU-bound processing.

The pipeline runs continuously, polling for new files at a configurable interval. This enables real-time ingestion where new files are automatically discovered and processed as they arrive.

On each iteration:
1. **Prepare**: Lists source files and identifies pending work
2. **Process**: Runs the full download -> process -> upload pipeline for pending files
3. **Wait**: Sleeps for the configured poll interval before checking for new files


## Architecture Overview

```
                      ┌──────────────────┐
                      │   Source Files   │
                      │  (S3/GCS/Azure)  │
                      │    NDJSON.gz     │
                      └────────┬─────────┘
                               │
┌──────────────────────────────┼──────────────────────────────────────────────┐
│  Blizzard Pipeline           ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                                                                     │    │
│  │  ┌──────────────┐        ┌──────────────┐        ┌──────────────┐   │    │
│  │  │  Downloader  │        │  Processor   │        │   Uploader   │   │    │
│  │  │  (I/O bound) │───────>│  (CPU bound) │───────>│  (I/O bound) │   │    │
│  │  │ async tokio  │ channel│   blocking   │ channel│ async tokio  │   │    │
│  │  └──────────────┘        └──────────────┘        └──────────────┘   │    │
│  │         │                       │                       │           │    │
│  │         │ download              │ decompress            │ upload    │    │
│  │         │ files                 │ parse NDJSON          │ parquet   │    │
│  │         │                       │ write parquet         │ + metadata│    │
│  │         │                       │                       │           │    │
│  └─────────┼───────────────────────┼───────────────────────┼───────────┘    │
│            │                       │                       │                │
│            │                       │                       ▼                │
│            │                       │              ┌──────────────────┐      │
│            │                       │              │     Staging      │      │
│            │                       └─────────────>│   Directory      │      │
│            │                         parquet      │                  │      │
│            │                         schema       └────────┬─────────┘      │
│            │                                               │                │
│            │                                               │                │
│            │                                               ▼                │
│            │                                      ┌──────────────────┐      │
│            │                                      │     Penguin      │      │
│            │                                      │   (Committer)    │      │
│            │                                      └────────┬─────────┘      │
│            │                                               │                │
│            │                                               ▼                │
│            │                                      ┌──────────────────┐      │
│            │                                      │   Delta Lake     │      │
│            │                                      │     Table        │      │
│            │                                      └──────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Processing Stages

The pipeline consists of three concurrent stages connected by bounded channels:

| Stage | Thread Pool | Work Type | Description |
|-------|-------------|-----------|-------------|
| **Downloader** | Tokio async | I/O bound | Concurrent file downloads from cloud storage |
| **Processor** | Tokio blocking | CPU bound | Decompress and parse NDJSON to Arrow batches |
| **Uploader** | Tokio async | I/O bound | Concurrent multipart uploads to staging |

### Stage 1: Downloader

The downloader task manages concurrent file downloads:

- Downloads compressed NDJSON files from source storage
- Respects `max_concurrent_files` concurrency limit
- Sends downloaded bytes through a bounded channel

```
Source Files: [file1, file2, file3, file4, ...]
                    │
                    ▼
        ┌────────────────────────┐
        │   Concurrent Download  │
        │   (max_concurrent=16)  │
        └────────────────────────┘
                    │
                    ▼
          mpsc channel (buffered)
```

### Stage 2: Processor

The processor runs on Tokio's blocking thread pool for CPU-intensive work:

1. **Decompress**: Gzip or Zstd decompression
2. **Parse**: NDJSON to Arrow RecordBatches
3. **Write**: Batches to in-memory Parquet buffer
4. **Roll**: Complete Parquet files when size threshold reached

```
Downloaded File
      │
      ▼
┌─────────────────┐
│  Decompress     │  (gzip/zstd)
└─────────────────┘
      │
      ▼
┌─────────────────┐
│  Parse NDJSON   │  (Arrow JSON decoder)
└─────────────────┘
      │
      ▼
┌─────────────────┐
│  Write Parquet  │  (buffered, rolling)
└─────────────────┘
      │
      ▼
Finished Parquet Files
```

### Stage 3: Uploader

The uploader handles concurrent file uploads:

- Uploads finished Parquet files to staging directory
- Uses parallel multipart uploads for large files
- Writes metadata files for Penguin to pick up

## Backpressure

Bounded channels between stages provide natural backpressure:

| Channel | Buffer Size | Purpose |
|---------|-------------|---------|
| Download -> Process | `max_concurrent_files` | Limits memory for downloaded files |
| Process -> Upload | `max_concurrent_uploads x 4` | Allows upload queue to stay ahead |

When channels fill, upstream stages block until downstream catches up.

## Concurrency Configuration

```yaml
source:
  max_concurrent_files: 16    # Parallel downloads/processing

sink:
  max_concurrent_uploads: 4   # Parallel file uploads
  max_concurrent_parts: 8     # Parts per multipart upload
```

## Graceful Shutdown

The pipeline supports graceful shutdown via `CancellationToken`. Shutdown can occur at multiple points in the polling loop:

- **During initialization**: Pipeline exits immediately
- **During processing**: Current iteration completes, then exits
- **During poll wait**: Wakes up and exits

Shutdown sequence within a processing iteration:

1. **Signal received**: SIGINT/SIGTERM/SIGQUIT triggers shutdown
2. **Downloads stop**: No new downloads started
3. **Processing drains**: Finish in-flight files
4. **Final flush**: Close Parquet writer, upload remaining files
5. **Exit**: Report stats and exit

```
┌─────────────────────────────────────────────────────────────────┐
│                      Shutdown Sequence                           │
├─────────────────────────────────────────────────────────────────┤
│  1. Shutdown signal received                                     │
│  2. Downloader: Stop accepting new files                        │
│  3. Processor: Finish in-flight batches                         │
│  4. Writer: Close and flush final Parquet file                  │
│  5. Uploader: Upload remaining files to staging                 │
│  6. Exit with stats                                             │
└─────────────────────────────────────────────────────────────────┘
```

## Pipeline Statistics

The pipeline tracks comprehensive statistics:

| Metric | Description |
|--------|-------------|
| `files_processed` | Number of source files processed |
| `records_processed` | Total records written |
| `bytes_written` | Total Parquet bytes written |
| `parquet_files_written` | Number of Parquet files created |
| `staging_files_written` | Number of files written to staging |

## Error Handling

The pipeline handles errors at each stage:

| Error Type | Behavior |
|------------|----------|
| Download failure | Skip file, record to DLQ, continue |
| Decompression failure | Skip file, record to DLQ, continue |
| Parse failure | Skip file, record to DLQ, continue |
| Upload failure | Retry or record to DLQ |
| Max failures reached | Stop pipeline with error |

See [Error Handling](/blizzard/reference/errors/) for details.