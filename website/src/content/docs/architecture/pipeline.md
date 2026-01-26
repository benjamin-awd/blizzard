---
title: Pipeline Architecture
description: How Blizzard's processing pipeline connects source, sink, and checkpoint components
---

Blizzard's pipeline connects sources, sinks, and checkpoints into a streaming data flow with backpressure and graceful shutdown. It uses a producer-consumer pattern to maximize parallelism by separating I/O-bound work from CPU-bound processing.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Blizzard Pipeline                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                 │
│  │   Downloader │     │  Processor   │     │   Uploader   │                 │
│  │  (I/O bound) │────▶│ (CPU bound)  │────▶│  (I/O bound) │                 │
│  └──────────────┘     └──────────────┘     └──────────────┘                 │
│         │                    │                    │                          │
│         ▼                    ▼                    ▼                          │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                 │
│  │ Source Files │     │   Parquet    │     │  Delta Lake  │                 │
│  │ (S3/GCS/etc) │     │   Writer     │     │    Table     │                 │
│  └──────────────┘     └──────────────┘     └──────────────┘                 │
│                                                   │                          │
│                              ┌────────────────────┘                          │
│                              ▼                                               │
│                       ┌──────────────┐                                       │
│                       │  Checkpoint  │                                       │
│                       │  Coordinator │                                       │
│                       └──────────────┘                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Processing Stages

The pipeline consists of three concurrent stages connected by bounded channels:

| Stage | Thread Pool | Work Type | Description |
|-------|-------------|-----------|-------------|
| **Downloader** | Tokio async | I/O bound | Concurrent file downloads from cloud storage |
| **Processor** | Tokio blocking | CPU bound | Decompress and parse NDJSON to Arrow batches |
| **Uploader** | Tokio async | I/O bound | Concurrent multipart uploads to cloud storage |

### Stage 1: Downloader

The downloader task manages concurrent file downloads:

- Downloads compressed NDJSON files from source storage
- Respects `max_concurrent_files` concurrency limit
- Handles checkpoint recovery by tracking records to skip
- Sends downloaded bytes through a bounded channel

```
Source Files: [file1, file2, file3, file4, ...]
                    │
                    ▼
        ┌───────────────────────┐
        │   Concurrent Download  │
        │   (max_concurrent=16)  │
        └───────────────────────┘
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

The uploader handles concurrent file uploads with parallel multipart:

- Uploads finished Parquet files to sink storage
- Uses parallel multipart uploads for large files
- Commits files to Delta Lake in batches
- Stores checkpoint state atomically with commits

## Backpressure

Bounded channels between stages provide natural backpressure:

| Channel | Buffer Size | Purpose |
|---------|-------------|---------|
| Download → Process | `max_concurrent_files` | Limits memory for downloaded files |
| Process → Upload | `max_concurrent_uploads × 4` | Allows upload queue to stay ahead |

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

The pipeline supports graceful shutdown via `CancellationToken`:

1. **Signal received**: SIGINT/SIGTERM triggers shutdown
2. **Downloads stop**: No new downloads started
3. **Processing drains**: Finish in-flight files
4. **Final flush**: Close Parquet writer, upload remaining files
5. **Checkpoint commit**: Final checkpoint with Delta commit

```
┌─────────────────────────────────────────────────────────────────┐
│                      Shutdown Sequence                           │
├─────────────────────────────────────────────────────────────────┤
│  1. Shutdown signal received                                     │
│  2. Downloader: Stop accepting new files                        │
│  3. Processor: Finish in-flight batches                         │
│  4. Writer: Close and flush final Parquet file                  │
│  5. Uploader: Upload remaining files                            │
│  6. Delta: Commit with final checkpoint                         │
│  7. Exit with stats                                             │
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
| `delta_commits` | Number of Delta Lake commits |
| `checkpoints_saved` | Number of checkpoint saves |

## Error Handling

The pipeline handles errors at each stage:

| Error Type | Behavior |
|------------|----------|
| Download failure | Skip file, record to DLQ, continue |
| Decompression failure | Skip file, record to DLQ, continue |
| Parse failure | Skip file, record to DLQ, continue |
| Upload failure | Retry or record to DLQ |
| Max failures reached | Stop pipeline with error |

See [Error Handling](/reference/errors/) for details.

## Code References

| Component | File |
|-----------|------|
| Pipeline struct | `src/pipeline/mod.rs` |
| Downloader task | `src/pipeline/tasks.rs:300` |
| Uploader task | `src/pipeline/tasks.rs:97` |
| Processing loop | `src/pipeline/mod.rs:254` |
| Graceful shutdown | `src/pipeline/mod.rs:261` |
