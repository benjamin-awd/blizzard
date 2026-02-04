---
title: Overview
description: How Blizzard's processing pipeline connects source and sink components
---

Blizzard's pipeline connects sources and sinks into a streaming data flow with backpressure and graceful shutdown. It uses a producer-consumer pattern to maximize parallelism by separating I/O-bound work from CPU-bound processing.

The pipeline runs continuously, polling for new files at a configurable interval. This enables real-time ingestion where new files are automatically discovered and processed as they arrive.

On each iteration:
1. **Prepare**: Lists source files and identifies pending work
2. **Process**: Runs the full download -> process -> upload pipeline for pending files
3. **Wait**: Sleeps for the configured poll interval before checking for new files

## Prepare

The prepare phase lists source files and identifies pending work by diffing against tracked state:

```d2
direction: right

source: Source Storage { shape: cylinder }

list: List Files {
  label: "List Files\n(async)"
}

state: State Tracker {
  label: "State Tracker\n(processed files)"
  shape: document
}

diff: Diff {
  label: "Diff\n(new - processed)"
}

pending: Pending Files {
  shape: queue
}

source -> list: "list objects"
list -> diff: "all files"
state -> diff: "processed files"
diff -> pending: "pending files"
```

## Process

The pipeline consists of concurrent stages connected by bounded channels:

| Stage | Thread Pool | Work Type | Description |
|-------|-------------|-----------|-------------|
| **Download** | Tokio async | I/O bound | Concurrent file downloads from cloud storage |
| **Process** | Tokio blocking | CPU bound | Decompress and parse NDJSON to Arrow batches |
| **Upload** | Tokio async | I/O bound | Concurrent multipart uploads to table directory |

1. **Download**: `DownloadTask` manages concurrent downloads via `FuturesUnordered`, sending `DownloadedFile` through a bounded channel
2. **Process**: `spawn_blocking` decompresses (gzip/zstd) and parses NDJSON to Arrow RecordBatches
3. **Sink**: `ParquetWriter` writes batches, queues rolled files to `UploadTask` via bounded channel, drains results with `try_recv()`
4. **Upload**: `UploadTask` runs concurrent multipart uploads to the table directory

```d2
direction: down

source: Source Files { shape: document }

download: DownloadTask {
  label: "DownloadTask\n(async, 4 concurrent)"
}

process: spawn_blocking {
  label: "spawn_blocking\n(decompress + parse)"
}

sink: Sink {
  writer: ParquetWriter
}

upload: UploadTask {
  label: "UploadTask\n(async, 4 concurrent)"
}

output: Table Directory { shape: cylinder }

source -> download: "file list"
download -> process: "DownloadedFile\n(bounded channel)"
process -> sink.writer: RecordBatches
sink.writer -> upload: "rolled Parquet\n(bounded channel)"
upload -> output: multipart upload
upload -> sink: "drain results" {style.stroke-dash: 3}
```

## Detailed Processing Flow

The following diagram shows the internal flow within a single processing iteration, including the biased `tokio::select!` loop priorities and how backpressure propagates through the system.

```d2
direction: down

iteration: Iteration::run() {
  label: "Iteration::run()"
}

download_task: DownloadTask {
  label: "DownloadTask\n(4 concurrent downloads)"
}

upload_task: UploadTask {
  label: "UploadTask\n(4 concurrent uploads)"
}

iteration -> download_task: spawn
iteration -> upload_task: spawn

downloader: Downloader::run() {
  label: "Downloader::run()\nbiased select! loop"

  p1: "1. Shutdown" { style.fill: "#ffcccc" }
  p2: "2. Process completed" { style.fill: "#ffffcc" }
  p3: "3. Checkpoint" { style.fill: "#e0e0e0" }
  p4: "4. Accept downloads" { style.fill: "#ccffcc" }

  p1 -> p2 -> p3 -> p4: priority {style.stroke-dash: 3}
}

download_task -> downloader: DownloadedFile

blocking: spawn_blocking {
  label: "spawn_blocking\n(CPU-bound: decompress + parse)"
  style.fill: "#ffeecc"
}

downloader.p4 -> blocking: spawn read task

processed: ProcessedFile { shape: document }

blocking -> processed
processed -> downloader.p2: complete

sink: Sink {
  label: "Sink::write_file_batches()\nwrite Parquet batches"
}

downloader.p2 -> sink

upload_chan: {
  label: "channel\n(capacity: 4)"
  shape: queue
}

sink -> upload_chan: Parquet file
upload_chan -> upload_task

result_chan: {
  label: "results"
  shape: queue
}

upload_task -> result_chan
result_chan -> sink: drain {style.stroke-dash: 3}
```

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

## Pipeline Statistics

The pipeline tracks comprehensive statistics:

| Metric | Description |
|--------|-------------|
| `files_processed` | Number of source files processed |
| `records_processed` | Total records written |
| `bytes_written` | Total Parquet bytes written |
| `parquet_files_written` | Number of Parquet files created |
| `parquet_files_written` | Number of Parquet files written to table |

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