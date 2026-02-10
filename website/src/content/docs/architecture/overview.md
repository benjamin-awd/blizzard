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
2. **Process**: `spawn_blocking` decompresses (gzip/zstd) and parses NDJSON to Arrow RecordBatches. Read tasks are spawned eagerly — the downloader accepts incoming downloads via biased `select!` while dispatching processed files to workers, so decompression overlaps with sink writes
3. **Sink**: N sink workers (configured by `sink_parallelism`, default 1) each run their own `ParquetWriter` + `UploadTask`. Workers pull files from a bounded channel (capacity 1), providing natural backpressure
4. **Upload**: Each worker's `UploadTask` runs concurrent uploads to the table directory

```d2
direction: down

source: Source Files { shape: document }

download: DownloadTask {
  label: "DownloadTask\n(async, concurrent)"
}

process: spawn_blocking {
  label: "spawn_blocking\n(decompress + parse)"
}

workers: Sink Workers {
  label: "N sink workers\n(sink_parallelism)"

  worker: Worker {
    writer: ParquetWriter
    upload: UploadTask {
      label: "UploadTask\n(async, concurrent)"
    }
    writer -> upload: "rolled Parquet\n(bounded channel)"
  }
}

output: Table Directory { shape: cylinder }

source -> download: "file list"
download -> process: "DownloadedFile\n(bounded channel)"
process -> workers.worker.writer: ProcessedFile
workers.worker.upload -> output: upload
```

## Detailed Processing Flow

The `Downloader` runs a main loop that first tries to dispatch pending files to workers via round-robin `try_send`, then falls through to a single biased `select!` that prioritises: shutdown > worker completions > checkpoint > accepting new downloads.

```d2
direction: down

download_task: DownloadTask {
  label: "DownloadTask\n(concurrent downloads)"
}

downloader: Downloader {
  label: "Downloader::run()\nbiased select! loop"
}

blocking: spawn_blocking {
  label: "spawn_blocking\n(decompress + parse)"
}

workers: Sink Workers {
  label: "N sink workers (sink_parallelism)"

  worker: Worker {
    sink: Sink
    upload: UploadTask
    sink -> upload: "rolled Parquet"
  }
}

output: Table Directory { shape: cylinder }

download_task -> downloader: DownloadedFile
downloader -> blocking: "spawn read task"
blocking -> downloader: ProcessedFile
downloader -> workers.worker.sink: "round-robin dispatch"
workers.worker.sink -> downloader: "completion result"
workers.worker.upload -> output: upload
```

## Backpressure

Bounded channels between stages provide natural backpressure:

| Channel | Buffer Size | Purpose |
|---------|-------------|---------|
| Download -> Process | `max_concurrent_files` | Limits memory for downloaded files |
| Downloader -> Worker | 1 | Workers pull files on demand via round-robin dispatch |
| Process -> Upload | `max_concurrent_uploads` | Limits queued upload files per worker |

When channels fill, upstream stages block until downstream catches up.

## Concurrency Configuration

```yaml
max_concurrent_files: 4       # Parallel downloads/processing (default: 4)
sink_parallelism: 1           # Number of sink workers (default: 1)

sink:
  max_concurrent_uploads: 4   # Parallel file uploads per worker (default: 4)
```

## Pipeline Statistics

The pipeline tracks comprehensive statistics:

| Metric | Description |
|--------|-------------|
| `files_processed` | Number of source files processed |
| `records_processed` | Total records written |
| `bytes_written` | Total Parquet bytes written |
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