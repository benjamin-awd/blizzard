---
title: Pipeline Architecture
description: How Blizzard's processing pipeline connects source and sink components
---

Blizzard's pipeline connects sources and sinks into a streaming data flow with backpressure and graceful shutdown. It uses a producer-consumer pattern to maximize parallelism by separating I/O-bound work from CPU-bound processing.

The pipeline runs continuously, polling for new files at a configurable interval. This enables real-time ingestion where new files are automatically discovered and processed as they arrive.

On each iteration:
1. **Prepare**: Lists source files and identifies pending work
2. **Process**: Runs the full download -> process -> upload pipeline for pending files
3. **Wait**: Sleeps for the configured poll interval before checking for new files


## Architecture Overview

```d2
source: Source Storage {
  shape: cylinder
}

pipeline: Blizzard Pipeline {
  download: Downloader {
    label: "Downloader\n(async, I/O bound)"
  }

  process: Processor {
    label: "Processor\n(blocking, CPU bound)"
  }

  upload: Uploader {
    label: "Uploader\n(async, I/O bound)"
  }

  download -> process: channel
  process -> upload: channel
}

table: Table Directory {
  shape: cylinder
}

penguin: Penguin {
  shape: hexagon
}

delta: Delta Lake {
  shape: cylinder
}

source -> pipeline.download: NDJSON.gz
pipeline -> table: Parquet
table -> penguin -> delta
```

## Processing Stages

The pipeline consists of three concurrent stages connected by bounded channels:

| Stage | Thread Pool | Work Type | Description |
|-------|-------------|-----------|-------------|
| **Downloader** | Tokio async | I/O bound | Concurrent file downloads from cloud storage |
| **Processor** | Tokio blocking | CPU bound | Decompress and parse NDJSON to Arrow batches |
| **Uploader** | Tokio async | I/O bound | Concurrent multipart uploads to table directory |

### Stage 1: Downloader

The `DownloadTask` runs as a background tokio task managing concurrent file downloads:

- Downloads compressed NDJSON files from source storage
- Uses `FuturesUnordered` to manage concurrent downloads
- Respects `max_concurrent_files` concurrency limit (default: 4)
- Sends `DownloadedFile { path, compressed_data }` through a bounded channel

```d2
direction: down

files: Pending Files {
  label: "Pending Files\n[file1, file2, ...]"
  shape: document
}

download_task: DownloadTask {
  futures: FuturesUnordered {
    label: "FuturesUnordered\n<DownloadFuture>"
  }
  concurrent: {
    label: "max_concurrent_files\n(default: 4)"
    style.stroke-dash: 3
  }
}

channel: {
  label: "mpsc::channel\n(bounded: max_concurrent_files)"
  shape: queue
}

files -> download_task.futures: spawn download
download_task.futures -> channel: DownloadedFile
```

### Stage 2: Processor

The processor runs CPU-intensive work via `tokio::task::spawn_blocking`:

1. **Decompress**: Gzip or Zstd decompression
2. **Parse**: NDJSON to Arrow RecordBatches using the Arrow JSON decoder
3. **Return**: `ProcessedFile { path, batches }` to the main loop

The main `Downloader` loop then writes batches to the `Sink`, which handles Parquet writing and rolling.

```d2
direction: down

input: DownloadedFile {
  label: "DownloadedFile\n{ path, compressed_data }"
  shape: document
}

blocking: spawn_blocking {
  decompress: Decompress {
    label: "Decompress\n(gzip/zstd)"
  }
  parse: Parse {
    label: "Parse NDJSON\n(Arrow JSON decoder)"
  }
  decompress -> parse
}

output: ProcessedFile {
  label: "ProcessedFile\n{ path, batches: Vec<RecordBatch> }"
  shape: document
}

input -> blocking.decompress
blocking.parse -> output
```

### Stage 3: Uploader

The uploader handles concurrent file uploads:

- Uploads finished Parquet files to table directory
- Uses parallel multipart uploads for large files
- Penguin discovers these files for Delta Lake commits

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

```d2
direction: down

signal: "SIGINT/SIGTERM received" { shape: oval }
abort: "DownloadTask.abort()\nStop new downloads"
drain: "Drain FuturesUnordered\nFinish in-flight parsing"
close: "BatchWriter.close()\nFlush final Parquet"
finalize: "UploadTask.finalize()\nWait for uploads"
save: "StateTracker.save()\nCheckpoint progress"
exit: "Exit with stats" { shape: oval }

signal -> abort -> drain -> close -> finalize -> save -> exit
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