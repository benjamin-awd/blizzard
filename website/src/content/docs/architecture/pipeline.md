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
source: Source Files {
  label: "Source Files\n(S3/GCS/Azure)\nNDJSON.gz"
}

Blizzard Pipeline: {
  downloader: Downloader {
    label: "Downloader\n(I/O bound)\nasync tokio"
  }
  processor: Processor {
    label: "Processor\n(CPU bound)\nblocking"
  }
  uploader: Uploader {
    label: "Uploader\n(I/O bound)\nasync tokio"
  }

  downloader -> processor: channel
  processor -> uploader: channel

  downloader_note: "download\nfiles" {
    style.stroke-dash: 3
  }
  processor_note: "decompress\nparse NDJSON\nwrite parquet" {
    style.stroke-dash: 3
  }
  uploader_note: "upload\nparquet" {
    style.stroke-dash: 3
  }
}

table_dir: Table Directory {
  label: "Table Directory\n{partition}/\n*.parquet"
}

penguin: Penguin {
  label: "Penguin\n(Committer)"
}

delta: Delta Lake Table

source -> Blizzard Pipeline.downloader
Blizzard Pipeline.uploader -> table_dir
table_dir -> penguin -> delta
```

## Processing Stages

The pipeline consists of three concurrent stages connected by bounded channels:

| Stage | Thread Pool | Work Type | Description |
|-------|-------------|-----------|-------------|
| **Downloader** | Tokio async | I/O bound | Concurrent file downloads from cloud storage |
| **Processor** | Tokio blocking | CPU bound | Decompress and parse NDJSON to Arrow batches |
| **Uploader** | Tokio async | I/O bound | Concurrent multipart uploads to table directory |

### Stage 1: Downloader

The downloader task manages concurrent file downloads:

- Downloads compressed NDJSON files from source storage
- Respects `max_concurrent_files` concurrency limit
- Sends downloaded bytes through a bounded channel

```d2
direction: down
files: "Source Files: [file1, file2, file3, file4, ...]"
download: Concurrent Download {
  label: "Concurrent Download\n(max_concurrent=16)"
}
channel: "mpsc channel (buffered)"

files -> download -> channel
```

### Stage 2: Processor

The processor runs on Tokio's blocking thread pool for CPU-intensive work:

1. **Decompress**: Gzip or Zstd decompression
2. **Parse**: NDJSON to Arrow RecordBatches
3. **Write**: Batches to in-memory Parquet buffer
4. **Roll**: Complete Parquet files when size threshold reached

```d2
direction: down
input: Downloaded File
decompress: Decompress {
  label: "Decompress\n(gzip/zstd)"
}
parse: Parse NDJSON {
  label: "Parse NDJSON\n(Arrow JSON decoder)"
}
write: Write Parquet {
  label: "Write Parquet\n(buffered, rolling)"
}
output: Finished Parquet Files

input -> decompress -> parse -> write -> output
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
Shutdown Sequence: {
  step1: "1. Shutdown signal received"
  step2: "2. Downloader: Stop accepting new files"
  step3: "3. Processor: Finish in-flight batches"
  step4: "4. Writer: Close and flush final Parquet file"
  step5: "5. Uploader: Upload remaining files to table"
  step6: "6. Exit with stats"

  step1 -> step2 -> step3 -> step4 -> step5 -> step6
}
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