---
title: Source Processing
description: How Blizzard reads and parses compressed NDJSON files from cloud storage
---

Blizzard's source layer handles reading compressed NDJSON files from cloud storage, decompressing them, and parsing the JSON into Arrow RecordBatches for efficient columnar processing.

## Source Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                     Source Processing                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Cloud Storage                                                  │
│  ┌───────────────────────────────────────────┐                  │
│  │  s3://bucket/data/*.ndjson.gz             │                  │
│  │  ├── file1.ndjson.gz                      │                  │
│  │  ├── file2.ndjson.gz                      │                  │
│  │  └── file3.ndjson.gz                      │                  │
│  └───────────────────────────────────────────┘                  │
│                        │                                        │
│                        ▼                                        │
│              ┌──────────────────┐                               │
│              │  Download (I/O)  │                               │
│              └──────────────────┘                               │
│                        │                                        │
│                        ▼ Bytes                                  │
│              ┌──────────────────┐                               │
│              │  Decompress      │  gzip / zstd                  │
│              └──────────────────┘                               │
│                        │                                        │
│                        ▼ NDJSON                                 │
│              ┌──────────────────┐                               │
│              │  Parse JSON      │  Arrow JSON decoder           │
│              └──────────────────┘                               │
│                        │                                        │
│                        ▼                                        │
│              ┌──────────────────┐                               │
│              │  RecordBatches   │  Columnar Arrow format        │
│              └──────────────────┘                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## File Discovery

Blizzard discovers source files by listing the configured path and filtering by extension:

```yaml
source:
  path: "s3://bucket/data/events"  # Lists recursively
```

Behavior:
- Recursively lists all files under the path
- Filters to `.ndjson.gz` files only
- Sorts files alphabetically for deterministic ordering
- Filters out already-processed files (from checkpoint)

## Compression Formats

| Format | Extension | Library | Description |
|--------|-----------|---------|-------------|
| **Gzip** | `.gz` | flate2 | Default, widely compatible |
| **Zstd** | `.zst` | zstd | Better compression ratio and speed |
| **None** | `.ndjson` | - | Uncompressed NDJSON |

Configuration:

```yaml
source:
  compression: gzip  # gzip (default), zstd, or none
```

## NDJSON Format

Blizzard expects newline-delimited JSON (NDJSON) where each line is a valid JSON object:

```json
{"id": "1", "timestamp": "2024-01-01T00:00:00Z", "value": 42}
{"id": "2", "timestamp": "2024-01-01T00:00:01Z", "value": 43}
{"id": "3", "timestamp": "2024-01-01T00:00:02Z", "value": 44}
```

Requirements:
- One JSON object per line
- No trailing commas
- UTF-8 encoded
- Lines can be any length

## Schema Mapping

JSON fields are mapped to Arrow/Parquet types based on the schema configuration:

| JSON Type | Config Type | Arrow Type | Notes |
|-----------|-------------|------------|-------|
| string | `string` | `Utf8` | UTF-8 string |
| number | `int32` | `Int32` | 32-bit signed integer |
| number | `int64` | `Int64` | 64-bit signed integer |
| number | `float32` | `Float32` | 32-bit float |
| number | `float64` | `Float64` | 64-bit float |
| boolean | `boolean` | `Boolean` | true/false |
| string | `timestamp` | `Timestamp(Microsecond, UTC)` | ISO 8601 format |
| string | `date` | `Date32` | Date without time |
| object/array | `json` | `Utf8` | Stored as JSON string |
| string (base64) | `binary` | `Binary` | Base64-decoded bytes |

## Batching

Records are parsed into Arrow RecordBatches for efficient columnar processing:

```yaml
source:
  batch_size: 8192  # Records per batch (default: 8192)
```

The batch size affects:
- Memory usage during parsing
- Granularity of checkpoint recovery
- Write amplification in Parquet

## Checkpoint Recovery

When recovering from a checkpoint, the source layer skips already-processed records:

```
Checkpoint: file2.ndjson.gz → RecordsRead(5000)

Processing:
┌─────────────────────────────────────────┐
│ file2.ndjson.gz (10000 records)         │
├─────────────────────────────────────────┤
│ [0-4999]     │ [5000-9999]              │
│   SKIP       │   PROCESS                │
└─────────────────────────────────────────┘
```

The skip logic is handled in the JSON decoder:
1. Parse records into batches
2. If `skip_records > 0`, slice or skip batches accordingly
3. Only emit records after the skip point

## Concurrent Downloads

Files are downloaded concurrently to maximize I/O throughput:

```yaml
source:
  max_concurrent_files: 4  # Concurrent downloads (default: 4)
```

The downloader task:
1. Maintains a pool of concurrent download futures
2. Sends completed downloads through a bounded channel
3. Respects backpressure from the processing stage

## Error Handling

Source errors are handled gracefully:

| Error Type | Behavior |
|------------|----------|
| File not found (404) | Skip file, log warning, continue |
| Download failure | Record to DLQ, increment failure count |
| Decompression failure | Record to DLQ, increment failure count |
| Parse failure | Record to DLQ, increment failure count |

With `max_failures` configured, the pipeline stops if too many files fail:

```yaml
error_handling:
  max_failures: 10  # Stop after 10 failures (0 = unlimited)
```

## Source State

The `SourceState` tracks file processing progress:

| State | Description |
|-------|-------------|
| `Finished` | File completely processed |
| `RecordsRead(n)` | Partially processed with `n` records read |
| (not present) | Not yet processed |

```json
{
  "files": {
    "file1.ndjson.gz": "Finished",
    "file2.ndjson.gz": {"RecordsRead": 5000},
    "file3.ndjson.gz": "Finished"
  }
}
```

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `blizzard_bytes_read_total` | Counter | Compressed bytes downloaded |
| `blizzard_file_download_duration_seconds` | Histogram | Download latency |
| `blizzard_file_decompression_duration_seconds` | Histogram | Decompression latency |
| `blizzard_active_downloads` | Gauge | Currently downloading files |
| `blizzard_decompression_queue_depth` | Gauge | Files waiting to decompress |
| `blizzard_recovered_records_total` | Counter | Records skipped during recovery |

## Configuration Example

```yaml
source:
  path: "s3://my-bucket/events/2024"
  compression: gzip
  batch_size: 8192
  max_concurrent_files: 4
  storage_options:
    AWS_REGION: "us-east-1"
```

## Code References

| Component | File |
|-----------|------|
| Source module | `src/source/mod.rs` |
| NDJSON reader | `src/source/reader.rs` |
| Source state tracking | `src/source/state.rs` |
| File listing | `src/storage/mod.rs` |
| Download task | `src/pipeline/tasks.rs:300` |
