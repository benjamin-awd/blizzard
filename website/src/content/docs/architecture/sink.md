---
title: Sink & Staging
description: How Blizzard writes Parquet files to staging for Penguin to commit
---

Blizzard's sink layer handles writing Arrow RecordBatches to Parquet files and uploading them to a staging area. Penguin then commits these files to Delta Lake.

## Sink Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Sink Processing                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  RecordBatches                                                               │
│       │                                                                      │
│       ▼                                                                      │
│  ┌──────────────────┐                                                        │
│  │  Parquet Writer  │  Buffer batches, compress, roll files                  │
│  └──────────────────┘                                                        │
│       │                                                                      │
│       ▼ FinishedFile (bytes + metadata)                                     │
│  ┌──────────────────┐                                                        │
│  │  Uploader Task   │  Concurrent multipart uploads                          │
│  └──────────────────┘                                                        │
│       │                                                                      │
│       ▼ Uploaded files                                                       │
│  ┌──────────────────┐                                                        │
│  │  Staging Writer  │  Write parquet + metadata for Penguin                  │
│  └──────────────────┘                                                        │
│       │                                                                      │
│       ▼                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        Staging Directory                              │   │
│  │  table_uri/                                                           │   │
│  │    ├── _staging/pending/                                              │   │
│  │    │     └── {uuid}.meta.json  ◀── Metadata for Penguin               │   │
│  │    └── {partition}/{uuid}.parquet  ◀── Parquet data files             │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Parquet Writer

The Parquet writer buffers RecordBatches and writes them to Parquet files:

### Rolling Policies

Files are rolled (completed) based on configurable policies:

| Policy | Config | Description |
|--------|--------|-------------|
| **Size limit** | `file_size_mb` | Roll when file reaches target size |
| **Inactivity timeout** | `inactivity_timeout_secs` | Roll after period of no writes |
| **Rollover timeout** | `rollover_timeout_secs` | Roll after file has been open too long |

```yaml
sink:
  file_size_mb: 128              # Target file size (default: 128 MB)
  inactivity_timeout_secs: 60    # Roll after 60s of inactivity
  rollover_timeout_secs: 300     # Roll after 5 minutes max
```

### Row Groups

Parquet files are organized into row groups for efficient reading:

```yaml
sink:
  row_group_size_bytes: 134217728  # 128 MB per row group (default)
```

Row groups are flushed when the in-progress size exceeds the threshold, enabling:
- Memory-bounded buffering
- Predicate pushdown on row group statistics
- Parallel row group processing during reads

### Compression

Parquet compression is configured separately from source compression:

```yaml
sink:
  compression: snappy  # snappy (default), gzip, zstd, lz4, uncompressed
```

| Codec | Speed | Ratio | Use Case |
|-------|-------|-------|----------|
| **Snappy** | Fast | Good | Default, balanced |
| **Zstd** | Medium | Best | Storage-optimized |
| **LZ4** | Fastest | Fair | Compute-optimized |
| **Gzip** | Slow | Good | Compatibility |

### File Naming

Files are named with UUIDv7 for:
- Uniqueness across parallel writers
- Temporal ordering (time-based prefix)
- No coordination required

```
019234ab-cdef-7890-1234-567890abcdef.parquet
```

## Staging Protocol

Blizzard writes files to a staging area where Penguin picks them up for Delta Lake commits.

### Directory Structure

```
table_uri/
├── _delta_log/              # Delta transaction log (managed by Penguin)
├── _staging/pending/        # Coordination metadata (.meta.json files)
├── date=2024-01-01/         # Partitioned parquet files
│   └── uuid.parquet
└── ...
```

### Write Protocol

1. Blizzard writes parquet file to `{table_uri}/{partition}/{uuid}.parquet`
2. Blizzard writes metadata to `{table_uri}/_staging/pending/{uuid}.meta.json`
3. Penguin reads `.meta.json`, commits to Delta log, deletes `.meta.json`

The metadata file is written **last**, so Penguin can use its presence as an atomic signal that the Parquet file is complete and ready for commit.

### Metadata Format

The `.meta.json` file contains:

```json
{
  "filename": "date=2024-01-01/019234ab-cdef-7890.parquet",
  "size": 134217728,
  "record_count": 1000000,
  "partition_values": {
    "date": "2024-01-01"
  },
  "source_file": "s3://bucket/input/events.ndjson.gz"
}
```

## Upload Pipeline

### Concurrent Uploads

Files are uploaded concurrently:

```yaml
sink:
  max_concurrent_uploads: 4  # Parallel file uploads (default: 4)
```

### Multipart Uploads

Large files use parallel multipart uploads:

```yaml
sink:
  part_size_mb: 32           # Part size (default: 32 MB)
  min_multipart_size_mb: 5   # Minimum for multipart (default: 5 MB)
  max_concurrent_parts: 8    # Concurrent parts (default: 8)
```

Files smaller than `min_multipart_size_mb` use simple PUT.

## Configuration Example

```yaml
sink:
  path: "s3://my-bucket/delta-table"
  file_size_mb: 128
  row_group_size_bytes: 134217728
  compression: snappy
  max_concurrent_uploads: 4
  part_size_mb: 32
  min_multipart_size_mb: 5
  max_concurrent_parts: 8
  storage_options:
    AWS_REGION: "us-east-1"
```

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `blizzard_bytes_written_total` | Counter | Total Parquet bytes written |
| `blizzard_parquet_write_duration_seconds` | Histogram | Parquet file write latency |
| `blizzard_staging_file_written_total` | Counter | Files written to staging |
| `blizzard_active_uploads` | Gauge | Currently uploading files |
| `blizzard_active_multipart_parts` | Gauge | Currently uploading parts |
| `blizzard_multipart_uploads_total` | Counter | Completed multipart uploads |

## Finished File Structure

When a Parquet file is complete, it's represented as:

```rust
struct FinishedFile {
    filename: String,              // "date=2024-01-01/019234ab.parquet"
    size: usize,                   // 134217728 (bytes)
    record_count: usize,           // 1000000
    bytes: Option<Bytes>,          // Parquet file content
    partition_values: HashMap,     // {"date": "2024-01-01"}
    source_file: Option<String>,   // Original source file path
}
```

The `bytes` field:
- Contains file content for upload
- Set to `None` after upload completes

## Code References

| Component | File |
|-----------|------|
| Sink module | `crates/blizzard/src/sink/mod.rs` |
| Parquet writer | `crates/blizzard/src/sink/parquet.rs` |
| Staging writer | `crates/blizzard/src/staging.rs` |
| Rolling policies | `crates/blizzard/src/sink/parquet.rs:50` |
| Uploader task | `crates/blizzard/src/pipeline/tasks.rs` |
