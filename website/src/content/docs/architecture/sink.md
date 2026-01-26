---
title: Sink & Delta Lake
description: How Blizzard writes Parquet files and commits them to Delta Lake tables
---

Blizzard's sink layer handles writing Arrow RecordBatches to Parquet files and committing them atomically to Delta Lake tables with exactly-once semantics.

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
│  │  Delta Sink      │  Batch commits with checkpoint                         │
│  └──────────────────┘                                                        │
│       │                                                                      │
│       ▼                                                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        Delta Lake Table                                │   │
│  │  _delta_log/                                                           │   │
│  │    ├── 00000000000000000000.json                                       │   │
│  │    ├── 00000000000000000001.json                                       │   │
│  │    └── 00000000000000000002.json  ◀── Txn + Add actions                │   │
│  │  *.parquet                                                             │   │
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

## Delta Lake Integration

### Table Creation

Blizzard automatically creates Delta tables if they don't exist:

1. Attempt to open existing table
2. If not found, create with schema from config
3. Register cloud storage handlers (S3, GCS)

### Commit Protocol

Files are committed to Delta Lake in batches:

```
┌─────────────────────────────────────────────────────────────┐
│                    Delta Lake Commit                        │
├─────────────────────────────────────────────────────────────┤
│  Commit N:                                                  │
│    ├── Txn { app_id: "blizzard:<checkpoint>", version: N }  │
│    ├── Add { path: "file1.parquet", size: 128MB, ... }      │
│    ├── Add { path: "file2.parquet", size: 128MB, ... }      │
│    └── Add { path: "file3.parquet", size: 128MB, ... }      │
│                              ▲                              │
│                              │                              │
│                    Atomic commit (all or nothing)           │
└─────────────────────────────────────────────────────────────┘
```

### Batch Commits

To reduce commit overhead, files are committed in batches:

```
COMMIT_BATCH_SIZE = 10 files per commit
```

This balances:
- Checkpoint frequency (more frequent = less re-processing on failure)
- Commit overhead (fewer commits = better throughput)

### Atomic Checkpointing

Checkpoint state is embedded in the Delta commit using `Txn` actions:

1. Serialize checkpoint state to JSON
2. Base64 encode the JSON
3. Store in `Txn.app_id` with `blizzard:` prefix
4. Commit atomically with Add actions

See [Checkpoint & Recovery](/architecture/checkpoint-recovery/) for details.

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
| `blizzard_delta_commit_duration_seconds` | Histogram | Delta commit latency |
| `blizzard_active_uploads` | Gauge | Currently uploading files |
| `blizzard_active_multipart_parts` | Gauge | Currently uploading parts |
| `blizzard_multipart_uploads_total` | Counter | Completed multipart uploads |

## Finished File Structure

When a Parquet file is complete, it's represented as:

```rust
struct FinishedFile {
    filename: String,      // "019234ab-cdef-7890.parquet"
    size: usize,           // 134217728 (bytes)
    record_count: usize,   // 1000000
    bytes: Option<Bytes>,  // Parquet file content
}
```

The `bytes` field:
- Contains file content for upload
- Set to `None` after upload completes
- Cleared in commit records (not stored in checkpoint)

## Code References

| Component | File |
|-----------|------|
| Sink module | `src/sink/mod.rs` |
| Parquet writer | `src/sink/parquet.rs` |
| Delta sink | `src/sink/delta.rs` |
| Rolling policies | `src/sink/parquet.rs:50` |
| Uploader task | `src/pipeline/tasks.rs:97` |
| Commit with checkpoint | `src/sink/delta.rs:68` |
