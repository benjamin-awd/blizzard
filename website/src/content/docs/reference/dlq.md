---
title: Dead Letter Queue
description: Configuring and using Blizzard's Dead Letter Queue for failed records
---

The Dead Letter Queue (DLQ) captures information about files that fail processing, enabling debugging, monitoring, and reprocessing workflows.

## Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Dead Letter Queue Flow                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  File Processing                                                │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────────┐                                            │
│  │ Error occurs    │                                            │
│  └─────────────────┘                                            │
│       │                                                         │
│       ▼                                                         │
│  ┌─────────────────┐      ┌─────────────────────────────────┐   │
│  │ DLQ configured? │─Yes─▶│ Record failure to DLQ           │   │
│  └─────────────────┘      │ (path, error, stage, timestamp) │   │
│       │                   └─────────────────────────────────┘   │
│       │ No                          │                           │
│       ▼                             ▼                           │
│  Continue processing          Buffer in memory                  │
│                                     │                           │
│                                     ▼ (every 100 records)       │
│                              ┌─────────────────┐                │
│                              │ Flush to storage│                │
│                              └─────────────────┘                │
│                                     │                           │
│                                     ▼                           │
│                              s3://bucket/dlq/                   │
│                              failures-20240126-103000.ndjson    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Configuration

Enable the DLQ by specifying a path in the error handling configuration:

```yaml
error_handling:
  dlq_path: "s3://my-bucket/dlq"
  dlq_storage_options:
    AWS_REGION: "us-east-1"
```

### Configuration Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dlq_path` | string | none | Path to write DLQ files (enables DLQ when set) |
| `dlq_storage_options` | map | `{}` | Storage credentials for DLQ location |

The DLQ path can use any supported storage backend:

```yaml
# S3
dlq_path: "s3://bucket/dlq"

# GCS
dlq_path: "gs://bucket/dlq"

# Azure
dlq_path: "abfss://container@account.dfs.core.windows.net/dlq"

# Local filesystem
dlq_path: "/var/log/blizzard/dlq"
```

## DLQ Record Format

Failed files are recorded as NDJSON (newline-delimited JSON):

```json
{"path":"s3://bucket/file1.ndjson.gz","error":"Gzip decompression failed: invalid header","stage":"decompress","timestamp":"2024-01-26T10:30:00Z","retry_count":0}
{"path":"s3://bucket/file2.ndjson.gz","error":"JSON parse error at line 1542: expected ',' or '}'","stage":"parse","timestamp":"2024-01-26T10:30:05Z","retry_count":0}
```

### Record Fields

| Field | Type | Description |
|-------|------|-------------|
| `path` | string | Full path to the failed source file |
| `error` | string | Error message describing the failure |
| `stage` | string | Processing stage where failure occurred |
| `timestamp` | string | ISO 8601 UTC timestamp of the failure |
| `retry_count` | integer | Number of retry attempts (reserved for future use) |

## Failure Stages

Each failure is tagged with the processing stage where it occurred:

| Stage | Description | Common Causes |
|-------|-------------|---------------|
| `download` | Failed to download file from source | Network timeout, permission denied, file deleted |
| `decompress` | Failed to decompress file | Corrupt file, truncated download, wrong compression format |
| `parse` | Failed to parse JSON | Invalid JSON syntax, schema mismatch, encoding issues |
| `upload` | Failed to upload Parquet file | Network error, permission denied, quota exceeded |

## File Naming

DLQ files are named with a timestamp to ensure uniqueness:

```
failures-{YYYYMMDD}-{HHMMSS}.ndjson
```

Example:
```
failures-20240126-103000.ndjson
failures-20240126-114523.ndjson
```

## Buffering and Flushing

The DLQ uses a buffered write strategy to minimize storage operations:

| Behavior | Value | Description |
|----------|-------|-------------|
| Buffer size | 100 records | Records buffered before flush |
| Auto-flush | On threshold | Flushes when buffer reaches 100 records |
| Final flush | On shutdown | All remaining records flushed at pipeline end |

```
Record 1  ──┐
Record 2  ──┤
  ...       ├──▶ Buffer (in memory) ──▶ Flush to storage
Record 99 ──┤                              │
Record 100 ─┘                              ▼
                                    failures-*.ndjson
```

## Failure Statistics

The DLQ tracks failure counts by stage:

```rust
FailureStats {
    download: 2,
    decompress: 5,
    parse: 3,
    upload: 0,
}
```

These statistics are:
- Logged at pipeline completion
- Available via `blizzard_files_failed_total` metric
- Returned in the pipeline result

## Integration with Max Failures

The DLQ works alongside `max_failures` to control pipeline behavior:

```yaml
error_handling:
  max_failures: 100      # Stop after 100 total failures
  dlq_path: "s3://bucket/dlq"
```

When `max_failures` is reached:
1. DLQ is finalized (buffer flushed to storage)
2. Failure statistics are logged
3. Pipeline stops with `MaxFailuresExceeded` error
4. Checkpoint is NOT committed (safe to retry)

## Reprocessing Failed Files

To reprocess files from the DLQ:

### 1. List DLQ Files

```bash
aws s3 ls s3://my-bucket/dlq/
# failures-20240126-103000.ndjson
# failures-20240126-114523.ndjson
```

### 2. Extract Failed Paths

```bash
# Extract unique file paths
aws s3 cp s3://my-bucket/dlq/failures-20240126-103000.ndjson - \
  | jq -r '.path' \
  | sort -u > failed_files.txt
```

### 3. Analyze Failures

```bash
# Group by stage
aws s3 cp s3://my-bucket/dlq/failures-20240126-103000.ndjson - \
  | jq -r '.stage' \
  | sort | uniq -c

# Show errors for a specific stage
aws s3 cp s3://my-bucket/dlq/failures-20240126-103000.ndjson - \
  | jq -r 'select(.stage == "parse") | .error'
```

### 4. Fix and Reprocess

After fixing the underlying issues:

```bash
# Move failed files to a reprocessing location
# Run Blizzard with a new source path pointing to the fixed files
```

## Best Practices

### Always Enable DLQ in Production

```yaml
error_handling:
  dlq_path: "s3://my-bucket/dlq"
```

Without DLQ, failed files are logged but not recorded for later analysis.

### Set Appropriate Max Failures

```yaml
error_handling:
  max_failures: 100  # Prevents runaway failures
  dlq_path: "s3://my-bucket/dlq"
```

Use `max_failures: 0` only for batch jobs where you want to process everything regardless of errors.

### Monitor DLQ Growth

Set up alerts for:
- New DLQ files appearing
- DLQ directory size growing
- High failure rate via metrics

### Separate DLQ by Environment

```yaml
# Production
dlq_path: "s3://my-bucket/dlq/prod"

# Staging
dlq_path: "s3://my-bucket/dlq/staging"
```

### Rotate DLQ Files

Implement a retention policy to clean up old DLQ files:

```bash
# Delete DLQ files older than 30 days
aws s3 ls s3://my-bucket/dlq/ \
  | awk '$1 < "'$(date -d '30 days ago' +%Y-%m-%d)'" {print $4}' \
  | xargs -I {} aws s3 rm s3://my-bucket/dlq/{}
```

## Metrics

DLQ-related metrics:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `blizzard_files_failed_total` | Counter | `stage` | Failed files by stage |
| `blizzard_files_processed_total` | Counter | `status=failed` | Total failed files |

Example queries:

```
# Failures by stage
blizzard_files_failed_total

# Failure rate
rate(blizzard_files_failed_total[5m])

# Parse failures specifically
blizzard_files_failed_total{stage="parse"}
```

## Code References

| Component | File |
|-----------|------|
| DeadLetterQueue struct | `src/dlq.rs` |
| FailedFile record | `src/dlq.rs:12` |
| FailureStats | `src/dlq.rs:21` |
| DLQ integration | `src/pipeline/mod.rs:379` |
| Failure stage enum | `src/metrics/events.rs:72` |
