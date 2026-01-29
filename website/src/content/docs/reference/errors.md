---
title: Error Handling
description: Error types, failure recovery, and Dead Letter Queue in Blizzard
---

Blizzard uses structured error types and provides configurable error handling with Dead Letter Queue (DLQ) support for failed records.

## Error Categories

Errors are organized into categories based on where they occur:

```
┌─────────────────────────────────────────────────────────────────┐
│                      Error Hierarchy                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  PipelineError (top-level)                                       │
│  ├── StorageError                                                │
│  │   ├── InvalidUrl                                              │
│  │   ├── ObjectStore                                             │
│  │   ├── S3Config / GcsConfig / AzureConfig                      │
│  │   └── Io                                                      │
│  ├── ConfigError                                                 │
│  │   ├── EmptySourcePath / EmptySinkPath / EmptySchema           │
│  │   ├── EnvInterpolation                                        │
│  │   ├── YamlParse                                               │
│  │   └── ReadFile                                                │
│  ├── ReaderError                                                 │
│  │   ├── GzipDecompression / ZstdDecompression                   │
│  │   ├── DecoderBuild                                            │
│  │   ├── JsonDecode                                              │
│  │   └── BatchFlush                                              │
│  ├── SchemaError                                                 │
│  │   ├── StructType / SchemaConversion                           │
│  │   └── UrlParse                                                │
│  ├── StagingError                                                │
│  │   └── StagingWrite / Serialize                                │
│  ├── ParquetError                                                │
│  │   └── Write                                                   │
│  ├── DlqError                                                    │
│  │   ├── DlqWrite / DlqSerialize / DlqStorage                    │
│  └── MaxFailuresExceeded                                         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

## Storage Errors

| Error | Description | Recovery |
|-------|-------------|----------|
| `InvalidUrl` | URL format not recognized | Fix configuration |
| `ObjectStore` | Cloud storage operation failed | Check credentials, retry |
| `S3Config` | S3 configuration error | Check AWS credentials |
| `GcsConfig` | GCS configuration error | Check GCP credentials |
| `AzureConfig` | Azure configuration error | Check Azure credentials |
| `Io` | Local filesystem I/O error | Check permissions |

### Not Found Handling

404/NotFound errors are handled specially:
- File not found during download: Skip file, continue processing
- Automatically detected via `error.is_not_found()` check

## Configuration Errors

| Error | Description | Resolution |
|-------|-------------|------------|
| `EmptySourcePath` | Source path is empty | Set `source.path` |
| `EmptySinkPath` | Sink path is empty | Set `sink.path` |
| `EmptySchema` | No schema fields defined | Add fields to `schema.fields` |
| `EnvInterpolation` | Environment variable not found | Set missing env var or use default |
| `YamlParse` | Invalid YAML syntax | Fix YAML formatting |
| `ReadFile` | Cannot read config file | Check file path and permissions |

## Reader Errors

| Error | Description | DLQ Stage |
|-------|-------------|-----------|
| `GzipDecompression` | Gzip decompression failed | `decompress` |
| `ZstdDecompression` | Zstd decompression failed | `decompress` |
| `DecoderBuild` | Failed to create JSON decoder | `parse` |
| `JsonDecode` | JSON parsing failed | `parse` |
| `BatchFlush` | Failed to flush record batch | `parse` |

## Schema Errors

These errors occur during schema conversion for Delta Lake compatibility:

| Error | Description |
|-------|-------------|
| `StructType` | Failed to create Delta schema |
| `SchemaConversion` | Arrow to Delta schema conversion failed |
| `UrlParse` | Table URL parsing failed |

## Failure Stages

Failures are categorized by the processing stage:

| Stage | Description | Common Causes |
|-------|-------------|---------------|
| `download` | File download failed | Network error, permissions, not found |
| `decompress` | Decompression failed | Corrupt file, wrong compression format |
| `parse` | JSON parsing failed | Invalid JSON, schema mismatch |
| `upload` | File upload failed | Network error, permissions |

## Dead Letter Queue (DLQ)

Failed files can be recorded to a Dead Letter Queue for later analysis and reprocessing.

```yaml
error_handling:
  dlq_path: "s3://bucket/dlq"
  dlq_storage_options:
    AWS_REGION: "us-east-1"
```

When enabled, the DLQ captures:
- File path that failed
- Error message
- Failure stage
- Timestamp

See [Dead Letter Queue](/blizzard/reference/dlq/) for detailed configuration, record format, and reprocessing workflows.

## Max Failures

Configure `max_failures` to stop the pipeline after too many errors:

```yaml
error_handling:
  max_failures: 100  # Stop after 100 failures
```

Behavior:
- `0` (default): Unlimited failures, continue processing
- `> 0`: Stop pipeline when failure count reaches threshold

When max failures is reached:
1. DLQ is finalized (flushed to storage)
2. Pipeline returns `MaxFailuresExceeded` error
3. In-progress files may not be written to staging (safe for retry)

## Error Handling Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Error Handling Flow                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Error occurs during processing                                  │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────────┐                                             │
│  │ Is "not found"? │──── Yes ──▶ Skip file, continue             │
│  └─────────────────┘                                             │
│       │ No                                                       │
│       ▼                                                          │
│  ┌─────────────────┐                                             │
│  │ Increment       │                                             │
│  │ failure count   │                                             │
│  └─────────────────┘                                             │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────────┐                                             │
│  │ DLQ configured? │──── Yes ──▶ Record to DLQ                   │
│  └─────────────────┘                                             │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────────┐                                             │
│  │ max_failures    │                                             │
│  │ exceeded?       │──── Yes ──▶ Stop pipeline                   │
│  └─────────────────┘                                             │
│       │ No                                                       │
│       ▼                                                          │
│  Continue processing next file                                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `blizzard_files_processed_total` | Counter | `status` | Files by status |
| `blizzard_files_failed_total` | Counter | `stage` | Failed files by stage |

Status labels: `success`, `skipped`, `failed`
Stage labels: `download`, `decompress`, `parse`, `upload`

## Best Practices

1. **Always configure DLQ** for production to capture failed records
2. **Set max_failures** to prevent runaway failures
3. **Monitor failure metrics** to detect data quality issues
4. **Review DLQ periodically** and fix underlying data issues
5. **Use appropriate compression** to avoid decompression failures

## Code References

| Component | File |
|-----------|------|
| Error types | `src/error.rs` |
| Dead Letter Queue | `src/dlq.rs` |
| Failure stages | `src/metrics/events.rs:72` |
| Error handling in pipeline | `src/pipeline/mod.rs:379` |
| Max failures check | `src/pipeline/mod.rs:311` |
