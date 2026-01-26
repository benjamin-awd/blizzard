# Blizzard

A high-performance Rust tool for streaming NDJSON.gz files to Delta Lake with checkpoint-based recovery for exactly-once processing semantics.

## Features

- **Multi-cloud storage**: S3, GCS, Azure Blob Storage, and local filesystem
- **Exactly-once processing**: Checkpoint-based recovery ensures no duplicates or data loss
- **High throughput**: Concurrent file downloads, parallel decompression, and multipart uploads
- **Delta Lake integration**: Transactional writes with ACID guarantees
- **Resilient error handling**: Skip failed files and continue processing, with Dead Letter Queue support
- **Prometheus metrics**: Built-in metrics endpoint with health checks

## Requirements

- Rust 1.85+ (Edition 2024)
- Cloud credentials configured for your storage backend (AWS, GCP, or Azure)

## Installation

```bash
# Clone and build
git clone <repository-url>
cd blizzard
cargo build --release

# Binary will be at target/release/blizzard
```

## Quick Start

1. Create a configuration file (e.g., `config.yaml`):

```yaml
source:
  path: "s3://my-bucket/input/*.ndjson.gz"
  compression: gzip
  max_concurrent_files: 16

sink:
  path: "s3://my-bucket/output/my-table"
  file_size_mb: 128
  compression: snappy

schema:
  fields:
    - name: id
      type: string
      nullable: false
    - name: timestamp
      type: timestamp
      nullable: false
    - name: value
      type: float64
      nullable: true
```

2. Run blizzard:

```bash
# Run the pipeline
blizzard --config config.yaml
```

## Configuration Reference

### Source

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | required | Storage path pattern (supports S3, GCS, Azure, local) |
| `compression` | string | `gzip` | Input compression: `none`, `gzip`, `zstd` |
| `batch_size` | int | `8192` | Records per batch for Arrow processing |
| `max_concurrent_files` | int | `16` | Maximum concurrent file downloads |
| `storage_options` | map | `{}` | Cloud-specific credentials/settings |

### Sink

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | required | Delta Lake table path |
| `file_size_mb` | int | `128` | Target Parquet file size |
| `row_group_size_bytes` | int | `134217728` | Target row group size (128MB) |
| `compression` | string | `snappy` | Parquet compression: `uncompressed`, `snappy`, `gzip`, `zstd`, `lz4` |
| `inactivity_timeout_secs` | int | none | Roll file after N seconds of inactivity |
| `rollover_timeout_secs` | int | none | Roll file after N seconds open |
| `max_concurrent_uploads` | int | `4` | Maximum concurrent file uploads |
| `max_concurrent_parts` | int | `8` | Maximum concurrent multipart parts |
| `part_size_mb` | int | `32` | Multipart upload part size |
| `min_multipart_size_mb` | int | `5` | Minimum size before using multipart |
| `storage_options` | map | `{}` | Cloud-specific credentials/settings |

### Schema

Define the structure of your NDJSON data. Supported types:

| Type | Arrow Type | Description |
|------|------------|-------------|
| `string` | Utf8 | UTF-8 string |
| `int32` | Int32 | 32-bit signed integer |
| `int64` | Int64 | 64-bit signed integer |
| `float32` | Float32 | 32-bit float |
| `float64` | Float64 | 64-bit float |
| `boolean` | Boolean | true/false |
| `timestamp` | Timestamp(us, UTC) | Microsecond timestamp |
| `date` | Date32 | Date without time |
| `json` | Utf8 | Raw JSON stored as string |
| `binary` | Binary | Base64-encoded binary |

### Error Handling

Configure resilient error handling and Dead Letter Queue (DLQ) for failed files:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_failures` | int | `0` | Maximum failures before stopping (0 = unlimited) |
| `dlq_path` | string | none | Path to write failed file records (enables DLQ) |
| `dlq_storage_options` | map | `{}` | Cloud-specific credentials for DLQ storage |

`max_failures` acts as a circuit breaker for systemic issues (schema mismatch, expired credentials, corrupted source). Without it, the pipeline could "succeed" while silently skipping all files. The default (`0`) disables this check.

Example:

```yaml
error_handling:
  max_failures: 100        # Stop after 100 failures
  dlq_path: "s3://my-bucket/dlq/pipeline-name/"
```

**DLQ Output Format**: Failed files are written as NDJSON to `{dlq_path}/failures-{timestamp}.ndjson`:

```json
{"path":"s3://bucket/file.ndjson.gz","error":"invalid JSON at line 42","stage":"parse","timestamp":"2025-01-26T10:30:00Z","retry_count":0}
```

Failure stages: `download`, `decompress`, `parse`, `upload`

### Metrics

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable Prometheus metrics |
| `address` | string | `0.0.0.0:9090` | Metrics HTTP server address |

## Environment Variables

Configuration supports environment variable interpolation:

```yaml
source:
  path: "${INPUT_PATH}"
  storage_options:
    aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
    aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
```

Supported syntax:
- `$VAR` or `${VAR}` - Required variable (fails if unset)
- `${VAR:-default}` - Variable with default value

## CLI Options

```
blizzard [OPTIONS] --config <CONFIG>

Options:
  -c, --config <CONFIG>    Path to the configuration file
      --log-level <LEVEL>  Log level: trace, debug, info, warn, error [default: info]
      --dry-run            Validate configuration without processing
  -h, --help               Print help
  -V, --version            Print version
```

## Metrics & Monitoring

When metrics are enabled, blizzard exposes:

- **`GET /metrics`** - Prometheus metrics endpoint
- **`GET /health`** - Health check endpoint (returns 200 OK)

Key metrics include:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `blizzard_files_processed_total` | counter | `status` | Files processed (success/skipped/failed) |
| `blizzard_files_failed_total` | counter | `stage` | Failed files by stage (download/decompress/parse/upload) |
| `blizzard_records_processed_total` | counter | - | Total records processed |
| `blizzard_bytes_written_total` | counter | - | Total bytes written to Parquet |

## Storage Backends

### Amazon S3

```yaml
source:
  path: "s3://bucket/path/*.ndjson.gz"
  storage_options:
    aws_region: "us-east-1"
    # Credentials from environment or IAM role
```

### Google Cloud Storage

```yaml
source:
  path: "gs://bucket/path/*.ndjson.gz"
  storage_options:
    # Uses GOOGLE_APPLICATION_CREDENTIALS or instance metadata
```

### Local Filesystem

```yaml
source:
  path: "/data/input/*.ndjson.gz"
```

## Architecture

Blizzard uses a three-stage pipeline with backpressure:

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────┐
│  Download   │────▶│  Decompress &   │────▶│   Upload    │
│  (I/O)      │     │  Parse (CPU)    │     │  (I/O)      │
└─────────────┘     └─────────────────┘     └─────────────┘
      │                     │                      │
      ▼                     ▼                      ▼
   Tokio               Blocking              Tokio tasks
   tasks                 pool              + Delta commits
```

**Checkpoint Recovery**: On restart, blizzard reads the last checkpoint and resumes from where it left off, skipping already-processed records and completing any pending uploads.

## Development

```bash
# Run tests
cargo test

# Run benchmarks
cargo bench

# Check formatting
cargo fmt --check

# Run clippy
cargo clippy
```
