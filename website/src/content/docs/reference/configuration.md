---
title: Configuration Reference
description: Complete reference for Blizzard YAML configuration options
---

Blizzard is configured using a YAML file that defines source, sink, schema, and optional settings.

## Configuration File

```yaml
source:
  path: "s3://my-bucket/input/*.ndjson.gz"
  compression: gzip
  batch_size: 8192
  max_concurrent_files: 16
  storage_options:
    AWS_REGION: "us-east-1"

sink:
  path: "s3://my-bucket/output/table"
  file_size_mb: 128
  compression: snappy
  storage_options:
    AWS_REGION: "us-east-1"

schema:
  fields:
    - name: id
      type: string
    - name: timestamp
      type: timestamp
    - name: value
      type: float64
      nullable: true

metrics:
  enabled: true
  address: "0.0.0.0:9090"

error_handling:
  max_failures: 0
  dlq_path: "s3://my-bucket/dlq"
```

## Environment Variable Interpolation

Configuration values can reference environment variables:

```yaml
source:
  path: "${SOURCE_PATH}"
  storage_options:
    AWS_ACCESS_KEY_ID: "${AWS_ACCESS_KEY_ID}"
    AWS_SECRET_ACCESS_KEY: "${AWS_SECRET_ACCESS_KEY}"
```

Syntax:
- `${VAR}` - Required variable (fails if not set)
- `${VAR:-default}` - With default value

## Source Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | **required** | Path to source files (supports cloud URLs) |
| `compression` | string | `gzip` | Compression format: `gzip`, `zstd`, `none` |
| `batch_size` | integer | `8192` | Records per Arrow batch |
| `max_concurrent_files` | integer | `16` | Concurrent file downloads |
| `storage_options` | map | `{}` | Cloud provider credentials and options |

### Source Storage Options

#### AWS S3

```yaml
storage_options:
  AWS_REGION: "us-east-1"
  AWS_ACCESS_KEY_ID: "AKIA..."
  AWS_SECRET_ACCESS_KEY: "..."
  AWS_SESSION_TOKEN: "..."           # For temporary credentials
  AWS_ENDPOINT: "http://localhost:9000"  # For S3-compatible
  AWS_ALLOW_HTTP: "true"             # Allow non-HTTPS
```

#### Google Cloud Storage

```yaml
storage_options:
  GOOGLE_SERVICE_ACCOUNT_KEY: '{"type":"service_account",...}'
  # Or use GOOGLE_APPLICATION_CREDENTIALS environment variable
```

#### Azure Blob Storage

```yaml
storage_options:
  AZURE_STORAGE_ACCOUNT_NAME: "myaccount"
  AZURE_STORAGE_ACCOUNT_KEY: "..."
  # Or use SAS token
  AZURE_STORAGE_SAS_TOKEN: "..."
```

## Sink Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path` | string | **required** | Path to Delta Lake table |
| `file_size_mb` | integer | `128` | Target Parquet file size in MB |
| `row_group_size_bytes` | integer | `134217728` | Row group size (128 MB) |
| `inactivity_timeout_secs` | integer | none | Roll file after inactivity |
| `rollover_timeout_secs` | integer | none | Max file open duration |
| `compression` | string | `snappy` | Parquet compression codec |
| `part_size_mb` | integer | `32` | Multipart upload part size |
| `min_multipart_size_mb` | integer | `5` | Minimum size for multipart |
| `max_concurrent_uploads` | integer | `4` | Concurrent file uploads |
| `max_concurrent_parts` | integer | `8` | Concurrent parts per upload |
| `storage_options` | map | `{}` | Cloud provider credentials |

### Parquet Compression Codecs

| Value | Description |
|-------|-------------|
| `uncompressed` | No compression |
| `snappy` | Fast compression (default) |
| `gzip` | Good compression ratio |
| `zstd` | Best compression ratio |
| `lz4` | Fastest compression |

## Schema Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `fields` | array | **required** | List of field definitions |

### Field Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | **required** | Field name in JSON |
| `type` | string | **required** | Data type (see below) |
| `nullable` | boolean | `true` | Whether field can be null |

### Supported Field Types

| Type | Arrow Type | JSON Input | Notes |
|------|------------|------------|-------|
| `string` | `Utf8` | `"text"` | UTF-8 string |
| `int32` | `Int32` | `123` | 32-bit signed integer |
| `int64` | `Int64` | `123456789` | 64-bit signed integer |
| `float32` | `Float32` | `1.23` | 32-bit float |
| `float64` | `Float64` | `1.23456789` | 64-bit float |
| `boolean` | `Boolean` | `true`/`false` | Boolean |
| `timestamp` | `Timestamp(Microsecond, UTC)` | `"2024-01-01T00:00:00Z"` | ISO 8601 |
| `date` | `Date32` | `"2024-01-01"` | Date without time |
| `json` | `Utf8` | `{...}` or `[...]` | Stored as JSON string |
| `binary` | `Binary` | `"base64..."` | Base64-encoded bytes |

### Schema Example

```yaml
schema:
  fields:
    - name: event_id
      type: string
      nullable: false
    - name: timestamp
      type: timestamp
      nullable: false
    - name: user_id
      type: string
    - name: event_type
      type: string
    - name: payload
      type: json
      nullable: true
    - name: metrics
      type: float64
      nullable: true
```

## Metrics Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable Prometheus metrics |
| `address` | string | `"0.0.0.0:9090"` | Metrics HTTP server address |

```yaml
metrics:
  enabled: true
  address: "0.0.0.0:9090"
```

Access metrics at `http://localhost:9090/metrics`.

## Error Handling Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_failures` | integer | `0` | Stop after N failures (0 = unlimited) |
| `dlq_path` | string | none | Path to write failed records |
| `dlq_storage_options` | map | `{}` | DLQ storage credentials |

```yaml
error_handling:
  max_failures: 100
  dlq_path: "s3://my-bucket/dlq"
  dlq_storage_options:
    AWS_REGION: "us-east-1"
```

## Full Configuration Example

```yaml
# Source: Where to read NDJSON files from
source:
  path: "s3://data-lake/raw/events/2024"
  compression: gzip
  batch_size: 8192
  max_concurrent_files: 16
  storage_options:
    AWS_REGION: "us-east-1"

# Sink: Where to write Delta Lake table
sink:
  path: "s3://data-lake/curated/events"
  file_size_mb: 128
  row_group_size_bytes: 134217728
  compression: snappy
  inactivity_timeout_secs: 60
  rollover_timeout_secs: 300
  max_concurrent_uploads: 4
  part_size_mb: 32
  min_multipart_size_mb: 5
  max_concurrent_parts: 8
  storage_options:
    AWS_REGION: "us-east-1"

# Schema: Structure of the JSON data
schema:
  fields:
    - name: event_id
      type: string
      nullable: false
    - name: timestamp
      type: timestamp
      nullable: false
    - name: user_id
      type: string
    - name: event_type
      type: string
    - name: device
      type: json
    - name: metrics
      type: float64
      nullable: true

# Metrics: Prometheus endpoint
metrics:
  enabled: true
  address: "0.0.0.0:9090"

# Error handling: Resilience settings
error_handling:
  max_failures: 100
  dlq_path: "s3://data-lake/dlq/events"
  dlq_storage_options:
    AWS_REGION: "us-east-1"
```

## Validation

Configuration is validated on load:

| Check | Error |
|-------|-------|
| Source path empty | `Source path cannot be empty` |
| Sink path empty | `Sink path cannot be empty` |
| No schema fields | `Schema must have at least one field` |
| Missing env var | `Environment variable interpolation failed` |
| Invalid YAML | `Failed to parse YAML configuration` |

## Code References

| Component | File |
|-----------|------|
| Config structs | `src/config/mod.rs` |
| Environment interpolation | `src/config/vars.rs` |
| Schema to Arrow | `src/config/mod.rs:290` |
| Validation | `src/config/mod.rs:282` |
