---
title: Penguin Reference
description: Complete reference for Penguin, the Delta Lake commit service
---

Penguin is the second stage of the Blizzard data pipeline. It watches a staging directory for completed Parquet files and commits them to Delta Lake with full ACID guarantees.

## Overview

While Blizzard handles the ingestion of source data and writes Parquet files to a staging area, Penguin's job is to:

1. Poll the staging directory for new files
2. Commit files to the Delta Lake table atomically
3. Clean up staging metadata after successful commits

This two-stage architecture provides better fault tolerance—if Penguin crashes, Blizzard can continue writing to staging, and Penguin will pick up where it left off on restart.

## How It Works with Blizzard

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Source    │────▶│    Blizzard      │────▶│   Staging   │
│  (NDJSON)   │     │  (Parquet Writer)│     │  Directory  │
└─────────────┘     └──────────────────┘     └──────┬──────┘
                                                    │
                                                    ▼
                                            ┌─────────────┐
                                            │   Penguin   │
                                            │ (Committer) │
                                            └──────┬──────┘
                                                   │
                                                   ▼
                                            ┌─────────────┐
                                            │ Delta Lake  │
                                            │   Table     │
                                            └─────────────┘
```

Blizzard writes Parquet files directly to the table directory (e.g., `{table_uri}/{partition}/`) and metadata to `{table_uri}/_staging/pending/`. Penguin monitors the staging directory for metadata files and commits the corresponding Parquet files to the Delta Lake table.

## CLI Options

```
penguin [OPTIONS] --config <CONFIG>

Options:
  -c, --config <CONFIG>    Path to the configuration file
      --log-level <LEVEL>  Log level: trace, debug, info, warn, error [default: info]
  -h, --help               Print help
  -V, --version            Print version
```

## Configuration Reference

### Table Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `table_uri` | string | **required** | URI of the Delta Lake table. Staging metadata is read from `{table_uri}/_staging/` |
| `poll_interval_secs` | integer | `10` | Interval in seconds between polling for new files |
| `partition_by` | array | `[]` | Columns to partition the Delta table by |
| `delta_checkpoint_interval` | integer | `10` | Number of commits between Delta checkpoints |
| `max_concurrent_uploads` | integer | `4` | Maximum concurrent file uploads |
| `max_concurrent_parts` | integer | `8` | Maximum concurrent parts per multipart upload |
| `part_size_mb` | integer | `10` | Part size for multipart uploads in MB |
| `min_multipart_size_mb` | integer | `100` | Minimum file size to trigger multipart upload |
| `storage_options` | map | `{}` | Cloud provider credentials and options |

### Metrics Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable Prometheus metrics endpoint |
| `address` | string | `"0.0.0.0:9090"` | Address to bind the metrics HTTP server |

## Full Configuration Example

```yaml
# Source: Delta Lake table to commit to
source:
  table_uri: "s3://my-bucket/delta-tables/events"
  poll_interval_secs: 10
  partition_by:
    - date
    - region
  delta_checkpoint_interval: 10
  max_concurrent_uploads: 4
  max_concurrent_parts: 8
  part_size_mb: 10
  min_multipart_size_mb: 100
  storage_options:
    AWS_REGION: "us-east-1"

# Metrics: Prometheus endpoint
metrics:
  enabled: true
  address: "0.0.0.0:9091"  # Use different port than Blizzard
```

## Environment Variable Interpolation

Like Blizzard, Penguin supports environment variable interpolation:

```yaml
source:
  table_uri: "${DELTA_TABLE_URI}"
  storage_options:
    AWS_ACCESS_KEY_ID: "${AWS_ACCESS_KEY_ID}"
    AWS_SECRET_ACCESS_KEY: "${AWS_SECRET_ACCESS_KEY}"
```

Syntax:
- `${VAR}` - Required variable (fails if not set)
- `${VAR:-default}` - With default value

## Storage Options

Penguin uses the same storage options as Blizzard for cloud provider authentication. See the [Configuration Reference](/blizzard/reference/configuration/#source-storage-options) for details on AWS S3, Google Cloud Storage, and Azure Blob Storage options.

## Code References

| Component | File |
|-----------|------|
| Config structs | `crates/penguin/src/config/mod.rs` |
| Source config | `crates/penguin/src/config/mod.rs:20` |
| Metrics config | `crates/blizzard-common/src/config/mod.rs:16` |
