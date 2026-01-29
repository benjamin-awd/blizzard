---
title: Penguin Configuration
description: Complete configuration reference for Penguin, the Delta Lake commit service
---

This page provides the complete configuration reference for Penguin. For an overview of how Penguin works, see the [Penguin Overview](./).

## Configuration Reference

### Table Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `table_uri` | string | **required** | URI of the Delta Lake table. Staging metadata is read from `{table_uri}/_staging/` |
| `poll_interval_secs` | integer | `10` | Interval in seconds between polling for new files |
| `partition_by` | array | `[]` | Columns to partition the Delta table by |
| `delta_checkpoint_interval` | integer | `10` | Number of commits between Delta checkpoints |
| `schema_evolution` | string | `"merge"` | Schema evolution mode: `strict`, `merge`, or `overwrite`. See [Schema Evolution](./schema-evolution/) |
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
  schema_evolution: merge  # Allow adding new nullable columns
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

Penguin uses the same storage options as Blizzard for cloud provider authentication. See the [Blizzard Configuration Reference](/blizzard/reference/configuration/#source-storage-options) for details on AWS S3, Google Cloud Storage, and Azure Blob Storage options.