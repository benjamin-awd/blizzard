---
title: Penguin Configuration
description: Complete configuration reference for Penguin, the Delta Lake commit service
---

This page provides the complete configuration reference for Penguin.

## Configuration Reference

### Table Configuration

Penguin supports multiple tables via the `tables` map. Each table is identified by a unique key.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `table_uri` | string | **required** | URI of the Delta Lake table. Penguin scans for uncommitted parquet files in this directory. |
| `poll_interval_secs` | integer | `10` | Interval in seconds between polling for new files |
| `partition_by` | object | - | Partition configuration with strftime-style prefix template |
| `partition_by.prefix_template` | string | - | strftime-style template (e.g., `"date=%Y-%m-%d/hour=%H"`) |
| `partition_filter` | object | - | Partition filter for cold start when no watermark exists |
| `partition_filter.prefix_template` | string | - | strftime-style template for date-based filtering |
| `partition_filter.lookback` | integer | `0` | Number of units to look back (days or hours depending on template) |
| `delta_checkpoint_interval` | integer | `10` | Number of commits between Delta checkpoints |
| `schema_evolution` | string | `"merge"` | Schema evolution mode: `strict`, `merge`, or `overwrite`. See [Schema Evolution](./schema-evolution/) |
| `max_concurrent_uploads` | integer | `4` | Maximum concurrent file uploads |
| `max_concurrent_parts` | integer | `8` | Maximum concurrent parts per multipart upload |
| `part_size_mb` | integer | `10` | Part size for multipart uploads in MB |
| `min_multipart_size_mb` | integer | `100` | Minimum file size to trigger multipart upload |
| `storage_options` | map | `{}` | Cloud provider credentials and options |

### Global Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `total_concurrency` | integer | - | Limit total concurrent operations across all tables |

### Metrics Configuration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable Prometheus metrics endpoint |
| `address` | string | `"0.0.0.0:9090"` | Address to bind the metrics HTTP server |

## Full Configuration Example

```yaml
# Tables: Named Delta Lake tables to commit to
tables:
  events:
    table_uri: "s3://my-bucket/delta-tables/events"
    poll_interval_secs: 10
    partition_by:
      prefix_template: "date=%Y-%m-%d/region=%s"
    partition_filter:
      prefix_template: "date=%Y-%m-%d"
      lookback: 7  # Scan last 7 days on cold start
    delta_checkpoint_interval: 10
    schema_evolution: merge
    storage_options:
      AWS_REGION: "us-east-1"

  users:
    table_uri: "s3://my-bucket/delta-tables/users"
    poll_interval_secs: 60
    storage_options:
      AWS_REGION: "us-east-1"

# Global: Shared settings
global:
  total_concurrency: 8

# Metrics: Prometheus endpoint
metrics:
  enabled: true
  address: "0.0.0.0:9091"
```

## Partition Configuration

### partition_by

The `partition_by` option uses strftime-style templates to define how output files are partitioned:

```yaml
tables:
  events:
    table_uri: "s3://bucket/events"
    partition_by:
      prefix_template: "date=%Y-%m-%d/hour=%H"
```

Partition columns are automatically extracted from the template. For example, `"date=%Y-%m-%d/hour=%H"` creates partitions with columns `date` and `hour`.

#### Adding partition_by to an existing table

Partition columns are only set in the Delta Lake table metadata at table creation time. If you add `partition_by` to a table that already exists:

- **Existing files are not repartitioned** - Data already committed remains in its current location
- **Table metadata is not modified** - The Delta Lake schema's partition columns are not updated
- **No automatic backfill** - Penguin does not reorganize or migrate existing data

To properly partition an existing table, you must either:
1. Create a new table with the partition configuration and migrate data separately
2. Delete the existing table and let Penguin recreate it with the new partition configuration

### partition_filter

The `partition_filter` option enables efficient date-based listing during cold starts (when no watermark exists yet):

```yaml
tables:
  events:
    table_uri: "s3://bucket/events"
    partition_filter:
      prefix_template: "date=%Y-%m-%d"
      lookback: 7  # Scan last 7 days
```

This avoids scanning the entire table directory on first startup by limiting the scan to recent partitions.

## Environment Variable Interpolation

Like Blizzard, Penguin supports environment variable interpolation:

```yaml
tables:
  events:
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