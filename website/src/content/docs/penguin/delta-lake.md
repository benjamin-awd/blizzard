---
title: Delta Lake Commits
description: How Penguin commits staged Parquet files to Delta Lake
---

Penguin discovers Parquet files in the table directory and commits them to Delta Lake with full ACID guarantees. This separation from [Blizzard's Parquet writer](/blizzard/architecture/sink/) provides fault tolerance—if Penguin crashes, Blizzard continues writing files, and Penguin picks up where it left off on restart using watermark-based scanning.

## High Watermark Protocol

Blizzard writes files directly to the table directory. Penguin discovers and commits them using a high watermark to track progress.

### Directory Structure

```
table_uri/
├── _delta_log/              # Delta transaction log (managed by Penguin)
├── date=2024-01-01/         # Partitioned parquet files
│   └── {uuidv7}.parquet
└── ...
```

### Write Protocol

1. Blizzard writes parquet file to `{table_uri}/{partition}/{uuidv7}.parquet`
2. Penguin scans for files above the current watermark
3. Penguin commits discovered files to Delta log
4. Penguin updates watermark to highest committed file path

File names use UUIDv7 which provides lexicographic ordering by time, enabling efficient watermark-based scanning.

## Commit Process

Penguin performs the following steps to commit files to Delta Lake:

```d2
direction: down
Penguin Commit Flow: {
  scan: Scan Directory {
    label: "Scan Directory\nList files above watermark in partition directories"
  }
  metadata: Read Metadata {
    label: "Read Metadata\nParse parquet metadata for record count, schema"
  }
  commit: Delta Commit {
    label: "Delta Commit\nAdd file actions to _delta_log/"
  }
  watermark: Update Watermark {
    label: "Update Watermark\nStore highest committed path for next scan"
  }

  scan -> metadata -> commit -> watermark
}
```

### Polling

Penguin periodically scans the table directory for uncommitted files:

```yaml
tables:
  events:
    poll_interval_secs: 10  # Check every 10 seconds (default)
```

### Delta Checkpoints

Delta Lake creates checkpoint files to speed up log replay:

```yaml
tables:
  events:
    delta_checkpoint_interval: 10  # Checkpoint every 10 commits (default)
```

## Schema Evolution

Penguin automatically handles schema changes in incoming Parquet files. By default, it uses **merge mode** which allows adding new nullable columns while rejecting incompatible changes.

```yaml
tables:
  events:
    schema_evolution: merge  # strict, merge (default), or overwrite
```

For details on how schema evolution works and the available modes, see [Schema Evolution](./schema-evolution/).

## Configuration

### Basic Configuration

```yaml
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

metrics:
  enabled: true
  address: "0.0.0.0:9091"
```

For the full configuration reference, see the [Penguin Configuration](./configuration/).