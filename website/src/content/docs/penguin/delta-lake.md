---
title: Delta Lake Commits
description: How Penguin commits staged Parquet files to Delta Lake
---

Penguin watches a staging directory for completed Parquet files and commits them to Delta Lake with full ACID guarantees. This separation from [Blizzard's Parquet writer](/blizzard/architecture/sink/) provides fault tolerance—if Penguin crashes, Blizzard continues writing to staging, and Penguin picks up where it left off on restart.

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

## Commit Process

Penguin performs the following steps to commit files to Delta Lake:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Penguin Commit Flow                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────┐                                                        │
│  │   Poll Staging   │  Check _staging/pending/ for .meta.json files          │
│  └──────────────────┘                                                        │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────┐                                                        │
│  │  Read Metadata   │  Parse file info, partition values, record count       │
│  └──────────────────┘                                                        │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────┐                                                        │
│  │  Delta Commit    │  Add file actions to _delta_log/                       │
│  └──────────────────┘                                                        │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────┐                                                        │
│  │  Cleanup Meta    │  Delete .meta.json after successful commit             │
│  └──────────────────┘                                                        │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Polling

Penguin periodically polls the staging directory for new metadata files:

```yaml
source:
  poll_interval_secs: 10  # Check every 10 seconds (default)
```

### Delta Checkpoints

Delta Lake creates checkpoint files to speed up log replay:

```yaml
source:
  delta_checkpoint_interval: 10  # Checkpoint every 10 commits (default)
```

## Configuration

### Basic Configuration

```yaml
source:
  table_uri: "s3://my-bucket/delta-tables/events"
  poll_interval_secs: 10
  partition_by:
    - date
    - region
  delta_checkpoint_interval: 10
  storage_options:
    AWS_REGION: "us-east-1"

metrics:
  enabled: true
  address: "0.0.0.0:9091"
```

For the full configuration reference, see the [Penguin Configuration](./configuration/).