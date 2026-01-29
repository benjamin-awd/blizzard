---
title: Fault Tolerance
description: How Blizzard and Penguin provide crash recovery and exactly-once semantics
---

The Blizzard/Penguin pipeline provides fault tolerance through a staging-based coordination protocol. This two-stage architecture ensures data is never lost or duplicated, even when either component crashes.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Fault Tolerance Flow                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐     ┌──────────────────┐     ┌─────────────┐  │
│  │   Blizzard   │────▶│     Staging      │────▶│   Penguin   │  │
│  │  (Parquet)   │     │   (Metadata)     │     │  (Commits)  │  │
│  └──────────────┘     └──────────────────┘     └─────────────┘  │
│                              │                        │          │
│                              ▼                        ▼          │
│                       _staging/pending/        _delta_log/       │
│                       *.meta.json              *.json            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Coordination Protocol

### 1. Blizzard Writes to Staging

When Blizzard finishes processing a source file:

1. Writes Parquet file to `{table_uri}/{partition}/{uuid}.parquet`
2. Writes metadata to `{table_uri}/_staging/pending/{uuid}.meta.json`

The metadata file is written **last**, serving as an atomic signal that the Parquet file is complete.

### 2. Penguin Commits to Delta Lake

Penguin polls the staging directory and for each `.meta.json` file:

1. Reads the metadata to get file information
2. Commits the Parquet file to Delta Lake
3. Deletes the `.meta.json` file

The deletion of the metadata file signals successful commit.

## Failure Scenarios

### Blizzard Crashes Before Metadata Write

```
State: Parquet file written, no metadata
Result: File is orphaned (not committed)
Recovery: Re-run Blizzard to reprocess the source file
```

The orphaned Parquet file can be cleaned up manually or will be ignored by Penguin.

### Blizzard Crashes After Metadata Write

```
State: Parquet file + metadata written
Result: Penguin will commit the file
Recovery: Automatic - Penguin picks up pending files
```

### Penguin Crashes Before Commit

```
State: Metadata exists in _staging/pending/
Result: File not yet committed
Recovery: Automatic - Penguin picks up pending files on restart
```

### Penguin Crashes After Commit, Before Metadata Deletion

```
State: File committed, metadata still exists
Result: Penguin may attempt to re-commit
Recovery: Delta Lake's optimistic concurrency handles duplicates
```

Delta Lake's transactional guarantees ensure that duplicate commits are rejected.

## Guarantees

| Guarantee | How It's Achieved |
|-----------|-------------------|
| **No data loss** | Files remain in staging until committed |
| **No duplicates** | Delta Lake rejects duplicate file additions |
| **Crash resilience** | Both components can restart and resume |
| **Independent scaling** | Blizzard and Penguin operate independently |

## Staging Directory Structure

```
table_uri/
├── _delta_log/                    # Delta transaction log
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   └── ...
├── _staging/
│   └── pending/                   # Pending commits
│       ├── uuid1.meta.json
│       └── uuid2.meta.json
├── date=2024-01-01/               # Committed data files
│   ├── uuid3.parquet
│   └── uuid4.parquet
└── date=2024-01-02/
    └── uuid5.parquet
```

## Comparison to Single-Process Architecture

The two-stage architecture has several advantages over a single process that does both ingestion and commits:

| Aspect | Single Process | Two-Stage (Blizzard + Penguin) |
|--------|----------------|--------------------------------|
| **Failure isolation** | Crash loses in-flight data | Components fail independently |
| **Backpressure** | Commit latency affects ingestion | Ingestion continues while commits catch up |
| **Scaling** | Single bottleneck | Scale writers and committers independently |
| **Recovery** | Must checkpoint all state atomically | Staging metadata is the checkpoint |

## Operational Considerations

### Monitoring Staging Backlog

Monitor the number of pending files in `_staging/pending/`:

```bash
# Count pending files
aws s3 ls s3://bucket/table/_staging/pending/ | wc -l
```

A growing backlog indicates Penguin is falling behind.

### Cleaning Orphaned Files

If Blizzard crashes repeatedly, orphaned Parquet files may accumulate. These can be identified as files without corresponding metadata and removed manually.

### Penguin Polling Interval

Configure Penguin's poll interval based on latency requirements:

```yaml
source:
  poll_interval_secs: 10  # Check for new files every 10 seconds
```

Lower intervals reduce commit latency but increase API calls.