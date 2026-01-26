---
title: Checkpoint & Recovery
description: How Blizzard implements atomic checkpoint/recovery using Delta Lake transactions
---

Blizzard implements an atomic checkpoint/recovery mechanism using Delta Lake's `Txn` (Transaction) actions. This ensures exactly-once processing semantics by storing checkpoint state atomically alongside data commits, eliminating race conditions and data loss scenarios.

## How It Works

```
┌──────────────────────────────────────────────────────────────────┐
│                     Delta Lake Transaction Log                   │
├──────────────────────────────────────────────────────────────────┤
│  Commit N:                                                       │
│    ├── Add { path: "part-00001.parquet", ... }                   │
│    ├── Add { path: "part-00002.parquet", ... }                   │
│    └── Txn { app_id: "blizzard:<base64_checkpoint>", ... }       │
│                              ▲                                   │
│                              │                                   │
│                    Atomic commit (all or nothing)                │
└──────────────────────────────────────────────────────────────────┘
```

Checkpoints are embedded directly in Delta's transaction log rather than stored in separate files. When Blizzard commits processed data, it includes the checkpoint state in the same atomic transaction. If the commit fails, neither the data nor checkpoint is written.

## Checkpoint State

The checkpoint captures:

| Field | Type | Description |
|-------|------|-------------|
| `schema_version` | `u32` | Schema version for forward compatibility |
| `source_state` | `SourceState` | Map of files to processing status |
| `delta_version` | `i64` | Last committed Delta table version |

### Source State

Each source file is tracked with one of two states:

| State | Description |
|-------|-------------|
| `Finished` | File completely processed |
| `RecordsRead(n)` | Partially processed with `n` records read |

Example checkpoint (JSON representation):

```json
{
    "schema_version": 1,
    "source_state": {
        "files": {
            "s3://bucket/file1.ndjson.gz": "Finished",
            "s3://bucket/file2.ndjson.gz": { "RecordsRead": 5000 },
            "s3://bucket/file3.ndjson.gz": "Finished"
        }
    },
    "delta_version": 42
}
```

## Storage Format

Checkpoints are stored in Delta Lake's `Txn` action:

1. State is serialized to JSON
2. JSON is base64-encoded
3. Stored in `Txn.app_id` with prefix `blizzard:`
4. Committed atomically with file `Add` actions

```
Txn.app_id = "blizzard:" + base64(json(CheckpointState))
```

## When Checkpoints Occur

Checkpoints are triggered:

| Trigger | Description |
|---------|-------------|
| **Batch threshold** | Every 10 uploaded Parquet files |
| **Pipeline completion** | Final flush when all files processed |

The batch size balances checkpoint frequency against commit overhead.

## Recovery Process

On startup, Blizzard recovers state in three phases:

### Phase 1: Find Latest Checkpoint

```
┌────────────────────────────────────────────────────┐
│            Delta Transaction Log                   │
│  ┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐  ┌─────┐       │
│  │ v96 │  │ v97 │  │ v98 │  │ v99 │  │v100 │       │
│  └─────┘  └─────┘  └─────┘  └─────┘  └─────┘       │
│                                ▲                   │
│                                │                   │
│                    Scan backwards, find first Txn  │
│                    with "blizzard:" prefix         │
└────────────────────────────────────────────────────┘
```

- Scans **backwards** from latest Delta version
- Searches last **100 commits** (configurable limit for efficiency)
- Stops at first `Txn` action with `blizzard:` prefix
- Decodes base64 → JSON → `CheckpointState`

### Phase 2: Restore State

The recovered checkpoint is loaded into the checkpoint coordinator:

- Source state map is restored
- Delta version is recorded for consistency verification

### Phase 3: Resume Processing

When processing resumes:

1. **Filter pending files**: Skip files marked as `Finished`
2. **Skip processed records**: For `RecordsRead(n)` files, skip first `n` records
3. **Continue normally**: Process remaining files and records

```
Source files: [file1, file2, file3, file4]
Checkpoint:   file1=Finished, file2=RecordsRead(5000)

Result:
  - file1: skipped entirely
  - file2: skip first 5000 records, process remainder
  - file3: process from beginning
  - file4: process from beginning
```

## Failure Scenarios

| Scenario | Behavior |
|----------|----------|
| Crash before commit | No data or checkpoint written; restart from last checkpoint |
| Crash during commit | Delta ensures atomicity; either all written or none |
| Crash after commit | Next restart finds new checkpoint; continues from there |
| Corrupted checkpoint | Falls back to previous checkpoint in log |

## Key Guarantees

- **Exactly-once semantics**: Records are never duplicated or lost
- **Atomic state**: Data and checkpoint always consistent
- **Crash resilience**: Safe recovery from any failure point
- **No external dependencies**: Checkpoints stored in Delta log itself

## Code References

| Component | File |
|-----------|------|
| Checkpoint state structures | `src/checkpoint/state.rs` |
| Checkpoint coordinator | `src/checkpoint/mod.rs` |
| Delta storage & recovery | `src/sink/delta.rs` |
| Source state tracking | `src/source/state.rs` |
| Checkpoint triggering | `src/pipeline/tasks.rs` |

## Tuning

The checkpoint batch size (default: 10 files) can be adjusted by modifying `COMMIT_BATCH_SIZE` in `src/pipeline/tasks.rs`:

- **Smaller batches**: More frequent checkpoints, less re-processing on failure, more Delta commits
- **Larger batches**: Fewer commits, better throughput, more re-processing on failure
