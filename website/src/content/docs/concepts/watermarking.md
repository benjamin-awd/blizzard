---
title: Watermarking
description: Efficient incremental file discovery and tracking using high watermarks
---

Watermark-based tracking is a core pattern used throughout the Blizzard/Penguin pipeline. It enables efficient incremental processing without maintaining unbounded in-memory state.

## What Is a Watermark?

A watermark is the file path of the last processed file. Since file paths are lexicographically sortable, components can efficiently find new files by listing only files "above" the watermark—files that sort after it alphabetically.

```
Current watermark: date=2024-01-28/1706450400-01926abc.parquet

Files in storage:
  date=2024-01-28/1706450100-01926ab0.parquet  ← Below watermark (already processed)
  date=2024-01-28/1706450200-01926ab5.parquet  ← Below watermark (already processed)
  date=2024-01-28/1706450400-01926abc.parquet  ← Watermark (already processed)
  date=2024-01-28/1706450500-01926abd.parquet  ← Above watermark (new file!)
  date=2024-01-29/1706536800-01926b00.parquet  ← Above watermark (new file!)
```

## Where Watermarks Are Used

| Component | Purpose | Watermark Tracks |
|-----------|---------|------------------|
| **Blizzard** | Source file discovery | Last processed source file (`.ndjson.gz`) |
| **Penguin** | Commit tracking | Last committed Parquet file to Delta Lake |

Both use the same underlying mechanism but for different purposes:

- **Blizzard** uses watermarks to discover new source files to ingest
- **Penguin** uses watermarks to discover uncommitted Parquet files to add to Delta Lake

## Watermark Format

Watermarks are lexicographically-sortable file paths that include partition prefixes:

```
# Single partition level
date=2024-01-28/1706450400-01926abc.ndjson.gz

# Nested partitions
date=2024-01-28/hour=14/1706450400-01926abc.ndjson.gz
```

For lexicographic ordering to match chronological order, file names must use sortable identifiers:

- **Timestamp prefixes**: Unix timestamps ensure files sort by creation time
- **UUIDv7**: Time-ordered UUIDs that sort chronologically

## Lexicographic Comparison

Watermark comparison uses standard string comparison—comparing characters left to right until a difference is found.

For filenames:

```
1706450500-uuid3.parquet
1706450400-uuid2.parquet
       ↑
       Position 7: '5' > '4'
       Result: "1706450500-uuid3" > "1706450400-uuid2"
```

For partitions:

```
date=2024-01-29
date=2024-01-28
              ↑
              Position 14: '9' > '8'
              Result: "date=2024-01-29" > "date=2024-01-28"
```

This is why file naming conventions matter—timestamps and UUIDv7 are designed so lexicographic order matches chronological order:

| Naming Scheme | Lexicographic = Chronological? |
|---------------|-------------------------------|
| `1706450400-uuid.parquet` | Yes (Unix timestamp prefix) |
| `01926abc-def0.parquet` | Yes (UUIDv7 is time-ordered) |
| `2024-01-28T14:00:00.parquet` | Yes (ISO 8601 sorts correctly) |
| `event-abc123.parquet` | No (random, won't work) |

## Efficient Partition Scanning

When listing files above a watermark, the system optimizes by:

1. **Parsing the watermark** into partition prefix and filename
2. **Filtering partitions** to only scan partitions >= the watermark's partition
3. **Filtering files** within the watermark's partition to those > the watermark filename

This means if your watermark is in `date=2024-01-28/`, earlier partitions like `date=2024-01-27/` are skipped entirely.

```
Watermark: date=2024-01-28/file2.parquet

Partitions:
  date=2024-01-26/  ← Skipped (before watermark partition)
  date=2024-01-27/  ← Skipped (before watermark partition)
  date=2024-01-28/  ← Scanned (watermark partition, files > file2)
  date=2024-01-29/  ← Scanned (after watermark partition, all files)
```

## Watermark State Tracking

Blizzard tracks not just the watermark position but also the **activity state** of the watermark. This provides operational visibility into why no files were processed on a given poll cycle.

### State Machine

The watermark transitions between three states:

```
                    ┌─────────────────────────────────┐
                    │                                 │
                    ▼                                 │
┌─────────┐   update_watermark()   ┌────────┐   update_watermark()
│ Initial │ ─────────────────────► │ Active │ ◄─────────────────────┐
└─────────┘                        └────────┘                       │
                                       │                            │
                                       │ mark_idle()                │
                                       │ (no new files)             │
                                       ▼                            │
                                   ┌────────┐                       │
                                   │  Idle  │ ──────────────────────┘
                                   └────────┘   (new files appear)
```

| State | Meaning |
|-------|---------|
| **Initial** | Cold start—no watermark has been set yet |
| **Active** | Actively processing files at the current position |
| **Idle** | No new files found above the watermark on the last poll |

### Why Track Idle State?

Without idle tracking, when no files are returned from a poll cycle, you can't distinguish between:

- **No files exist** — The source location is empty
- **Files filtered by watermark** — Files exist but all are at or below the watermark

The idle state makes this explicit: if the state is `Idle`, files exist but have already been processed. This is useful for:

- **Operational dashboards** — Show whether a pipeline is waiting for new data vs. actively processing
- **Alerting** — Distinguish "no new data" (expected) from "no data at all" (potential issue)
- **Debugging** — Quickly see if files are being filtered vs. missing

### Checkpoint Format

The watermark state is persisted in the checkpoint file:

```json
{
  "schema_version": 1,
  "watermark": {
    "state": "Active",
    "value": "date=2024-01-28/1706450400-uuid.ndjson.gz"
  },
  "partition_watermarks": {
    "date=2024-01-28": "1706450400-uuid.ndjson.gz",
    "date=2024-01-29": "1706536800-uuid.ndjson.gz"
  },
  "last_update_ts": 1706450500
}
```

## Per-Partition Watermarks

For high-throughput pipelines with **concurrent partitions** receiving data simultaneously, Blizzard tracks per-partition watermarks to avoid redundant scanning.

### The Problem

Consider a pipeline ingesting 1000+ files per hour partition, with 2 hour partitions active at any time:

```
date=2024-01-28/hour=13/  ← 1000 files, watermark at file #950
date=2024-01-28/hour=14/  ← 500 files, watermark at file #500
```

With a single global watermark at `hour=14/file500.ndjson.gz`:

- Every poll scans **all 500 files** in `hour=14/` to find new ones
- The `hour=13/` partition (with 50 unprocessed files) is completely ignored because the global watermark is in a "later" partition

### The Solution

Per-partition watermarks track progress independently within each partition:

```json
{
  "watermark": {"state": "Active", "value": "date=2024-01-28/hour=14/file500.ndjson.gz"},
  "partition_watermarks": {
    "date=2024-01-28/hour=13": "file950.ndjson.gz",
    "date=2024-01-28/hour=14": "file500.ndjson.gz"
  }
}
```

Now each partition is filtered independently:

- `hour=13/` — Only scans files above `file950` (finds 50 new files)
- `hour=14/` — Only scans files above `file500` (finds new files as they arrive)

### Behavior for New Partitions

When a partition has no entry in `partition_watermarks`:

- **All files are included** — No watermark means no filtering
- The global watermark is **not** used as a fallback (would incorrectly skip files)

This is correct for truly new partitions that appear after the pipeline started.

### Performance Impact

For a pipeline with 2 concurrent hour partitions receiving 1000 files/hour each:

| Approach | Files Listed Per Poll | Files Filtered |
|----------|----------------------|----------------|
| Global watermark only | 2000+ | Up to 1000 (entire newer partition) |
| Per-partition watermarks | Only new files | Minimal |

The improvement is most significant when:
- Multiple partitions receive data simultaneously
- Partitions have many files (100s to 1000s)
- Poll intervals are short (seconds to minutes)

## Watermark Advancement Atomicity

Watermarks only advance after successful sink writes. This ensures exactly-once semantics even during failures.

### Guarantee

In the processing loop:

```rust
// 1. Write to sink (must succeed)
ctx.sink.write_file_batches(&path, batches).await?;

// 2. Only then advance watermark
ctx.multi_tracker.mark_processed(&source_name, &path);
```

The `?` operator ensures that if the sink write fails:
- The error propagates immediately
- `mark_processed()` is never called
- The watermark stays at its previous position

### Recovery Behavior

If the pipeline crashes or restarts:

1. **Watermark is at last successfully written file**
2. **Failed file will be reprocessed** on restart
3. **No data loss** — Files are either fully processed or will be retried

:::tip[Idempotent Writes]
For true exactly-once semantics, ensure your sink is idempotent (e.g., Delta Lake with deduplication) so reprocessing the same file doesn't create duplicates.
:::

## Blizzard: Source File Discovery

Blizzard uses watermarks to track which source files have been processed.

### Cold Start Behavior

On first run (no watermark exists), Blizzard must discover existing files. Two modes are available:

**Full Scan** — Without a partition filter, Blizzard scans all files recursively:

```yaml
sources:
  events:
    source_uri: "s3://bucket/events/"
    use_watermark: true
    # No partition_filter - scans everything
```

**Filtered Scan** — With a partition filter, only recent partitions are scanned:

```yaml
sources:
  events:
    source_uri: "s3://bucket/events/"
    use_watermark: true
    partition_filter:
      prefix_template: "date=%Y-%m-%d"
      lookback: 7  # Only scan last 7 days
```

The `lookback` parameter generates partition prefixes for the last N days—useful when you have years of historical data but only want to process recent files.

### Configuration

```yaml
sources:
  events:
    source_uri: "s3://bucket/raw-events/"
    use_watermark: true  # Enable watermark tracking
    partition_filter:
      prefix_template: "date=%Y-%m-%d"
      lookback: 7
    checkpoint:
      interval_files: 100   # Checkpoint every 100 files
      interval_secs: 60     # Or every 60 seconds
```

| Field | Description | Default |
|-------|-------------|---------|
| `use_watermark` | Enable watermark-based tracking | `false` |
| `partition_filter.prefix_template` | strftime template for partition prefixes | - |
| `partition_filter.lookback` | Days to look back on cold start | - |

## Penguin: Commit Tracking

Penguin uses watermarks to track which Parquet files have been committed to Delta Lake.

### High Watermark Protocol

The watermark is persisted in the Delta transaction log using `txn` actions:

```json
{
  "txn": {
    "appId": "penguin-events",
    "version": 42,
    "lastUpdated": 1706450400000
  }
}
```

On each polling cycle, Penguin:

1. Reads the current watermark from the Delta log
2. Lists Parquet files above the watermark
3. Commits new files to Delta Lake
4. Updates the watermark to the highest committed path

### Crash Recovery

If Penguin crashes after committing but before updating the watermark:

- Delta Lake's optimistic concurrency rejects duplicate file additions
- Penguin safely resumes from the last persisted watermark

See [Fault Tolerance](/blizzard/concepts/fault-tolerance/) for detailed failure scenarios.

## Checkpoint Storage

Blizzard persists checkpoint state to cloud storage at:

```
{table_uri}/_blizzard/{pipeline}_{source}_checkpoint.json
```

For example:
```
s3://my-bucket/events/_blizzard/events_asia_checkpoint.json
s3://my-bucket/events/_blizzard/events_europe_checkpoint.json
```

### Atomic Writes

Checkpoints use atomic write semantics:

1. Write to temporary file: `{pipeline}_{source}_checkpoint.json.tmp`
2. Atomic rename to final path: `{pipeline}_{source}_checkpoint.json`

This ensures checkpoints are never partially written, even during crashes.

### Checkpoint Frequency

Configure how often checkpoints are saved:

```yaml
sources:
  events:
    checkpoint:
      interval_files: 100   # After every N files processed
      interval_secs: 60     # Or after N seconds elapsed
```

More frequent checkpoints mean less reprocessing after crashes but more I/O overhead.

## Requirements

Watermark-based tracking requires:

1. **Lexicographic ordering**: File names must sort in chronological order
2. **Immutable files**: Files must not be modified after creation
3. **Append-only**: New files must have names that sort after existing files

:::caution[File Naming]
If files don't follow lexicographic ordering (e.g., random UUIDs without timestamps), watermark tracking won't work correctly. Use timestamp prefixes or UUIDv7 for file names.
:::

## Comparison with Set-Based Tracking

| Aspect | Watermark | Set-Based |
|--------|-----------|-----------|
| Memory usage | O(1) - single path | O(n) - all processed paths |
| Recovery | Resume from watermark | Reload full set |
| Requirements | Lexicographic file names | Any file names |
| Reprocessing | Cannot reprocess old files | Can mark files unprocessed |
| Concurrent partitions | Efficient with per-partition watermarks | Always O(n) |

Watermark tracking is preferred for high-volume pipelines where memory efficiency and fast recovery are important.

## Debugging Watermarks

### View Current State

Check the checkpoint file directly:

```bash
# S3
aws s3 cp s3://bucket/table/_blizzard/pipeline_source_checkpoint.json -

# GCS
gsutil cat gs://bucket/table/_blizzard/pipeline_source_checkpoint.json
```

### Debug Logging

Enable debug logs to see watermark filtering:

```bash
RUST_LOG=blizzard=debug blizzard run config.yaml
```

You'll see:
- Partition watermark updates
- Files filtered by watermark
- Idle state transitions

### Common Issues

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| Files never discovered | File names don't sort chronologically | Use timestamp prefixes |
| Same files reprocessed | Checkpoint not persisting | Check storage permissions |
| Older partition ignored | Global watermark in newer partition | Per-partition watermarks handle this |
| Pipeline always idle | No new files arriving | Check source location |
