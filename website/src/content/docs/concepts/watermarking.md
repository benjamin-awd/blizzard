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

Watermark tracking is preferred for high-volume pipelines where memory efficiency and fast recovery are important.
