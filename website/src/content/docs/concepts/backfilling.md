---
title: Backfilling
description: How to process historical data and backfill old partitions
---

Backfilling allows you to process historical data that existed before your pipeline was set up, or to reprocess data after resetting your watermark.

## How Backfilling Works

Blizzard and Penguin use [watermark-based tracking](/blizzard/concepts/watermarking/) to process files incrementally. On a **cold start** (when no watermark exists), the system scans for existing files and processes them before entering the normal polling loop.

The `partition_filter` configuration controls how much historical data is scanned on cold start:

| Configuration | Cold Start Behavior |
|--------------|---------------------|
| No `partition_filter` | Scans **all** files in the source directory |
| `partition_filter` with `lookback: 7` | Scans only the last 7 days of partitions |
| `partition_filter` with `lookback: 30` | Scans only the last 30 days of partitions |

## Backfill Strategies

### Full Historical Backfill

To process all historical data, omit the `partition_filter` or set a large `lookback` value:

```yaml
# Penguin: Process all uncommitted files
tables:
  events:
    table_uri: "s3://bucket/events"
    # No partition_filter - scans everything on cold start
```

```yaml
# Blizzard: Process all source files
sources:
  events:
    source_uri: "s3://bucket/raw-events/"
    use_watermark: true
    # No partition_filter - scans everything on cold start
```

### Limited Historical Backfill

To backfill only recent data, configure `partition_filter` with the desired lookback:

```yaml
tables:
  events:
    table_uri: "s3://bucket/events"
    partition_filter:
      prefix_template: "date=%Y-%m-%d"
      lookback: 30  # Backfill last 30 days only
```

This is useful when you have years of data but only need to process recent history.

## Triggering a Backfill

To trigger a backfill, you need to reset the watermark so the system performs a cold start.

### Penguin

Penguin stores its watermark in the Delta transaction log. To reset it:

1. **Delete the Delta table** and let Penguin recreate it:

2. **Or create a new table** by changing the `table_uri`

3. **Restart Penguin** — it will perform a cold start scan

### Blizzard

Blizzard stores its watermark in a checkpoint file. To reset it:

1. **Delete** the checkpoint file

2. **Restart Blizzard** — it will perform a cold start scan

## Backfill Configuration

### partition_filter

Controls which partitions are scanned on cold start:

| Field | Type | Description |
|-------|------|-------------|
| `prefix_template` | string | strftime template matching your partition structure (e.g., `"date=%Y-%m-%d"`) |
| `lookback` | integer | Number of time units to look back (days for `%Y-%m-%d`, hours for `%H`) |

The `lookback` unit is determined by the finest granularity in your template:

| Template | Lookback Unit |
|----------|---------------|
| `date=%Y-%m-%d` | Days |
| `date=%Y-%m-%d/hour=%H` | Hours |
| `year=%Y/month=%m` | Months |

### Checkpoint Configuration (Blizzard)

For large backfills, configure checkpointing to save progress:

```yaml
sources:
  events:
    source_uri: "s3://bucket/raw-events/"
    use_watermark: true
    checkpoint:
      interval_files: 100   # Save checkpoint every 100 files
      interval_secs: 30     # Or every 30 seconds
```

This prevents losing progress if Blizzard restarts during a long backfill.

## Example: Backfilling a New Table

Suppose you have existing Parquet files that were written before Penguin was deployed:

```
s3://bucket/events/
├── date=2024-01-01/
│   └── 1704067200-abc.parquet
├── date=2024-01-02/
│   └── 1704153600-def.parquet
└── date=2024-01-03/
    └── 1704240000-ghi.parquet
```

To backfill these into a Delta Lake table:

1. **Configure Penguin** with the table URI and desired lookback:
   ```yaml
   tables:
     events:
       table_uri: "s3://bucket/events"
       partition_filter:
         prefix_template: "date=%Y-%m-%d"
         lookback: 365  # Scan last year
   ```

2. **Start Penguin** — it will:
   - Detect no existing watermark (cold start)
   - Scan partitions from the last 365 days
   - Commit all discovered files to Delta Lake
   - Set the watermark to the highest file path
   - Begin normal polling for new files

## Backfilling with a Live Instance

If you have a running Penguin instance already processing new files, you can run a separate backfill instance to process historical data concurrently.

### Why a Separate Instance?

The watermark-based design means files "below" the current watermark are never processed. If your live instance has a watermark at `date=2024-02-01/file-xyz`, older partitions like `date=2024-01-15/` won't be picked up — they're already "behind" the watermark.

### Running a Backfill Instance

Run a second Penguin instance with a different application ID:

```yaml
# backfill-penguin.yaml
tables:
  events:
    table_uri: "s3://bucket/events"  # Same table as live instance
    app_id: "penguin-events-backfill"  # Different app ID
    partition_filter:
      prefix_template: "date=%Y-%m-%d"
      lookback: 365  # Scan last year
```

The backfill instance maintains its own watermark (tracked via `appId` in the Delta log) and commits to the same table.

### Avoiding Data Duplication

Delta Lake's optimistic concurrency prevents data duplication automatically:

- If both instances try to commit the same file, the second attempt is rejected
- The `add` action in Delta is idempotent — adding an already-committed file fails safely
- No manual coordination required

### Overlap and Wasted Work

Since `partition_filter` only supports `lookback` (relative to current time), you cannot specify a fixed historical range. The backfill instance may scan some partitions the live instance already processed.

This causes some wasted work (scanning files that are already committed) but not data issues. To minimize overlap:

1. Use a smaller `lookback` on the live instance's cold start configuration
2. Accept that boundary overlap is handled safely by Delta Lake
3. Monitor both instances via metrics to track progress

### When Backfill Completes

Once the backfill instance processes all historical files, it will:

1. Set its watermark to the highest file it found
2. Continue polling but find no new files (the live instance handles those)
3. You can safely shut it down

## Considerations

### Backfill Duration

Backfilling large amounts of historical data can take significant time depending on:

- Number of files to process
- Network latency to storage
- Processing throughput configuration

Monitor progress via metrics or logs.

### Idempotency

Both Blizzard and Penguin are designed to be idempotent:

- **Penguin**: Delta Lake rejects duplicate file additions, so restarting mid-backfill is safe
- **Blizzard**: Checkpointing ensures files aren't reprocessed after a restart

### Ordering

Files are processed in lexicographic order. If your files use timestamp prefixes or UUIDv7, this matches chronological order. See [Watermarking](/blizzard/concepts/watermarking/) for details on file naming requirements.
