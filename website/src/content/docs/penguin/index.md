---
title: Overview
description: Penguin is the Delta Lake commit service for the Blizzard pipeline
---

Penguin is the second stage of the Blizzard data pipeline. It discovers Parquet files in the table directory and commits them to Delta Lake with full ACID guarantees.

## How It Works

While Blizzard handles the ingestion of source data and writes Parquet files directly to the table, Penguin's job is to:

1. Scan the table directory for uncommitted Parquet files
2. Track a high watermark to efficiently find new files
3. Commit files to the Delta Lake table atomically

This two-stage architecture provides better fault tolerance—if Penguin crashes, Blizzard can continue writing files, and Penguin will pick up where it left off on restart using watermark-based scanning.

:::caution[Watermark-only tracking]
Penguin only supports watermark-based tracking, which assumes files are written in lexicographical order and never modified after creation.
:::

## Architecture

```d2
direction: right
source: Source {
  label: "Source\n(NDJSON)"
}
blizzard: Blizzard {
  label: "Blizzard\n(Parquet Writer)"
}
staging: Staging Directory

source -> blizzard -> staging

penguin: Penguin {
  label: "Penguin\n(Committer)"
}
delta: Delta Lake Table

staging -> penguin -> delta
```

Blizzard writes Parquet files directly to the table directory (e.g., `{table_uri}/{partition}/{uuid}.parquet`). Penguin scans the table directory, discovers uncommitted files using watermark-based tracking, and commits them to the Delta Lake table.

## CLI Usage

```
penguin [OPTIONS] --config <CONFIG>

Options:
  -c, --config <CONFIG>    Path to the configuration file
      --log-level <LEVEL>  Log level: trace, debug, info, warn, error [default: info]
  -h, --help               Print help
  -V, --version            Print version
```

## Next Steps

- [Delta Lake Commits](./delta-lake/) - How Penguin commits files to Delta Lake
- [Schema Evolution](./schema-evolution/) - Handling schema changes in incoming data
- [Fault Tolerance](./fault-tolerance/) - Crash recovery and exactly-once semantics
- [Configuration](./configuration/) - Full configuration reference
