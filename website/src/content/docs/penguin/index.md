---
title: Penguin Overview
description: Penguin is the Delta Lake commit service for the Blizzard pipeline
---

Penguin is the second stage of the Blizzard data pipeline. It discovers Parquet files in the table directory and commits them to Delta Lake with full ACID guarantees.

## How It Works

While Blizzard handles the ingestion of source data and writes Parquet files directly to the table, Penguin's job is to:

1. Scan the table directory for uncommitted Parquet files
2. Track a high watermark to efficiently find new files
3. Commit files to the Delta Lake table atomically

This two-stage architecture provides better fault tolerance—if Penguin crashes, Blizzard can continue writing files, and Penguin will pick up where it left off on restart using watermark-based scanning.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Source    │────▶│    Blizzard      │────▶│   Staging   │
│  (NDJSON)   │     │  (Parquet Writer)│     │  Directory  │
└─────────────┘     └──────────────────┘     └──────┬──────┘
                                                   │
                                                   ▼
                                           ┌─────────────┐
                                           │   Penguin   │
                                           │ (Committer) │
                                           └──────┬──────┘
                                                  │
                                                  ▼
                                           ┌─────────────┐
                                           │ Delta Lake  │
                                           │   Table     │
                                           └─────────────┘
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
