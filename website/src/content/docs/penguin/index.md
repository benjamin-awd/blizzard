---
title: Penguin Overview
description: Penguin is the Delta Lake commit service for the Blizzard pipeline
---

Penguin is the second stage of the Blizzard data pipeline. It watches a staging directory for completed Parquet files and commits them to Delta Lake with full ACID guarantees.

## How It Works

While Blizzard handles the ingestion of source data and writes Parquet files to a staging area, Penguin's job is to:

1. Poll the staging directory for new files
2. Commit files to the Delta Lake table atomically
3. Clean up staging metadata after successful commits

This two-stage architecture provides better fault tolerance—if Penguin crashes, Blizzard can continue writing to staging, and Penguin will pick up where it left off on restart.

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

Blizzard writes Parquet files directly to the table directory (e.g., `{table_uri}/{partition}/`) and metadata to `{table_uri}/_staging/pending/`. Penguin monitors the staging directory for metadata files and commits the corresponding Parquet files to the Delta Lake table.

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
