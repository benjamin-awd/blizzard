# ❄️ Blizzard

A high-performance Rust tool for streaming NDJSON.gz files to Parquet, designed for Delta Lake ingestion pipelines.

## Features

- **Multi-cloud storage**: S3, GCS, Azure Blob Storage, and local filesystem
- **High throughput**: Concurrent file downloads, parallel decompression, and multipart uploads
- **Two-stage pipeline**: Blizzard writes Parquet to staging, Penguin commits to Delta Lake
- **Fault tolerant**: Staging-based coordination enables crash recovery
- **Resilient error handling**: Skip failed files and continue processing, with Dead Letter Queue support
- **Prometheus metrics**: Built-in metrics endpoint with health checks

## Architecture

This project includes two binaries:

- **`blizzard`** - Ingests NDJSON files and writes Parquet files to a staging area
- **`penguin`** - Watches the staging area and commits files to Delta Lake

This two-stage architecture provides better fault tolerance. If the commit service crashes, ingestion continues writing to staging. When the commit service restarts, it picks up where it left off.

For details on Penguin configuration, see the [Penguin Reference](https://benjamin-awd.github.io/blizzard/reference/penguin/).

## Installation

```bash
# Clone and build
git clone <repository-url>
cd blizzard
cargo build --release

# Binary will be at target/release/blizzard
```

## Quick Start

1. Create a configuration file (e.g., `config.yaml`):

```yaml
source:
  path: "s3://my-bucket/input/*.ndjson.gz"
  compression: gzip
  max_concurrent_files: 16

sink:
  path: "s3://my-bucket/output/my-table"
  file_size_mb: 128
  compression: snappy

schema:
  fields:
    - name: id
      type: string
      nullable: false
    - name: timestamp
      type: timestamp
      nullable: false
    - name: value
      type: float64
      nullable: true
```

2. Run blizzard:

```bash
blizzard --config config.yaml
```

## CLI Options

```
blizzard [OPTIONS] --config <CONFIG>

Options:
  -c, --config <CONFIG>    Path to the configuration file
      --log-level <LEVEL>  Log level: trace, debug, info, warn, error [default: info]
      --dry-run            Validate configuration without processing
  -h, --help               Print help
  -V, --version            Print version
```

## Metrics & Monitoring

When metrics are enabled, blizzard exposes:

- **`GET /metrics`** - Prometheus metrics endpoint
- **`GET /health`** - Health check endpoint (returns 200 OK)

## Documentation

For detailed configuration options, architecture, and API reference, visit the [documentation site](https://benjamin-awd.github.io/blizzard/).

## Development

```bash
# Run tests
cargo test

# Run benchmarks
cargo bench

# Check formatting
cargo fmt --check

# Run clippy
cargo clippy
```
