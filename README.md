# ❄️ Blizzard

A high-performance Rust tool for streaming NDJSON.gz files to Delta Lake with checkpoint-based recovery for exactly-once processing semantics.

## Features

- **Multi-cloud storage**: S3, GCS, Azure Blob Storage, and local filesystem
- **Exactly-once processing**: Checkpoint-based recovery ensures no duplicates or data loss
- **High throughput**: Concurrent file downloads, parallel decompression, and multipart uploads
- **Delta Lake integration**: Transactional writes with ACID guarantees
- **Resilient error handling**: Skip failed files and continue processing, with Dead Letter Queue support
- **Prometheus metrics**: Built-in metrics endpoint with health checks

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

For detailed configuration options, architecture, and API reference, visit the [documentation site](https://blizzard.dev).

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
