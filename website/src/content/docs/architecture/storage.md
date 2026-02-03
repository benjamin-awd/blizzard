---
title: Storage Backends
description: Multi-cloud storage abstraction supporting S3, GCS, Azure Blob, and local filesystem
---

Blizzard provides a unified storage abstraction layer that works seamlessly with Amazon S3, Google Cloud Storage, Azure Blob Storage, and local filesystems. This allows the same pipeline configuration to work across different cloud providers.

## Supported Backends

| Backend | URL Schemes | Description |
|---------|-------------|-------------|
| **Amazon S3** | `s3://`, `s3a://` | AWS S3 and S3-compatible storage |
| **Google Cloud Storage** | `gs://` | GCP Cloud Storage |
| **Azure Blob Storage** | `abfss://`, `https://*.blob.core.windows.net` | Azure Data Lake Gen2 |
| **Local Filesystem** | `file://`, `/path` | Local disk storage |

## URL Formats

### Amazon S3

```yaml
# Standard S3 URL
source:
  path: "s3://my-bucket/path/to/data"

# S3-compatible endpoint (MinIO, LocalStack, etc.)
source:
  path: "s3a::http://localhost:9000/my-bucket/data"

# Virtual-hosted style
source:
  path: "https://my-bucket.s3.us-east-1.amazonaws.com/path"

# Path-style
source:
  path: "https://s3.us-east-1.amazonaws.com/my-bucket/path"
```

### Google Cloud Storage

```yaml
# Standard GCS URL
source:
  path: "gs://my-bucket/path/to/data"

# HTTPS format
source:
  path: "https://storage.googleapis.com/my-bucket/path"

# Virtual-hosted style
source:
  path: "https://my-bucket.storage.googleapis.com/path"
```

### Azure Blob Storage

```yaml
# ABFS URL (Azure Data Lake Storage Gen2)
source:
  path: "abfss://container@account.dfs.core.windows.net/path"

# HTTPS format
source:
  path: "https://account.blob.core.windows.net/container/path"
```

### Local Filesystem

```yaml
# Absolute path
source:
  path: "/data/input/files"

# File URI
source:
  path: "file:///data/input/files"
```

## Authentication

### AWS S3

Authentication uses the standard AWS credential chain:

| Method | Description |
|--------|-------------|
| Environment variables | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |
| Shared credentials | `~/.aws/credentials` file |
| IAM role | EC2 instance role, ECS task role |
| Web identity | IRSA (EKS), Workload Identity |

Additional storage options:

```yaml
source:
  storage_options:
    AWS_REGION: "us-east-1"
    AWS_ENDPOINT: "http://localhost:9000"  # For S3-compatible
    AWS_ALLOW_HTTP: "true"                  # Allow non-HTTPS
```

### Google Cloud Storage

Authentication uses Application Default Credentials:

| Method | Description |
|--------|-------------|
| Environment variable | `GOOGLE_APPLICATION_CREDENTIALS` |
| gcloud CLI | `gcloud auth application-default login` |
| Service account | Attached to GCE/GKE |
| Workload Identity | GKE Workload Identity |

### Azure Blob Storage

```yaml
source:
  storage_options:
    AZURE_STORAGE_ACCOUNT_NAME: "myaccount"
    AZURE_STORAGE_ACCOUNT_KEY: "..."
    # Or use SAS token
    AZURE_STORAGE_SAS_TOKEN: "..."
```

## Storage Operations

The `StorageProvider` abstraction provides these operations:

| Operation | Description | Metrics |
|-----------|-------------|---------|
| `list()` | List files in a path | `blizzard_storage_requests_total{operation="list"}` |
| `get()` | Download file contents | `blizzard_storage_requests_total{operation="get"}` |
| `put()` | Upload file contents | `blizzard_storage_requests_total{operation="put"}` |
| `put_multipart_bytes()` | Parallel multipart upload | Multiple part metrics |

## Multipart Uploads

Large files are uploaded using parallel multipart uploads:

```d2
direction: down
Parallel Multipart Upload: {
  file: "File (256 MB)" {
    grid-columns: 6
    p1: "Part 1\n32 MB"
    p2: "Part 2\n32 MB"
    p3: "Part 3\n32 MB"
    p4: "Part 4\n32 MB"
    p5: "Part 5\n32 MB"
    p6: "Part 6\n32 MB"
  }
  concurrent: "Concurrent Part Uploads (max: 8)"
  complete: Complete Multipart

  file -> concurrent -> complete
}
```

### Configuration

```yaml
sink:
  part_size_mb: 32              # Size per part (default: 32 MB)
  min_multipart_size_mb: 5      # Minimum size to use multipart (default: 5 MB)
  max_concurrent_parts: 8       # Concurrent part uploads (default: 8)
```

### How It Works

1. **Check size**: Files smaller than `min_multipart_size_mb` use simple PUT
2. **Create multipart**: Initialize multipart upload, get upload ID
3. **Upload parts**: Upload parts in parallel with explicit part numbers
4. **Complete**: Finalize the multipart upload

## Path Handling

Paths are handled consistently across backends:

- **Listing**: Returns paths relative to the configured prefix
- **Get/Put**: Automatically qualifies paths with the prefix
- **Round-trip**: `list()` results can be passed directly to `get()`

```rust
// Storage at s3://bucket/prefix/
let files = storage.list(true).await?;  // Returns ["file1.parquet", "subdir/file2.parquet"]
let data = storage.get("file1.parquet").await?;  // Gets s3://bucket/prefix/file1.parquet
```

## File Listing

The `list_ndjson_files()` function recursively lists `.ndjson.gz` files:

```rust
let files = list_ndjson_files(&storage).await?;
// Returns sorted list: ["path/file1.ndjson.gz", "path/file2.ndjson.gz", ...]
```

Behavior:
- Recursively lists all subdirectories
- Filters to `.ndjson.gz` extension only
- Returns paths sorted alphabetically
- Paths are relative to the configured prefix

## Metrics

All storage operations emit metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `blizzard_storage_requests_total` | Counter | Total requests by operation and status |
| `blizzard_storage_request_duration_seconds` | Histogram | Request latency by operation |
| `blizzard_active_multipart_parts` | Gauge | Currently uploading parts |
| `blizzard_multipart_uploads_total` | Counter | Completed multipart uploads |

Labels:
- `operation`: `get`, `put`, `list`, `create_multipart`, `put_part`, `complete_multipart`
- `status`: `success`, `error`