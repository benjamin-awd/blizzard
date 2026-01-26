---
title: Metrics Reference
description: Complete reference for Blizzard's Prometheus metrics
---

Blizzard exposes Prometheus metrics for monitoring pipeline health, performance, and throughput.

## Enabling Metrics

```yaml
metrics:
  enabled: true
  address: "0.0.0.0:9090"
```

Access metrics at `http://localhost:9090/metrics`.

## Metric Categories

### Throughput Counters

| Metric | Type | Description |
|--------|------|-------------|
| `blizzard_records_processed_total` | Counter | Total records written to Parquet |
| `blizzard_bytes_read_total` | Counter | Total compressed bytes downloaded |
| `blizzard_bytes_written_total` | Counter | Total Parquet bytes written |
| `blizzard_batches_processed_total` | Counter | Total Arrow batches processed |
| `blizzard_files_processed_total` | Counter | Files processed by status |
| `blizzard_recovered_records_total` | Counter | Records skipped during recovery |

#### File Processing Labels

`blizzard_files_processed_total` includes a `status` label:

| Status | Description |
|--------|-------------|
| `success` | File processed successfully |
| `skipped` | File skipped (not found) |
| `failed` | File failed processing |

### Failure Counters

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `blizzard_files_failed_total` | Counter | `stage` | Failed files by failure stage |

Stage labels:

| Stage | Description |
|-------|-------------|
| `download` | Failed during download |
| `decompress` | Failed during decompression |
| `parse` | Failed during JSON parsing |
| `upload` | Failed during upload |

### Timing Histograms

| Metric | Type | Description |
|--------|------|-------------|
| `blizzard_file_download_duration_seconds` | Histogram | File download latency |
| `blizzard_file_decompression_duration_seconds` | Histogram | File decompression latency |
| `blizzard_parquet_write_duration_seconds` | Histogram | Parquet file write latency |
| `blizzard_delta_commit_duration_seconds` | Histogram | Delta Lake commit latency |

### Concurrency Gauges

| Metric | Type | Description |
|--------|------|-------------|
| `blizzard_active_downloads` | Gauge | Currently downloading files |
| `blizzard_active_uploads` | Gauge | Currently uploading files |
| `blizzard_active_multipart_parts` | Gauge | Currently uploading multipart parts |
| `blizzard_pending_batches` | Gauge | Batches waiting to be written |
| `blizzard_decompression_queue_depth` | Gauge | Files waiting for decompression |

### Storage Request Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `blizzard_storage_requests_total` | Counter | `operation`, `status` | Storage operations |
| `blizzard_storage_request_duration_seconds` | Histogram | `operation` | Storage operation latency |
| `blizzard_multipart_uploads_total` | Counter | - | Completed multipart uploads |

#### Operation Labels

| Operation | Description |
|-----------|-------------|
| `get` | Download file |
| `put` | Upload file (simple) |
| `list` | List files |
| `create_multipart` | Start multipart upload |
| `put_part` | Upload multipart part |
| `complete_multipart` | Complete multipart upload |

#### Status Labels

| Status | Description |
|--------|-------------|
| `success` | Operation succeeded |
| `error` | Operation failed |

### Checkpoint Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `blizzard_checkpoint_age_seconds` | Gauge | Time since last checkpoint |

## Example Prometheus Queries

### Throughput

```
# Records per second
rate(blizzard_records_processed_total[5m])

# Bytes written per second
rate(blizzard_bytes_written_total[5m])

# Files processed per minute
increase(blizzard_files_processed_total[1m])
```

### Latency

```
# P99 download latency
histogram_quantile(0.99, rate(blizzard_file_download_duration_seconds_bucket[5m]))

# P95 commit latency
histogram_quantile(0.95, rate(blizzard_delta_commit_duration_seconds_bucket[5m]))

# Average decompression time
rate(blizzard_file_decompression_duration_seconds_sum[5m])
  / rate(blizzard_file_decompression_duration_seconds_count[5m])
```

### Error Rate

```
# Failure rate by stage
rate(blizzard_files_failed_total[5m])

# Success rate
rate(blizzard_files_processed_total{status="success"}[5m])
  / rate(blizzard_files_processed_total[5m])

# Parse failures per minute
increase(blizzard_files_failed_total{stage="parse"}[1m])
```

### Concurrency & Backpressure

```
# Download concurrency
blizzard_active_downloads

# Upload concurrency
blizzard_active_uploads

# Backpressure indicator (queue building up)
blizzard_decompression_queue_depth > 10
```

### Storage Operations

```
# Storage error rate
rate(blizzard_storage_requests_total{status="error"}[5m])
  / rate(blizzard_storage_requests_total[5m])

# Multipart uploads per minute
rate(blizzard_multipart_uploads_total[1m])

# P95 storage latency by operation
histogram_quantile(0.95,
  rate(blizzard_storage_request_duration_seconds_bucket[5m])
) by (operation)
```

## Grafana Dashboard Panels

### Suggested Panels

| Panel | Query | Type |
|-------|-------|------|
| Records/sec | `rate(blizzard_records_processed_total[5m])` | Graph |
| Bytes Written/sec | `rate(blizzard_bytes_written_total[5m])` | Graph |
| Active Downloads | `blizzard_active_downloads` | Gauge |
| Active Uploads | `blizzard_active_uploads` | Gauge |
| File Success Rate | Success rate query above | Stat |
| Download Latency P99 | P99 latency query above | Graph |
| Failures by Stage | `blizzard_files_failed_total` | Bar |
| Queue Depth | `blizzard_decompression_queue_depth` | Graph |

### Alert Rules

```yaml
# High failure rate
- alert: BlizzardHighFailureRate
  expr: |
    rate(blizzard_files_failed_total[5m])
    / rate(blizzard_files_processed_total[5m]) > 0.1
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Blizzard failure rate above 10%"

# Pipeline stalled
- alert: BlizzardPipelineStalled
  expr: rate(blizzard_records_processed_total[10m]) == 0
  for: 10m
  labels:
    severity: critical
  annotations:
    summary: "Blizzard pipeline has stopped processing"

# High checkpoint age
- alert: BlizzardCheckpointStale
  expr: blizzard_checkpoint_age_seconds > 300
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Blizzard checkpoint older than 5 minutes"
```

## Internal Events

Metrics are emitted via the `InternalEvent` trait:

```rust
pub trait InternalEvent {
    fn emit(self);
}

// Example usage
emit!(RecordsProcessed { count: batch.num_rows() as u64 });
emit!(FileFailed { stage: FailureStage::Parse });
```

The `emit!` macro calls the `emit()` method which increments/records metrics.

## Code References

| Component | File |
|-----------|------|
| Metrics events | `src/metrics/events.rs` |
| Metrics server | `src/metrics/mod.rs` |
| InternalEvent trait | `src/metrics/events.rs:12` |
| emit! macro | `src/metrics/mod.rs` |
