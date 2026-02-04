---
title: Scaling
description: How to tune Blizzard's concurrency model for different workload sizes
---

Blizzard uses a concurrent pipeline architecture designed to maximize throughput while maintaining backpressure. This guide covers how to tune concurrency settings for different workload sizes and identify bottlenecks.

## Vertical vs Horizontal Scaling

Blizzard supports both vertical scaling (more resources per instance) and horizontal scaling (more instances).

### Vertical Scaling

Increase throughput on a single instance by tuning concurrency settings:

```d2
direction: right

instance: Single Instance {
  cpu: "More CPU cores\n→ higher batch_size"
  memory: "More memory\n→ higher max_concurrent_files"
  network: "More bandwidth\n→ higher max_concurrent_uploads"
}
```

**When to use:**
- Fewer than 10-20 pipelines
- Simple deployment model preferred
- Storage rate limits aren't the bottleneck

**How to scale vertically:**
1. Increase `max_concurrent_files` to utilize more network I/O
2. Increase `max_concurrent_uploads` and `max_concurrent_parts` for upload throughput
3. Increase `batch_size` to better utilize CPU cores
4. Use `total_concurrency` as a safety cap to prevent resource exhaustion

### Horizontal Scaling

Run multiple Blizzard instances, each handling a subset of pipelines:

```d2
direction: down

config: Pipeline Configs {
  a: "users.yaml\norders.yaml"
  b: "products.yaml\nevents.yaml"
  c: "logs.yaml\nmetrics.yaml"
}

instances: {
  i1: Instance 1 {
    label: "Instance 1\n(users, orders)"
  }
  i2: Instance 2 {
    label: "Instance 2\n(products, events)"
  }
  i3: Instance 3 {
    label: "Instance 3\n(logs, metrics)"
  }
}

storage: Shared Storage { shape: cylinder }

config.a -> instances.i1
config.b -> instances.i2
config.c -> instances.i3

instances.i1 -> storage
instances.i2 -> storage
instances.i3 -> storage
```

**When to use:**
- Many pipelines (20+)
- Need fault isolation between pipeline groups
- Approaching storage rate limits on a single instance
- Want to scale beyond a single machine's resources

**How to scale horizontally:**
1. Partition pipelines across instances by workload characteristics
2. Group related pipelines on the same instance for connection pooling benefits
3. Use separate instances for pipelines with different SLAs
4. Each instance manages its own state tracking independently

### Scaling Strategy Comparison

| Factor | Vertical | Horizontal |
|--------|----------|------------|
| Deployment complexity | Lower | Higher |
| Fault isolation | Shared fate | Independent |
| Resource efficiency | Higher (shared pools) | Lower (duplicate overhead) |
| Maximum throughput | Single machine limit | Linear with instances |
| State management | Single tracker | Multiple trackers |

### Hybrid Approach

For large deployments, combine both strategies:

1. Group pipelines by source/destination to maximize connection pooling
2. Scale each group vertically until hitting limits
3. Add instances when vertical scaling plateaus

## Pipeline-Level Scaling

### Download Concurrency

Controls how many source files are downloaded in parallel:

```yaml
source:
  max_concurrent_files: 4  # Default: 4
```

| Setting | Use Case |
|---------|----------|
| 1-2 | Rate-limited sources, low bandwidth |
| 4-8 | Standard workloads (default) |
| 16-32 | High-bandwidth networks, many small files |

Higher values increase memory usage since downloaded files are buffered before processing.

### Processing Throughput

Batch size controls how many records are grouped into each Arrow batch:

```yaml
source:
  batch_size: 8192  # Default: 8192
```

| Setting | Trade-off |
|---------|-----------|
| Smaller (1024-4096) | Lower latency, higher per-record overhead |
| Larger (8192-32768) | Better throughput, higher memory per batch |

### Upload Concurrency

Two levels of parallelism for uploads:

```yaml
sink:
  max_concurrent_uploads: 4  # Parallel file uploads (default: 4)
  max_concurrent_parts: 8    # Parts per multipart upload (default: 8)
```

Total concurrent upload operations = `max_concurrent_uploads × max_concurrent_parts`

| Scenario | Recommended Settings |
|----------|---------------------|
| Small files (<32 MB) | Higher `max_concurrent_uploads`, lower `max_concurrent_parts` |
| Large files (>128 MB) | Lower `max_concurrent_uploads`, higher `max_concurrent_parts` |
| Rate-limited destination | Reduce both to stay under limits |

### Multipart Configuration

For large files, tune multipart upload settings:

```yaml
sink:
  part_size_mb: 32           # Part size (default: 32 MB)
  min_multipart_size_mb: 5   # Threshold for multipart (default: 5 MB)
```

Larger part sizes reduce API calls but require more memory per upload.

## Multi-Pipeline Scaling

When running multiple pipelines, the [topology layer](/blizzard/concepts/topology/) provides additional controls.

### Global Concurrency Limit

Restrict total concurrent operations across all pipelines:

```yaml
global:
  total_concurrency: 32  # Optional cap across all pipelines
```

```d2
direction: down

semaphore: Global Semaphore {
  label: "Global Semaphore\n(32 permits)"
  shape: cylinder
}

pipeline_a: Pipeline A {
  label: "Pipeline A\n(max_concurrent_files: 8)"
}

pipeline_b: Pipeline B {
  label: "Pipeline B\n(max_concurrent_files: 8)"
}

pipeline_c: Pipeline C {
  label: "Pipeline C\n(max_concurrent_files: 8)"
}

semaphore -> pipeline_a: "acquire permit"
semaphore -> pipeline_b: "acquire permit"
semaphore -> pipeline_c: "acquire permit"
```

Without a global limit, three pipelines with `max_concurrent_files: 8` could run 24 concurrent downloads. With `total_concurrency: 32`, the semaphore ensures the combined total stays bounded.

### Connection Pooling

Reuse storage connections across pipelines to reduce overhead:

```yaml
global:
  connection_pooling: true
```

Benefits:
- Fewer TCP connections to storage
- Reduced authentication latency
- Lower memory for connection state

### Jittered Starts

Prevent thundering herd when starting many pipelines:

```yaml
global:
  poll_jitter_secs: 10  # Random delay 0-10s per pipeline
```

## Backpressure

Bounded channels between stages create natural backpressure:

| Channel | Buffer Size | Effect When Full |
|---------|-------------|-----------------|
| Download → Process | `max_concurrent_files` | Downloads pause |
| Process → Upload | `max_concurrent_uploads × 4` | Processing pauses |

When a downstream stage is slow, upstream stages automatically throttle. This prevents memory exhaustion and balances the pipeline.

## Scaling Scenarios

### Low Volume (1-2 pipelines, <1M records/day)

Use defaults or conservative settings:

```yaml
source:
  max_concurrent_files: 4
  batch_size: 8192

sink:
  max_concurrent_uploads: 4
  file_size_mb: 128
```

### Medium Volume (5-10 pipelines, 10M+ records/day)

Increase parallelism and add global controls:

```yaml
global:
  total_concurrency: 32
  connection_pooling: true
  poll_jitter_secs: 10

source:
  max_concurrent_files: 8
  batch_size: 8192

sink:
  max_concurrent_uploads: 8
  max_concurrent_parts: 8
  file_size_mb: 256
```

### High Volume (many pipelines, 100M+ records/day)

Maximize throughput with careful resource management:

```yaml
global:
  total_concurrency: 64
  connection_pooling: true
  poll_jitter_secs: 30

source:
  max_concurrent_files: 16
  batch_size: 16384

sink:
  max_concurrent_uploads: 8
  max_concurrent_parts: 16
  file_size_mb: 512
  part_size_mb: 64
```

## Identifying Bottlenecks

Use metrics to identify which stage limits throughput:

| Symptom | Likely Bottleneck | Tuning |
|---------|------------------|--------|
| High `active_downloads`, low `active_uploads` | Upload stage | Increase `max_concurrent_uploads` |
| Low `active_downloads`, high `active_uploads` | Download stage | Increase `max_concurrent_files` |
| Both low, CPU high | Processing stage | Increase `batch_size`, check compression |
| Memory pressure | Too much parallelism | Reduce concurrency settings |
| Storage throttling (429 errors) | Too aggressive | Reduce concurrency, add global limit |

Key metrics to monitor:

| Metric | Description |
|--------|-------------|
| `blizzard_active_downloads` | Currently downloading files |
| `blizzard_active_uploads` | Currently uploading files |
| `blizzard_active_multipart_parts` | Currently uploading parts |
| `blizzard_records_processed_total` | Throughput indicator |

## Memory Considerations

Memory usage scales with concurrency settings:

| Component | Memory Factor |
|-----------|--------------|
| Downloaded files | `max_concurrent_files × avg_file_size` |
| Record batches | `batch_size × avg_record_size × active_batches` |
| Parquet buffers | `row_group_size_bytes` per open file |
| Upload buffers | `max_concurrent_uploads × file_size_mb` |

For memory-constrained environments, prioritize reducing:
1. `max_concurrent_files` - biggest impact
2. `row_group_size_bytes` - reduces buffering
3. `file_size_mb` - smaller output files
