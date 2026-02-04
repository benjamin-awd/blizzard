---
title: Topology
description: How Blizzard and Penguin orchestrate multiple concurrent pipelines with shared resources and graceful shutdown
---

The topology layer provides shared orchestration primitives for running multiple pipelines concurrently. It handles resource sharing, jittered starts, and coordinated graceful shutdown across all components.

Both Blizzard and Penguin use this topology framework to manage their multi-pipeline and multi-table operations.

## Architecture Overview

A single configuration spawns one pipeline per table. All pipelines run together and share resources automatically:

```d2
direction: down

config: config.yaml {
  tables: |yaml
    tables:
      - users
      - orders
      - products
  |
}

PipelineRunner: {
  Shared Resources: {
    semaphore: Semaphore {
      label: "Semaphore\n(optional)"
    }
    storage: StoragePool {
      label: "StoragePool\n(optional)"
    }
    token: CancellationToken {
      label: "CancellationToken\n(shutdown)"
    }
  }

  shared: "shared across all pipelines" {
    style.stroke-dash: 3
  }

  pipeline_a: Pipeline (users)
  pipeline_b: Pipeline (orders)
  pipeline_c: Pipeline (products)

  Shared Resources -> shared
  shared -> pipeline_a
  shared -> pipeline_b
  shared -> pipeline_c
}

config -> PipelineRunner: "spawns one pipeline\nper table" {
  style.stroke-dash: 3
}
```

This approach lets you:
- Run multiple tables from a single config
- Share connection pools to reduce overhead
- Apply a global concurrency limit across all pipelines
- Shut down all pipelines gracefully with one signal

## Jittered Starts

Pipelines start with random delays to avoid thundering herd problems. Each pipeline waits a random duration (0 to `poll_jitter_secs`) before starting, spreading load on source storage and smoothing resource usage spikes:

```d2
shape: sequence_diagram

users: Pipeline (users)
orders: Pipeline (orders)
products: Pipeline (products)
Storage

users -> Storage: "t=0s: fetch"
orders -> Storage: "t=3s: fetch"
products -> Storage: "t=7s: fetch"

Storage: "Load spread across 7s" {
  style.stroke-dash: 3
}

users -> Storage: "t=60s: fetch"
orders -> Storage: "t=63s: fetch"
products -> Storage: "t=67s: fetch"
```

Configuration:

```yaml
global:
  poll_jitter_secs: 10  # Max jitter in seconds (0 to disable)
```

## Concurrency Control

A global concurrency limit restricts the total concurrent operations across all pipelines:

```yaml
global:
  total_concurrency: 32  # Max concurrent operations (optional)
```

Without a global limit, each pipeline operates independently with its own concurrency settings.

**When to use global concurrency:**

| Scenario | Recommendation |
|----------|----------------|
| Few pipelines with independent resources | Skip global limit, use per-pipeline settings |
| Many pipelines hitting same storage | Set global limit to prevent throttling |
| Memory-constrained environment | Set limit based on available memory |
| Mixed workloads (some heavy, some light) | Use global limit as a safety cap |

## Connection Pooling

Enable connection pooling to reuse storage connections across pipelines:

```yaml
global:
  connection_pooling: true
```

This allows pipelines to reuse authenticated connections to S3, GCS, or Azure, reducing connection overhead and authentication latency.

**When NOT to use pooling:**

- Pipelines require different authentication credentials
- Source and destination use incompatible client configurations
- You need to isolate connection failures between pipelines
