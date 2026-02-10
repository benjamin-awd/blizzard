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

Pipelines start with random delays to avoid thundering herd problems. A random duration (0 to `poll_jitter_secs`) is added both to the initial startup and to every subsequent poll interval, spreading load on source storage and smoothing resource usage spikes:

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

users -> Storage: "t=62s: fetch"
orders -> Storage: "t=65s: fetch"
products -> Storage: "t=68s: fetch"

Storage: "Different jitter each cycle" {
  style.stroke-dash: 3
}
```

Configuration:

```yaml
global:
  poll_jitter_secs: 10  # Max jitter in seconds (default: 30, 0 to disable)
```

## Graceful Shutdown

All pipelines share a single `CancellationToken`. When a shutdown signal is received (SIGINT, SIGTERM, or SIGQUIT), the token is cancelled and every pipeline finishes its current work before exiting:

1. **Signal received** — the shutdown handler cancels the shared token
2. **Pipelines notice** — each pipeline checks the token at its next poll boundary
3. **Drain** — in-flight files finish downloading, processing, and uploading
4. **Exit** — the runner waits for all pipelines to complete, then exits cleanly

Pipelines that haven't started yet (still waiting on their jitter delay) exit immediately without doing any work.

## See Also

- [Scaling](/architecture/scaling/) — tuning concurrency, connection pooling, and jitter for different workload sizes
- [Configuration Reference](/architecture/configuration/) — full field reference for `global` settings
