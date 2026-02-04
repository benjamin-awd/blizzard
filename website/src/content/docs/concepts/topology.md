---
title: Topology
description: How Blizzard and Penguin orchestrate multiple concurrent pipelines with shared resources and graceful shutdown
---

The topology layer provides shared orchestration primitives for running multiple pipelines concurrently. It handles resource sharing, jittered starts, and coordinated graceful shutdown across all components.

Both Blizzard and Penguin use this topology framework to manage their multi-pipeline and multi-table operations.

## Architecture Overview

```d2
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

  pipeline_a: Pipeline A
  pipeline_b: Pipeline B
  pipeline_c: Pipeline C

  Shared Resources -> shared
  shared -> pipeline_a
  shared -> pipeline_b
  shared -> pipeline_c
}
```

## Jittered Starts

Pipelines start with random delays to avoid thundering herd problems. Each pipeline waits a random duration (0 to `poll_jitter_secs`) before starting, spreading load on source storage and smoothing resource usage spikes:

```d2
direction: right
timeline: {
  t0: "t=0s"
  t1: "t=2s"
  t2: "t=7s"
}

Pipeline A: {
  a_start: "start" {style.fill: "#ccffcc"}
}
Pipeline B: {
  b_delay: "waiting" {style.fill: "#e0e0e0"}
  b_start: "start" {style.fill: "#ccffcc"}
  b_delay -> b_start
}
Pipeline C: {
  c_delay: "waiting" {style.fill: "#e0e0e0"}
  c_start: "start" {style.fill: "#ccffcc"}
  c_delay -> c_start
}

timeline.t0 -> Pipeline A.a_start
timeline.t1 -> Pipeline B.b_start
timeline.t2 -> Pipeline C.c_start
```

Configuration:

```yaml
global:
  poll_jitter_secs: 10  # Max jitter in seconds (0 to disable)
```

## Graceful Shutdown

The topology coordinates shutdown across all components:

```d2
direction: down
Shutdown Sequence: {
  step1: "1. Signal received (SIGINT/SIGTERM/SIGQUIT)"
  step2: "2. Cancellation triggered"
  step3: "3. Pipelines in jitter delay: Exit immediately"
  step4: "4. Running pipelines: Finish current work"
  step5: "5. All tasks collected and logged"
  step6: "6. Runner exits"

  step1 -> step2 -> step3 -> step4 -> step5 -> step6
}
```

Shutdown respects work in progress - pipelines complete their current iteration before stopping.

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

## Related Concepts

- [Fault Tolerance](/concepts/fault-tolerance/) - How topology handles failures and retries
- [Storage](/concepts/storage/) - StoragePool and connection reuse details
