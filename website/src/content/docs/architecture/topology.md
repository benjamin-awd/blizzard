---
title: Topology
description: How Blizzard orchestrates multiple concurrent pipelines with shared resources and graceful shutdown
---

Blizzard's topology layer provides shared orchestration primitives for running multiple pipelines concurrently. It handles resource sharing, jittered starts, and coordinated graceful shutdown across all components.

Both Blizzard and Penguin use this topology framework to manage their multi-pipeline and multi-table operations.

## Architecture Overview

```d2
PipelineRunner: {
  PipelineContext: {
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

  pipeline_a: Pipeline (key A)
  pipeline_b: Pipeline (key B)
  pipeline_c: Pipeline (key C)

  task_a: Task
  task_b: Task
  task_c: Task

  PipelineContext -> shared
  shared -> pipeline_a
  shared -> pipeline_b
  shared -> pipeline_c

  pipeline_a -> task_a: jittered start
  pipeline_b -> task_b: jittered start
  pipeline_c -> task_c: jittered start
}
```

## Core Components

The topology module provides four main abstractions:

| Component | Purpose |
|-----------|---------|
| `PipelineContext` | Shared resources across all pipelines |
| `Pipeline` | Trait for self-contained pipeline units |
| `PipelineRunner` | Orchestrates multiple pipeline executions |
| `Task` | Wraps async tasks with component identification |
| `RunningTopology` | Trait for managing running pipeline lifecycle |

### PipelineContext

Shared resources available to all pipelines in a topology:

```rust
pub struct PipelineContext {
    pub global_semaphore: Option<Arc<Semaphore>>,
    pub storage_pool: Option<StoragePoolRef>,
    pub poll_jitter_secs: u64,
    pub shutdown: CancellationToken,
}
```

| Field | Purpose |
|-------|---------|
| `global_semaphore` | Cross-pipeline concurrency limiting |
| `storage_pool` | Connection reuse across pipelines |
| `poll_jitter_secs` | Maximum jitter for staggered starts |
| `shutdown` | Shared cancellation token for graceful shutdown |

### Pipeline Trait

Defines the interface for a self-contained pipeline unit:

```rust
pub trait Pipeline: Send + 'static {
    type Key: Clone + Display + Send + 'static;
    type Error: std::error::Error + Send + 'static;

    fn key(&self) -> &Self::Key;
    fn run(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

Pipelines are identified by a key (e.g., table name) for logging and tracking. The runner handles spawning, jittered starts, and result collection.

### PipelineRunner

Orchestrates multiple pipelines with shared shutdown handling:

1. **Spawns pipelines** with jittered start times
2. **Handles shutdown signals** (SIGINT, SIGTERM, SIGQUIT)
3. **Collects results** and logs completion/failure status

```d2
direction: right
spawn: Spawn
jitter: Jitter Delay
run: Run
complete: Complete/Fail
cancel: Cancel

spawn -> jitter -> run -> complete
jitter -> cancel: shutdown signal
complete -> cancel
```

### Task

Wraps async tasks with component identification for better observability:

```rust
pub struct Task<K> {
    pub key: K,
    pub future: BoxFuture<'static>,
    pub typetag: &'static str,
}
```

Tasks can return three outcomes:

| Outcome | Description |
|---------|-------------|
| `TaskOutput` | Success with optional message |
| `TaskError::Cancelled` | Shutdown signal received |
| `TaskError::Panicked` | Task panicked during execution |
| `TaskError::Failed` | Task returned an error |

### RunningTopology Trait

Defines the lifecycle interface for a running topology:

```rust
#[async_trait]
pub trait RunningTopology {
    type Stats;

    async fn run_to_completion(self) -> Self::Stats;
    fn shutdown(&self);
}
```

| Method | Purpose |
|--------|---------|
| `run_to_completion` | Wait for all components to finish |
| `shutdown` | Initiate graceful shutdown |

## Jittered Starts

Pipelines start with random delays to avoid thundering herd:

```d2
direction: right
Pipeline A: {
  delay_a: "delay" {style.fill: "#e0e0e0"}
  run_a: "run" {style.fill: "#ccffcc"}
  delay_a -> run_a
}
Pipeline B: {
  delay_b: "delay" {style.fill: "#e0e0e0"}
  run_b: "run" {style.fill: "#ccffcc"}
  delay_b -> run_b
}
Pipeline C: {
  delay_c: "delay" {style.fill: "#e0e0e0"}
  run_c: "run" {style.fill: "#ccffcc"}
  delay_c -> run_c
}
```

Configuration:

```yaml
global:
  poll_jitter_secs: 10  # Max jitter in seconds (0 to disable)
```

Benefits:
- Spreads load on source storage
- Prevents connection pool exhaustion
- Smooths CPU/memory usage spikes

## Graceful Shutdown

The topology coordinates shutdown across all components:

```d2
direction: down
Shutdown Sequence: {
  step1: "1. Signal received (SIGINT/SIGTERM/SIGQUIT)"
  step2: "2. CancellationToken triggered"
  step3: "3. Pipelines in jitter delay: Exit immediately"
  step4: "4. Running pipelines: Finish current work"
  step5: "5. All tasks collected and logged"
  step6: "6. Runner exits"

  step1 -> step2 -> step3 -> step4 -> step5 -> step6
}
```

Shutdown respects work in progress - pipelines complete their current iteration before stopping.

## Concurrency Control

The global semaphore limits total concurrent operations across all pipelines:

```yaml
global:
  total_concurrency: 32  # Max concurrent operations (optional)
```

Without a global limit, each pipeline operates independently with its own concurrency settings. The global semaphore is useful when:

- Running many pipelines that share storage bandwidth
- Preventing memory exhaustion from too many concurrent downloads
- Controlling cloud API rate limits

## Connection Pooling

Enable connection pooling to reuse storage connections across pipelines:

```yaml
global:
  connection_pooling: true
```

The shared `StoragePool` allows pipelines to reuse authenticated connections to S3, GCS, or Azure, reducing connection overhead and authentication latency.

## Usage Pattern

The `run_pipelines` helper handles common setup:

```rust
run_pipelines(
    "0.0.0.0:9090",           // Metrics address
    &global_config,            // Global settings
    "pipeline",                // Type tag for logging
    |ctx| {
        // Create pipelines using the shared context
        vec![
            MyPipeline::new("table-a", ctx.clone()),
            MyPipeline::new("table-b", ctx.clone()),
        ]
    },
).await?;
```

This handles:
1. Metrics server initialization
2. Shutdown token and context creation
3. Pipeline spawning with jitter
4. Graceful shutdown signal handling
5. Result collection and logging
