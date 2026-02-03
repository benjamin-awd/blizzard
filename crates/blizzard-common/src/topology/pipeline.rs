//! Pipeline orchestration primitives.
//!
//! This module provides shared abstractions for running multiple pipelines
//! concurrently with graceful shutdown handling and jittered starts.

use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use snafu::ResultExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::StoragePool;
use crate::config::GlobalConfig;
use crate::error::{AddressParseSnafu, MetricsSnafu, PipelineSetupError};
use crate::resource::StoragePoolRef;
use crate::signal::shutdown_signal;

/// Shared resources for pipeline execution.
#[derive(Clone)]
pub struct PipelineContext {
    /// Optional global semaphore for cross-pipeline concurrency limiting.
    pub global_semaphore: Option<Arc<Semaphore>>,
    /// Optional shared storage pool for connection reuse.
    pub storage_pool: Option<StoragePoolRef>,
    /// Maximum jitter in seconds to add to poll intervals.
    pub poll_jitter_secs: u64,
    /// Cancellation token for graceful shutdown.
    pub shutdown: CancellationToken,
}

impl PipelineContext {
    /// Create a new pipeline context.
    pub fn new(
        total_concurrency: Option<usize>,
        connection_pooling: bool,
        poll_jitter_secs: u64,
        shutdown: CancellationToken,
    ) -> Self {
        let global_semaphore = total_concurrency.map(|n| Arc::new(Semaphore::new(n)));
        let storage_pool = connection_pooling.then(|| Arc::new(StoragePool::new()));

        Self {
            global_semaphore,
            storage_pool,
            poll_jitter_secs,
            shutdown,
        }
    }
}

/// A self-contained pipeline unit that can be executed.
///
/// Implement this trait for your specific pipeline type. The runner will
/// handle spawning, jittered starts, and result collection.
pub trait Pipeline: Send + 'static {
    /// The key type used to identify this pipeline.
    type Key: Clone + Display + Send + 'static;

    /// The error type returned by this pipeline.
    type Error: std::error::Error + Send + 'static;

    /// Get a reference to the pipeline's key.
    fn key(&self) -> &Self::Key;

    /// Run this pipeline to completion.
    fn run(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Orchestrates multiple pipeline executions with shared shutdown handling.
pub struct PipelineRunner<P: Pipeline> {
    pipelines: Vec<P>,
    shutdown: CancellationToken,
    poll_jitter_secs: u64,
    typetag: &'static str,
}

impl<P: Pipeline> PipelineRunner<P> {
    /// Create a new pipeline runner.
    pub fn new(
        pipelines: Vec<P>,
        shutdown: CancellationToken,
        poll_jitter_secs: u64,
        typetag: &'static str,
    ) -> Self {
        Self {
            pipelines,
            shutdown,
            poll_jitter_secs,
            typetag,
        }
    }

    /// Spawn the shutdown signal handler.
    pub fn spawn_shutdown_handler(&self) {
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            shutdown.cancel();
        });
    }

    /// Run all pipelines to completion.
    #[allow(clippy::type_complexity)]
    pub async fn run(self) {
        let mut handles: JoinSet<(P::Key, Result<(), P::Error>)> = JoinSet::new();
        let typetag = self.typetag;

        for pipeline in self.pipelines {
            let shutdown = self.shutdown.clone();
            let key = pipeline.key().clone();
            let start_jitter = random_jitter(self.poll_jitter_secs);

            handles.spawn(async move {
                // Stagger start times, but respect shutdown signal
                if !start_jitter.is_zero() {
                    info!(
                        target = %key,
                        jitter_secs = start_jitter.as_secs(),
                        "Delaying {} start for jitter", typetag
                    );
                    if shutdown
                        .run_until_cancelled(tokio::time::sleep(start_jitter))
                        .await
                        .is_none()
                    {
                        info!(target = %key, "Shutdown requested during jitter delay");
                        return (key, Ok(()));
                    }
                }

                let result = pipeline.run().await;
                (key, result)
            });
        }

        info!("Spawned {} {} tasks", handles.len(), typetag);

        // Wait for all pipelines to complete
        while let Some(result) = handles.join_next().await {
            match result {
                Ok((key, Ok(()))) => {
                    info!(target = %key, "{} completed", typetag);
                }
                Ok((key, Err(e))) => {
                    error!(target = %key, error = %e, "{} failed", typetag);
                }
                Err(e) => {
                    error!(error = %e, "{} task panicked", typetag);
                }
            }
        }

        info!("All {}s complete", typetag);
    }
}

/// Run pipelines with shared setup logic.
///
/// This helper handles common pipeline orchestration:
/// 1. Parse and initialize metrics endpoint
/// 2. Create shutdown token and pipeline context
/// 3. Create pipelines via the provided closure
/// 4. Run all pipelines with graceful shutdown handling
///
/// # Arguments
///
/// * `metrics_address` - The address to bind the metrics server to (e.g., "0.0.0.0:9090")
/// * `global` - Global configuration with concurrency and pooling settings
/// * `typetag` - A label for logging (e.g., "pipeline" or "table")
/// * `create_pipelines` - A closure that creates pipelines given the pipeline context
pub async fn run_pipelines<P, F>(
    metrics_address: &str,
    global: &GlobalConfig,
    typetag: &'static str,
    create_pipelines: F,
) -> Result<(), PipelineSetupError>
where
    P: Pipeline,
    F: FnOnce(PipelineContext) -> Vec<P>,
{
    // Initialize metrics
    let addr = metrics_address.parse().context(AddressParseSnafu)?;
    crate::init_metrics(addr).context(MetricsSnafu)?;

    // Create shared context
    let shutdown = CancellationToken::new();
    let context = PipelineContext::new(
        global.total_concurrency,
        global.connection_pooling,
        global.poll_jitter_secs,
        shutdown.clone(),
    );

    // Create pipelines
    let pipelines = create_pipelines(context);

    // Run pipelines
    let runner = PipelineRunner::new(pipelines, shutdown, global.poll_jitter_secs, typetag);
    runner.spawn_shutdown_handler();
    runner.run().await;

    Ok(())
}

/// Generate a random jitter duration up to the specified maximum seconds.
pub fn random_jitter(max_secs: u64) -> Duration {
    if max_secs > 0 {
        Duration::from_millis(rand::rng().random_range(0..max_secs * 1000))
    } else {
        Duration::ZERO
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_jitter_zero() {
        let jitter = random_jitter(0);
        assert_eq!(jitter, Duration::ZERO);
    }

    #[test]
    fn test_random_jitter_within_bounds() {
        for _ in 0..100 {
            let jitter = random_jitter(10);
            assert!(jitter <= Duration::from_secs(10));
        }
    }

    #[test]
    fn test_pipeline_context_with_concurrency() {
        let ctx = PipelineContext::new(Some(5), false, 10, CancellationToken::new());
        assert!(ctx.global_semaphore.is_some());
        assert!(ctx.storage_pool.is_none());
        assert_eq!(ctx.poll_jitter_secs, 10);
    }

    #[test]
    fn test_pipeline_context_with_pooling() {
        let ctx = PipelineContext::new(None, true, 0, CancellationToken::new());
        assert!(ctx.global_semaphore.is_none());
        assert!(ctx.storage_pool.is_some());
    }

    #[test]
    fn test_pipeline_context_clone() {
        let ctx = PipelineContext::new(Some(5), true, 10, CancellationToken::new());
        let ctx2 = ctx.clone();

        // Cloned context should share the same semaphore and pool
        assert!(Arc::ptr_eq(
            ctx.global_semaphore.as_ref().unwrap(),
            ctx2.global_semaphore.as_ref().unwrap()
        ));
        assert!(Arc::ptr_eq(
            ctx.storage_pool.as_ref().unwrap(),
            ctx2.storage_pool.as_ref().unwrap()
        ));
    }
}
