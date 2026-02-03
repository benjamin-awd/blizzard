//! Pipeline for processing NDJSON files to Parquet staging.
//!
//! This module implements the main processing loop using the PollingProcessor
//! trait from blizzard-common. It supports running multiple pipelines
//! concurrently with shared shutdown handling and optional global concurrency limits.

mod partition;
mod processor;
mod tasks;
mod tracker;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;
use snafu::ResultExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use blizzard_common::polling::run_polling_loop;
use blizzard_common::{
    StoragePool, StoragePoolRef, StorageProvider, StorageProviderRef, shutdown_signal,
};

use crate::config::{Config, PipelineConfig, PipelineKey};
use crate::error::{AddressParseSnafu, MetricsSnafu, PipelineError, StorageSnafu};

use processor::BlizzardProcessor;

// ============================================================================
// Helpers
// ============================================================================

/// Generate a random jitter duration up to the specified maximum seconds.
fn random_jitter(max_secs: u64) -> Duration {
    if max_secs > 0 {
        Duration::from_millis(rand::rng().random_range(0..max_secs * 1000))
    } else {
        Duration::ZERO
    }
}

/// Create a storage provider, using the pool if available.
pub(crate) async fn create_storage(
    pool: &Option<StoragePoolRef>,
    url: &str,
    options: HashMap<String, String>,
) -> Result<StorageProviderRef, PipelineError> {
    match pool {
        Some(p) => p.get_or_create(url, options).await.context(StorageSnafu),
        None => Ok(Arc::new(
            StorageProvider::for_url_with_options(url, options)
                .await
                .context(StorageSnafu)?,
        )),
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Run the pipeline with the given configuration.
///
/// Spawns independent tasks for each configured pipeline, with shared shutdown
/// handling and optional global concurrency limits.
pub async fn run_pipeline(config: Config) -> Result<(), PipelineError> {
    // Initialize metrics once (shared across all pipelines)
    let addr = config.metrics.address.parse().context(AddressParseSnafu)?;
    blizzard_common::init_metrics(addr).context(MetricsSnafu)?;

    // Set up shared shutdown handling
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_clone.cancel();
    });

    // Create optional global semaphore for cross-pipeline concurrency limiting
    let global_semaphore = config
        .global
        .total_concurrency
        .map(|n| Arc::new(Semaphore::new(n)));

    // Create shared storage pool if connection pooling is enabled
    let storage_pool = config
        .global
        .connection_pooling
        .then(|| Arc::new(StoragePool::new()));

    let jitter_max_secs = config.global.poll_jitter_secs;

    // Spawn independent task per pipeline with jittered start
    let mut handles: JoinSet<(PipelineKey, Result<(), PipelineError>)> = JoinSet::new();

    for (pipeline_key, pipeline_config) in config.pipelines {
        let shutdown = shutdown.clone();
        let global_sem = global_semaphore.clone();
        let pool = storage_pool.clone();
        let poll_interval = Duration::from_secs(pipeline_config.source.poll_interval_secs);
        let key = pipeline_key.clone();

        // Add jitter to stagger pipeline starts (prevents thundering herd)
        let start_jitter = random_jitter(jitter_max_secs);

        handles.spawn(async move {
            // Stagger start times, but respect shutdown signal
            if !start_jitter.is_zero() {
                info!(
                    target = %key,
                    jitter_secs = start_jitter.as_secs(),
                    "Delaying pipeline start for jitter"
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

            let result = run_single_pipeline(
                key.clone(),
                pipeline_config,
                global_sem,
                pool,
                poll_interval,
                jitter_max_secs,
                shutdown,
            )
            .await;
            (key, result)
        });
    }

    info!("Spawned {} pipeline tasks", handles.len());

    // Wait for all pipelines to complete
    while let Some(result) = handles.join_next().await {
        match result {
            Ok((key, Ok(()))) => {
                info!(target = %key, "Pipeline completed");
            }
            Ok((key, Err(e))) => {
                error!(target = %key, error = %e, "Pipeline failed");
            }
            Err(e) => {
                error!(error = %e, "Pipeline task panicked");
            }
        }
    }

    info!("All pipelines complete");

    Ok(())
}

// ============================================================================
// Internal orchestration
// ============================================================================

/// Run a single pipeline's polling loop.
async fn run_single_pipeline(
    pipeline_key: PipelineKey,
    pipeline_config: PipelineConfig,
    _global_semaphore: Option<Arc<Semaphore>>,
    storage_pool: Option<StoragePoolRef>,
    poll_interval: Duration,
    poll_jitter_secs: u64,
    shutdown: CancellationToken,
) -> Result<(), PipelineError> {
    // Add jitter to poll interval for this pipeline
    let effective_interval = poll_interval + random_jitter(poll_jitter_secs);

    // Initialize processor, respecting shutdown signal
    let mut processor = tokio::select! {
        biased;

        _ = shutdown.cancelled() => {
            info!(target = %pipeline_key, "Shutdown requested during initialization");
            return Ok(());
        }

        result = BlizzardProcessor::new(
            pipeline_key.clone(),
            pipeline_config,
            storage_pool,
            shutdown.clone(),
        ) => result?,
    };

    info!(
        target = %pipeline_key,
        poll_interval_secs = effective_interval.as_secs(),
        "Pipeline processor initialized"
    );

    run_polling_loop(
        &mut processor,
        effective_interval,
        shutdown,
        pipeline_key.id(),
    )
    .await?;

    Ok(())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_shutdown_token_is_shared() {
        let shutdown = CancellationToken::new();
        let shutdown_for_processor = shutdown.clone();

        shutdown.cancel();

        assert!(
            shutdown_for_processor.is_cancelled(),
            "Shutdown token clones should share cancellation state"
        );
    }

    #[tokio::test]
    async fn test_cancellation_propagates_through_clones() {
        let original = CancellationToken::new();
        let clone1 = original.clone();
        let clone2 = clone1.clone();
        let clone3 = clone2.clone();

        assert!(!original.is_cancelled());
        assert!(!clone1.is_cancelled());
        assert!(!clone2.is_cancelled());
        assert!(!clone3.is_cancelled());

        original.cancel();

        assert!(clone1.is_cancelled());
        assert!(clone2.is_cancelled());
        assert!(clone3.is_cancelled());
    }

    #[tokio::test]
    async fn test_separate_tokens_do_not_share_cancellation() {
        let token1 = CancellationToken::new();
        let token2 = CancellationToken::new();

        token1.cancel();

        assert!(
            !token2.is_cancelled(),
            "Separate tokens should not share cancellation"
        );
    }

    #[tokio::test]
    async fn test_shutdown_cancellation_is_immediate() {
        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_clone.cancelled() => {
                    "cancelled"
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    "timeout"
                }
            }
        });

        shutdown.cancel();

        let result = tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("task should complete quickly")
            .expect("task should not panic");

        assert_eq!(result, "cancelled");
    }
}
