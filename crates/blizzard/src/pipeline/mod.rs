//! Pipeline for processing NDJSON files to Parquet staging.
//!
//! This module implements the main processing loop using the PollingProcessor
//! trait from blizzard-common. It supports running multiple pipelines
//! concurrently with shared shutdown handling and optional global concurrency limits.

mod processor;
mod tasks;
mod tracker;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use snafu::ResultExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use blizzard_common::polling::run_polling_loop;
use blizzard_common::{
    Pipeline, PipelineContext, PipelineRunner, StoragePoolRef, StorageProvider, StorageProviderRef,
    random_jitter,
};

use crate::config::{Config, PipelineConfig, PipelineKey};
use crate::error::{AddressParseSnafu, MetricsSnafu, PipelineError, StorageSnafu};

use processor::BlizzardProcessor;

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

/// A blizzard pipeline unit for processing NDJSON files to Parquet.
pub struct BlizzardPipeline {
    pub key: PipelineKey,
    pub config: PipelineConfig,
    pub context: PipelineContext,
}

impl BlizzardPipeline {
    /// Run this pipeline's polling loop.
    async fn execute(self) -> Result<(), PipelineError> {
        let poll_interval = Duration::from_secs(self.config.source.poll_interval_secs);
        let effective_interval = poll_interval + random_jitter(self.context.poll_jitter_secs);

        // Initialize processor, respecting shutdown signal
        let mut processor = tokio::select! {
            biased;

            _ = self.context.shutdown.cancelled() => {
                info!(target = %self.key, "Shutdown requested during initialization");
                return Ok(());
            }

            result = BlizzardProcessor::new(
                self.key.clone(),
                self.config,
                self.context.storage_pool,
                self.context.shutdown.clone(),
            ) => result?,
        };

        info!(
            target = %self.key,
            poll_interval_secs = effective_interval.as_secs(),
            "Pipeline processor initialized"
        );

        run_polling_loop(
            &mut processor,
            effective_interval,
            self.context.shutdown,
            self.key.id(),
        )
        .await?;

        Ok(())
    }
}

impl Pipeline for BlizzardPipeline {
    type Key = PipelineKey;
    type Error = PipelineError;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    async fn run(self) -> Result<(), Self::Error> {
        self.execute().await
    }
}

/// Create pipelines from configuration.
fn create_pipelines(config: &Config, context: PipelineContext) -> Vec<BlizzardPipeline> {
    config
        .pipelines
        .iter()
        .map(|(key, cfg)| BlizzardPipeline {
            key: key.clone(),
            config: cfg.clone(),
            context: context.clone(),
        })
        .collect()
}

/// Run the pipeline with the given configuration.
///
/// Spawns independent tasks for each configured pipeline, with shared shutdown
/// handling and optional global concurrency limits.
pub async fn run_pipeline(config: Config) -> Result<(), PipelineError> {
    // Initialize metrics once (shared across all pipelines)
    let addr = config.metrics.address.parse().context(AddressParseSnafu)?;
    blizzard_common::init_metrics(addr).context(MetricsSnafu)?;

    // Create shared context and pipelines
    let shutdown = CancellationToken::new();
    let context = PipelineContext::new(
        config.global.total_concurrency,
        config.global.connection_pooling,
        config.global.poll_jitter_secs,
        shutdown.clone(),
    );
    let pipelines = create_pipelines(&config, context);

    // Create and run the pipeline runner
    let runner = PipelineRunner::new(pipelines, shutdown, config.global.poll_jitter_secs, "pipeline");
    runner.spawn_shutdown_handler();
    runner.run().await;

    Ok(())
}

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
