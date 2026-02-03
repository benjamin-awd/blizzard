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
use tracing::info;

use blizzard_common::polling::run_polling_loop;
use blizzard_common::{
    Pipeline, PipelineContext, StoragePoolRef, StorageProvider, StorageProviderRef,
    random_jitter,
};

use crate::config::{Config, PipelineConfig, PipelineKey};
use crate::error::{PipelineError, StorageSnafu};

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
    /// Create pipelines from configuration.
    pub fn from_config(config: &Config, context: PipelineContext) -> Vec<Self> {
        config
            .pipelines
            .iter()
            .map(|(key, cfg)| Self {
                key: key.clone(),
                config: cfg.clone(),
                context: context.clone(),
            })
            .collect()
    }

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
