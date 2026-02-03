//! Pipeline for processing NDJSON files to Parquet staging.
//!
//! This module implements the main processing loop using the PollingProcessor
//! trait from blizzard-core. It supports running multiple pipelines
//! concurrently with shared shutdown handling and optional global concurrency limits.

mod download;
mod processor;
mod tasks;
mod tracker;

use std::sync::Arc;
use std::time::Duration;

use snafu::ResultExt;
use tracing::info;

use blizzard_core::polling::run_polling_loop;
use blizzard_core::{
    PipelineContext, StoragePoolRef, StorageProvider, StorageProviderRef, random_jitter,
};

use crate::config::{Config, Mergeable, PipelineConfig, PipelineKey, StorageSource};
use crate::error::{PipelineError, StorageSnafu};

use processor::Processor;

/// Create a storage provider from a config, using the pool if available.
pub(crate) async fn create_storage(
    pool: &Option<StoragePoolRef>,
    source: &impl StorageSource,
) -> Result<StorageProviderRef, PipelineError> {
    let url = source.url();
    let options = source.storage_options().clone();
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
pub struct Pipeline {
    pub key: PipelineKey,
    pub config: PipelineConfig,
    pub context: PipelineContext,
}

impl Pipeline {
    /// Create pipelines from configuration.
    pub fn from_config(config: &Config, context: PipelineContext) -> Vec<Self> {
        config.build_pipelines(context, |key, config, context| Self {
            key,
            config,
            context,
        })
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

            result = Processor::new(
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

impl blizzard_core::Pipeline for Pipeline {
    type Key = PipelineKey;
    type Error = PipelineError;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    async fn run(self) -> Result<(), Self::Error> {
        self.execute().await
    }
}
