//! Pipeline for watching staging and committing to Delta Lake.
//!
//! This module implements the main processing loop using the PollingProcessor
//! trait from blizzard-common.

use async_trait::async_trait;
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use snafu::ResultExt;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

use blizzard_common::polling::{IterationResult, PollingProcessor, run_polling_loop};
use blizzard_common::{FinishedFile, StorageProvider, shutdown_signal};

use crate::checkpoint::CheckpointCoordinator;
use crate::config::Config;
use crate::error::{AddressParseSnafu, MetricsSnafu, PipelineError, StorageSnafu};
use crate::sink::DeltaSink;
use crate::staging::StagingReader;

/// Statistics from a pipeline run.
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    /// Total files committed.
    pub files_committed: usize,
    /// Total records committed.
    pub records_committed: usize,
}

/// Run the pipeline with the given configuration.
pub async fn run_pipeline(config: Config) -> Result<PipelineStats, PipelineError> {
    // Initialize metrics if enabled
    if config.metrics.enabled {
        let addr = config.metrics.address.parse().context(AddressParseSnafu)?;
        blizzard_common::init_metrics(addr).context(MetricsSnafu)?;
        info!("Metrics server started on {}", config.metrics.address);
    }

    // Set up shutdown handling
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_clone.cancel();
    });

    // Create the pipeline processor, racing against shutdown
    let poll_interval_secs = config.source.poll_interval_secs;
    let mut processor = tokio::select! {
        biased;

        _ = shutdown.cancelled() => {
            info!("Shutdown requested during initialization");
            return Ok(PipelineStats::default());
        }

        result = PenguinProcessor::new(config) => result?,
    };

    // Run the polling loop
    let poll_interval = Duration::from_secs(poll_interval_secs);
    run_polling_loop(&mut processor, poll_interval, shutdown).await?;

    Ok(processor.stats)
}

/// State prepared for a single processing iteration.
struct PreparedState {
    /// Pending files to process.
    pending_files: Vec<FinishedFile>,
}

/// The penguin delta checkpointer pipeline processor.
struct PenguinProcessor {
    config: Config,
    staging_reader: StagingReader,
    delta_sink: DeltaSink,
    checkpoint_coordinator: CheckpointCoordinator,
    stats: PipelineStats,
}

impl PenguinProcessor {
    async fn new(config: Config) -> Result<Self, PipelineError> {
        // Create staging reader (reads from {table_uri}/_staging/)
        let staging_reader = StagingReader::new(
            &config.source.table_uri,
            config.source.storage_options.clone(),
        )
        .await
        .context(StorageSnafu)?;

        // Create sink storage provider (same URI, parquet files already there)
        let sink_storage = StorageProvider::for_url_with_options(
            &config.source.table_uri,
            config.source.storage_options.clone(),
        )
        .await
        .context(StorageSnafu)?;

        // For penguin, we infer the schema from the first parquet file we see.
        // For now, use a placeholder schema - in production this would be loaded
        // from the existing Delta table or the first parquet file.
        let placeholder_schema = Arc::new(Schema::new(vec![Field::new(
            "_placeholder",
            DataType::Utf8,
            true,
        )]));

        // Create Delta sink
        let delta_sink = DeltaSink::new(
            &sink_storage,
            &placeholder_schema,
            config.source.partition_by.clone(),
        )
        .await?;

        Ok(Self {
            config,
            staging_reader,
            delta_sink,
            checkpoint_coordinator: CheckpointCoordinator::new(),
            stats: PipelineStats::default(),
        })
    }
}

#[async_trait]
impl PollingProcessor for PenguinProcessor {
    type State = PreparedState;
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        // On cold start, recover checkpoint from Delta log
        if cold_start {
            info!("Cold start - recovering checkpoint from Delta log");
            match self
                .checkpoint_coordinator
                .restore_from_delta_log(&mut self.delta_sink)
                .await
            {
                Ok(true) => info!("Recovered checkpoint from Delta log"),
                Ok(false) => info!("No checkpoint found, starting fresh"),
                Err(e) => {
                    tracing::warn!("Failed to recover checkpoint: {}, starting fresh", e);
                }
            }
        }

        // Read pending files from staging
        let pending_files = self.staging_reader.read_pending_files().await?;

        if pending_files.is_empty() {
            return Ok(None);
        }

        info!("Found {} files to commit", pending_files.len());

        Ok(Some(PreparedState { pending_files }))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let PreparedState { pending_files } = state;

        // Commit files to Delta Lake (parquet files are already in table directory)
        let committed_count = self
            .checkpoint_coordinator
            .commit_files(
                &mut self.delta_sink,
                &pending_files,
                self.config.source.delta_checkpoint_interval,
            )
            .await;

        if committed_count > 0 {
            // Mark files as committed by deleting .meta.json files
            for file in &pending_files {
                if let Err(e) = self.staging_reader.mark_committed(file).await {
                    tracing::warn!("Failed to mark file as committed: {}", e);
                }

                // Update stats
                self.stats.files_committed += 1;
                self.stats.records_committed += file.record_count;

                // Update source state in checkpoint coordinator
                if let Some(source_file) = &file.source_file {
                    self.checkpoint_coordinator
                        .update_source_state(source_file, file.record_count, true)
                        .await;
                }
            }

            info!(
                "Committed {} files to Delta Lake (version {})",
                committed_count,
                self.delta_sink.version()
            );
        }

        Ok(IterationResult::ProcessedItems)
    }
}
