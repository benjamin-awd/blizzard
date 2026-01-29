//! Pipeline for watching staging and committing to Delta Lake.
//!
//! This module implements the main processing loop using the PollingProcessor
//! trait from blizzard-common.

use async_trait::async_trait;
use snafu::ResultExt;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

use blizzard_common::polling::{IterationResult, PollingProcessor, run_polling_loop};
use blizzard_common::{FinishedFile, StorageProvider, shutdown_signal};

use crate::checkpoint::CheckpointCoordinator;
use crate::config::Config;
use crate::error::{AddressParseSnafu, MetricsSnafu, PipelineError, StorageSnafu};
use crate::schema::evolution::EvolutionAction;
use crate::schema::infer_schema_from_first_file;
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
    sink_storage: StorageProvider,
    delta_sink: Option<DeltaSink>,
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

        // Try to open existing Delta table without creating it
        let delta_sink =
            match DeltaSink::try_open(&sink_storage, config.source.partition_by.clone()).await {
                Ok(sink) => {
                    info!("Opened existing Delta table");
                    Some(sink)
                }
                Err(e) if e.is_table_not_found() => {
                    info!("No existing Delta table found, will create on first file");
                    None
                }
                Err(e) => return Err(e.into()),
            };

        Ok(Self {
            config,
            staging_reader,
            sink_storage,
            delta_sink,
            checkpoint_coordinator: CheckpointCoordinator::new(),
            stats: PipelineStats::default(),
        })
    }

    /// Ensure the delta sink is initialized.
    ///
    /// If no sink exists yet, infers the schema from the first parquet file
    /// and creates the Delta table with the correct schema.
    ///
    /// If a sink exists, validates the incoming schema against the table schema
    /// and applies schema evolution if needed based on the configured mode.
    async fn ensure_delta_sink(
        &mut self,
        pending_files: &[FinishedFile],
    ) -> Result<(), PipelineError> {
        // Infer schema from incoming files
        let incoming_schema =
            infer_schema_from_first_file(&self.sink_storage, pending_files).await?;

        if self.delta_sink.is_none() {
            info!("Creating new Delta table with inferred schema");
            info!(
                "Inferred schema with {} fields: {:?}",
                incoming_schema.fields().len(),
                incoming_schema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            );

            // Create the Delta sink with the inferred schema
            let sink = DeltaSink::new(
                &self.sink_storage,
                &incoming_schema,
                self.config.source.partition_by.clone(),
            )
            .await?;

            self.delta_sink = Some(sink);
            return Ok(());
        }

        // Table exists - validate schema evolution
        let sink = self.delta_sink.as_mut().unwrap();
        let evolution_mode = self.config.source.schema_evolution;

        let action = sink.validate_schema(&incoming_schema, evolution_mode)?;

        match &action {
            EvolutionAction::None => {
                // Schema is compatible, no changes needed
            }
            EvolutionAction::Merge { new_schema } => {
                let new_field_names: Vec<_> = new_schema
                    .fields()
                    .iter()
                    .skip(sink.schema().map_or(0, |s| s.fields().len()))
                    .map(|f| f.name().as_str())
                    .collect();
                info!(
                    "Schema evolution: adding {} new field(s): {:?}",
                    new_field_names.len(),
                    new_field_names
                );
            }
            EvolutionAction::Overwrite { new_schema } => {
                info!(
                    "Schema evolution: overwriting with {} fields",
                    new_schema.fields().len()
                );
            }
        }

        // Apply the evolution action
        sink.evolve_schema(action).await?;

        Ok(())
    }
}

#[async_trait]
impl PollingProcessor for PenguinProcessor {
    type State = PreparedState;
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        // Read pending files from staging first
        let pending_files = self.staging_reader.read_pending_files().await?;

        if pending_files.is_empty() {
            return Ok(None);
        }

        info!("Found {} files to commit", pending_files.len());

        // Ensure delta sink exists (creates table with inferred schema if needed)
        self.ensure_delta_sink(&pending_files).await?;

        // On cold start, recover checkpoint from Delta log
        // This must happen AFTER ensure_delta_sink so we have a table to read from
        if cold_start {
            info!("Cold start - recovering checkpoint from Delta log");
            // delta_sink is guaranteed to be Some after ensure_delta_sink
            let delta_sink = self.delta_sink.as_mut().unwrap();
            match self
                .checkpoint_coordinator
                .restore_from_delta_log(delta_sink)
                .await
            {
                Ok(true) => info!("Recovered checkpoint from Delta log"),
                Ok(false) => info!("No checkpoint found, starting fresh"),
                Err(e) => {
                    tracing::warn!("Failed to recover checkpoint: {}, starting fresh", e);
                }
            }
        }

        Ok(Some(PreparedState { pending_files }))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let PreparedState { pending_files } = state;

        // delta_sink is guaranteed to be Some after prepare() calls ensure_delta_sink
        let delta_sink = self.delta_sink.as_mut().unwrap();

        // Commit files to Delta Lake (parquet files are already in table directory)
        let committed_count = self
            .checkpoint_coordinator
            .commit_files(
                delta_sink,
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
                delta_sink.version()
            );
        }

        Ok(IterationResult::ProcessedItems)
    }
}
