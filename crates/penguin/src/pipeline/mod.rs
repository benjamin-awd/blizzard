//! Pipeline for watching staging and committing to Delta Lake.
//!
//! This module implements the main processing loop using the PollingProcessor
//! trait from blizzard-common. It supports running multiple Delta tables
//! concurrently with shared shutdown handling and optional global concurrency limits.
//!

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use indexmap::IndexMap;
use snafu::{OptionExt, ResultExt};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use blizzard_common::polling::{IterationResult, PollingProcessor, run_polling_loop};
use blizzard_common::{FinishedFile, StorageProvider, shutdown_signal};

use crate::emit;
use crate::metrics::events::{DeltaTableVersion, FilesCommitted, PendingFiles, RecordsCommitted};

use crate::checkpoint::CheckpointCoordinator;
use crate::config::{Config, TableConfig, TableKey};
use crate::error::{
    AddressParseSnafu, DeltaSinkNotInitializedSnafu, MetricsSnafu, PipelineError, StorageSnafu,
};
use crate::schema::evolution::EvolutionAction;
use crate::schema::infer_schema_from_first_file;
use crate::sink::DeltaSink;
use crate::staging::StagingReader;

/// Statistics from a single table's pipeline run.
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    /// Total files committed.
    pub files_committed: usize,
    /// Total records committed.
    pub records_committed: usize,
}

/// Statistics from a multi-table pipeline run.
///
/// Tracks per-table statistics and any errors that occurred during processing.
#[derive(Debug, Clone, Default)]
pub struct MultiTableStats {
    /// Per-table statistics for tables that ran successfully.
    pub tables: IndexMap<TableKey, PipelineStats>,
    /// Errors that occurred, with the associated table key and error message.
    pub errors: Vec<(TableKey, String)>,
}

impl MultiTableStats {
    /// Returns true if no errors occurred.
    pub fn success(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns the total number of files committed across all tables.
    pub fn total_files_committed(&self) -> usize {
        self.tables.values().map(|s| s.files_committed).sum()
    }

    /// Returns the total number of records committed across all tables.
    pub fn total_records_committed(&self) -> usize {
        self.tables.values().map(|s| s.records_committed).sum()
    }
}

/// Run the pipeline with the given configuration.
///
/// Spawns independent tasks for each configured table, with shared shutdown
/// handling and optional global concurrency limits.
///
pub async fn run_pipeline(config: Config) -> Result<MultiTableStats, PipelineError> {
    // 1. Initialize metrics once (shared across all tables)
    if config.metrics.enabled {
        let addr = config.metrics.address.parse().context(AddressParseSnafu)?;
        blizzard_common::init_metrics(addr).context(MetricsSnafu)?;
        info!("Metrics server started on {}", config.metrics.address);
    }

    // 2. Set up shared shutdown handling
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_clone.cancel();
    });

    // 3. Create optional global semaphore for cross-table concurrency limiting
    let global_semaphore = config
        .global
        .total_concurrency
        .map(|n| Arc::new(Semaphore::new(n)));

    // 4. Spawn independent task per table
    let mut handles: JoinSet<(TableKey, Result<PipelineStats, PipelineError>)> = JoinSet::new();

    for (table_key, table_config) in config.tables {
        let shutdown = shutdown.clone();
        let global_sem = global_semaphore.clone();
        let poll_interval = Duration::from_secs(table_config.poll_interval_secs);
        let key = table_key.clone();

        handles.spawn(async move {
            let result = run_single_table(
                key.clone(),
                table_config,
                global_sem,
                poll_interval,
                shutdown,
            )
            .await;
            (key, result)
        });
    }

    info!("Spawned {} table tasks", handles.len());

    // 5. Collect results
    let mut stats = MultiTableStats::default();
    while let Some(result) = handles.join_next().await {
        match result {
            Ok((key, Ok(table_stats))) => {
                info!(table = %key, files = table_stats.files_committed, "Table completed successfully");
                stats.tables.insert(key, table_stats);
            }
            Ok((key, Err(e))) => {
                error!(table = %key, error = %e, "Table failed");
                stats.errors.push((key, e.to_string()));
            }
            Err(e) => {
                error!(error = %e, "Table task panicked");
                // JoinError doesn't give us the table key, so we can't attribute it
            }
        }
    }

    info!(
        "Pipeline complete: {} tables succeeded, {} failed",
        stats.tables.len(),
        stats.errors.len()
    );

    Ok(stats)
}

/// Run a single table's polling loop.
///
/// This is spawned as an independent task for each table.
async fn run_single_table(
    table_key: TableKey,
    table_config: TableConfig,
    global_semaphore: Option<Arc<Semaphore>>,
    poll_interval: Duration,
    shutdown: CancellationToken,
) -> Result<PipelineStats, PipelineError> {
    // Initialize processor, respecting shutdown signal
    let mut processor = tokio::select! {
        biased;

        _ = shutdown.cancelled() => {
            info!(table = %table_key, "Shutdown requested during initialization");
            return Ok(PipelineStats::default());
        }

        result = PenguinProcessor::new(table_key.clone(), table_config, global_semaphore) => result?,
    };

    info!(table = %table_key, "Table processor initialized");

    // Run the polling loop
    run_polling_loop(&mut processor, poll_interval, shutdown).await?;

    Ok(processor.stats)
}

/// State prepared for a single processing iteration.
struct PreparedState {
    /// Pending files to process.
    pending_files: Vec<FinishedFile>,
}

/// The penguin delta checkpointer pipeline processor.
///
/// Each table runs its own `PenguinProcessor` instance, with optional
/// sharing of a global semaphore for cross-table concurrency control.
struct PenguinProcessor {
    /// Identifier for this table (used in logging and metrics).
    table_key: TableKey,
    /// Configuration for this specific table.
    table_config: TableConfig,
    /// Reader for staging metadata files.
    staging_reader: StagingReader,
    /// Storage provider for the Delta table.
    sink_storage: StorageProvider,
    /// Delta Lake sink (lazily initialized on first file).
    delta_sink: Option<DeltaSink>,
    /// Coordinator for checkpoint management.
    checkpoint_coordinator: CheckpointCoordinator,
    /// Statistics for this table's run.
    stats: PipelineStats,
    /// Optional global semaphore for cross-table concurrency limiting.
    /// Currently stored for future use in concurrent upload limiting.
    #[allow(dead_code)]
    global_semaphore: Option<Arc<Semaphore>>,
}

impl PenguinProcessor {
    async fn new(
        table_key: TableKey,
        table_config: TableConfig,
        global_semaphore: Option<Arc<Semaphore>>,
    ) -> Result<Self, PipelineError> {
        // Create staging reader (reads from {table_uri}/_staging/)
        let staging_reader = StagingReader::new(
            &table_config.table_uri,
            table_config.storage_options.clone(),
            table_key.id().to_string(),
        )
        .await
        .context(StorageSnafu)?;

        // Create sink storage provider (same URI, parquet files already there)
        let sink_storage = StorageProvider::for_url_with_options(
            &table_config.table_uri,
            table_config.storage_options.clone(),
        )
        .await
        .context(StorageSnafu)?;

        // Try to open existing Delta table without creating it
        let delta_sink = match DeltaSink::try_open(
            &sink_storage,
            table_config.partition_by.clone(),
            table_key.id().to_string(),
        )
        .await
        {
            Ok(sink) => {
                info!(table = %table_key, "Opened existing Delta table");
                Some(sink)
            }
            Err(e) if e.is_table_not_found() => {
                info!(table = %table_key, "No existing Delta table found, will create on first file");
                None
            }
            Err(e) => return Err(e.into()),
        };

        let table_id = table_key.id().to_string();
        Ok(Self {
            table_key,
            table_config,
            staging_reader,
            sink_storage,
            delta_sink,
            checkpoint_coordinator: CheckpointCoordinator::new(table_id),
            stats: PipelineStats::default(),
            global_semaphore,
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
            info!(table = %self.table_key, "Creating new Delta table with inferred schema");
            info!(
                table = %self.table_key,
                fields = incoming_schema.fields().len(),
                field_names = ?incoming_schema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>(),
                "Inferred schema"
            );

            // Create the Delta sink with the inferred schema
            let sink = DeltaSink::new(
                &self.sink_storage,
                &incoming_schema,
                self.table_config.partition_by.clone(),
                self.table_key.id().to_string(),
            )
            .await?;

            self.delta_sink = Some(sink);
            return Ok(());
        }

        // Table exists - validate schema evolution
        let sink = self
            .delta_sink
            .as_mut()
            .context(DeltaSinkNotInitializedSnafu)?;
        let evolution_mode = self.table_config.schema_evolution;

        let action = sink.validate_schema(&incoming_schema, evolution_mode)?;

        match &action {
            EvolutionAction::None => {
                // Schema is compatible, no changes needed
            }
            EvolutionAction::Merge { new_schema } => {
                let existing_field_count = sink.schema().map_or(0, |s| s.fields().len());
                let new_field_names: Vec<_> = new_schema
                    .fields()
                    .iter()
                    .skip(existing_field_count)
                    .map(|f| f.name().as_str())
                    .collect();
                info!(
                    table = %self.table_key,
                    new_fields = new_field_names.len(),
                    field_names = ?new_field_names,
                    "Schema evolution: adding new fields"
                );
            }
            EvolutionAction::Overwrite { new_schema } => {
                info!(
                    table = %self.table_key,
                    fields = new_schema.fields().len(),
                    "Schema evolution: overwriting schema"
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

        info!(table = %self.table_key, files = pending_files.len(), "Found files to commit");

        // Ensure delta sink exists (creates table with inferred schema if needed)
        self.ensure_delta_sink(&pending_files).await?;

        // On cold start, recover checkpoint from Delta log
        // This must happen AFTER ensure_delta_sink so we have a table to read from
        if cold_start {
            info!(table = %self.table_key, "Cold start - recovering checkpoint from Delta log");
            let delta_sink = self
                .delta_sink
                .as_mut()
                .context(DeltaSinkNotInitializedSnafu)?;
            match self
                .checkpoint_coordinator
                .restore_from_delta_log(delta_sink)
                .await
            {
                Ok(true) => info!(table = %self.table_key, "Recovered checkpoint from Delta log"),
                Ok(false) => info!(table = %self.table_key, "No checkpoint found, starting fresh"),
                Err(e) => {
                    tracing::warn!(table = %self.table_key, error = %e, "Failed to recover checkpoint, starting fresh");
                }
            }
        }

        Ok(Some(PreparedState { pending_files }))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let PreparedState { pending_files } = state;

        // Emit pending files metric
        emit!(PendingFiles {
            table: self.table_key.id().to_string(),
            count: pending_files.len(),
        });

        let delta_sink = self
            .delta_sink
            .as_mut()
            .context(DeltaSinkNotInitializedSnafu)?;

        // Commit files to Delta Lake (parquet files are already in table directory)
        let committed_count = self
            .checkpoint_coordinator
            .commit_files(
                delta_sink,
                &pending_files,
                self.table_config.delta_checkpoint_interval,
            )
            .await;

        if committed_count > 0 {
            // Mark files as committed by deleting .meta.json files
            for file in &pending_files {
                if let Err(e) = self.staging_reader.mark_committed(file).await {
                    tracing::warn!(table = %self.table_key, error = %e, "Failed to mark file as committed");
                    continue;
                }

                // Update stats only on successful commit
                self.stats.files_committed += 1;
                self.stats.records_committed += file.record_count;

                // Update source state in checkpoint coordinator
                if let Some(source_file) = &file.source_file {
                    self.checkpoint_coordinator
                        .update_source_state(source_file, file.record_count, true)
                        .await;
                }
            }

            // Emit metrics for committed files
            emit!(FilesCommitted {
                table: self.table_key.id().to_string(),
                count: committed_count as u64,
            });
            emit!(RecordsCommitted {
                table: self.table_key.id().to_string(),
                count: self.stats.records_committed as u64,
            });
            emit!(DeltaTableVersion {
                table: self.table_key.id().to_string(),
                version: delta_sink.version(),
            });

            info!(
                table = %self.table_key,
                files = committed_count,
                version = delta_sink.version(),
                "Committed files to Delta Lake"
            );
        }

        // Reset pending files gauge after processing
        emit!(PendingFiles {
            table: self.table_key.id().to_string(),
            count: 0,
        });

        Ok(IterationResult::ProcessedItems)
    }
}
