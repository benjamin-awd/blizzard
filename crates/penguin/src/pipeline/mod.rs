//! Pipeline for discovering parquet files and committing to Delta Lake.
//!
//! This module implements the main processing loop using the PollingProcessor
//! trait from blizzard-common. It supports running multiple Delta tables
//! concurrently with shared shutdown handling and optional global concurrency limits.
//!

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rand::Rng;
use snafu::{OptionExt, ResultExt};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use blizzard_common::polling::{IterationResult, PollingProcessor, run_polling_loop};
use blizzard_common::{
    FinishedFile, StoragePool, StoragePoolRef, StorageProvider, shutdown_signal,
};

use crate::emit;
use crate::metrics::events::{DeltaTableVersion, FilesCommitted, PendingFiles, RecordsCommitted};

use crate::checkpoint::CheckpointCoordinator;
use crate::config::{Config, TableConfig, TableKey};
use crate::error::{
    AddressParseSnafu, DeltaSinkNotInitializedSnafu, MetricsSnafu, PipelineError, StorageSnafu,
};
use crate::incoming::{IncomingConfig, IncomingReader};
use crate::schema::evolution::EvolutionAction;
use crate::schema::infer_schema_from_first_file;
use crate::sink::DeltaSink;

/// Run the pipeline with the given configuration.
///
/// Spawns independent tasks for each configured table, with shared shutdown
/// handling and optional global concurrency limits.
///
pub async fn run_pipeline(config: Config) -> Result<(), PipelineError> {
    // 1. Initialize metrics once (shared across all tables)
    let addr = config.metrics.address.parse().context(AddressParseSnafu)?;
    blizzard_common::init_metrics(addr).context(MetricsSnafu)?;

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

    // 4. Create shared storage pool if connection pooling is enabled
    let storage_pool = config
        .global
        .connection_pooling
        .then(|| Arc::new(StoragePool::new()));


    // 5. Get jitter settings
    let jitter_max_secs = config.global.poll_jitter_secs;

    // 6. Spawn independent task per table with jittered start
    let mut handles: JoinSet<(TableKey, Result<(), PipelineError>)> = JoinSet::new();

    for (table_key, table_config) in config.tables {
        let shutdown = shutdown.clone();
        let global_sem = global_semaphore.clone();
        let pool = storage_pool.clone();
        let poll_interval = Duration::from_secs(table_config.poll_interval_secs);
        let key = table_key.clone();

        // Add jitter to stagger table starts (prevents thundering herd)
        let start_jitter = if jitter_max_secs > 0 {
            Duration::from_secs(rand::rng().random_range(0..jitter_max_secs))
        } else {
            Duration::ZERO
        };

        handles.spawn(async move {
            // Stagger start times, but respect shutdown signal
            if !start_jitter.is_zero() {
                info!(
                    target = %key,
                    jitter_secs = start_jitter.as_secs(),
                    "Delaying table start for jitter"
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

            let result = run_single_table(
                key.clone(),
                table_config,
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

    info!("Spawned {} table tasks", handles.len());

    // Wait for all tables to complete
    while let Some(result) = handles.join_next().await {
        match result {
            Ok((key, Ok(()))) => {
                info!(target = %key, "Table completed");
            }
            Ok((key, Err(e))) => {
                error!(target = %key, error = %e, "Table failed");
            }
            Err(e) => {
                error!(error = %e, "Table task panicked");
            }
        }
    }

    info!("All tables complete");

    Ok(())
}

/// Run a single table's polling loop.
///
/// This is spawned as an independent task for each table.
async fn run_single_table(
    table_key: TableKey,
    table_config: TableConfig,
    global_semaphore: Option<Arc<Semaphore>>,
    storage_pool: Option<StoragePoolRef>,
    poll_interval: Duration,
    poll_jitter_secs: u64,
    shutdown: CancellationToken,
) -> Result<(), PipelineError> {
    // Add jitter to poll interval for this table to spread polling load
    let effective_interval = if poll_jitter_secs > 0 {
        let jitter_ms = rand::rng().random_range(0..poll_jitter_secs * 1000);
        poll_interval + Duration::from_millis(jitter_ms)
    } else {
        poll_interval
    };

    // Initialize processor, respecting shutdown signal
    let mut processor = tokio::select! {
        biased;

        _ = shutdown.cancelled() => {
            info!(target = %table_key, "Shutdown requested during initialization");
            return Ok(());
        }

        result = PenguinProcessor::new(
            table_key.clone(),
            table_config,
            global_semaphore,
            storage_pool,
        ) => result?,
    };

    info!(
        target = %table_key,
        poll_interval_secs = effective_interval.as_secs(),
        "Table processor initialized"
    );

    // Run the polling loop with jittered interval
    run_polling_loop(&mut processor, effective_interval, shutdown, table_key.id()).await?;

    Ok(())
}

/// State prepared for a single processing iteration.
struct PreparedState {
    /// Files discovered in the table directory to commit.
    files: Vec<FinishedFile>,
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
    /// Reader for discovering uncommitted parquet files.
    file_reader: IncomingReader,
    /// Storage provider for the Delta table.
    sink_storage: Arc<StorageProvider>,
    /// Delta Lake sink (lazily initialized on first file).
    delta_sink: Option<DeltaSink>,
    /// Coordinator for checkpoint management.
    checkpoint_coordinator: CheckpointCoordinator,
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
        storage_pool: Option<StoragePoolRef>,
    ) -> Result<Self, PipelineError> {
        // Create sink storage provider - use pooled if available
        let sink_storage = if let Some(pool) = &storage_pool {
            pool.get_or_create(
                &table_config.table_uri,
                table_config.storage_options.clone(),
            )
            .await
            .context(StorageSnafu)?
        } else {
            Arc::new(
                StorageProvider::for_url_with_options(
                    &table_config.table_uri,
                    table_config.storage_options.clone(),
                )
                .await
                .context(StorageSnafu)?,
            )
        };

        // Create file reader for discovering uncommitted parquet files
        let file_reader = IncomingReader::new(
            sink_storage.clone(),
            table_key.id().to_string(),
            IncomingConfig {
                partition_filter: table_config.partition_filter.clone(),
            },
        );

        // Try to open existing Delta table without creating it
        let partition_columns = table_config
            .partition_by
            .as_ref()
            .map(|p| p.partition_columns())
            .unwrap_or_default();
        let delta_sink = match DeltaSink::try_open(
            &sink_storage,
            partition_columns.clone(),
            table_key.id().to_string(),
        )
        .await
        {
            Ok(sink) => {
                info!(target = %table_key, "Opened existing Delta table");
                Some(sink)
            }
            Err(e) if e.is_table_not_found() => {
                info!(target = %table_key, "No existing Delta table found, will create on first file");
                None
            }
            Err(e) => return Err(e.into()),
        };

        let table_id = table_key.id().to_string();
        Ok(Self {
            table_key,
            table_config,
            file_reader,
            sink_storage,
            delta_sink,
            checkpoint_coordinator: CheckpointCoordinator::new(table_id),
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
    async fn ensure_delta_sink(&mut self, files: &[FinishedFile]) -> Result<(), PipelineError> {
        if files.is_empty() {
            return Ok(());
        }

        // Infer schema from files
        let incoming_schema =
            infer_schema_from_first_file(&self.sink_storage, files, self.table_key.as_ref())
                .await?;

        if self.delta_sink.is_none() {
            info!(target = %self.table_key, "Creating new Delta table with inferred schema");
            info!(
                target = %self.table_key,
                fields = incoming_schema.fields().len(),
                field_names = ?incoming_schema
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>(),
                "Inferred schema"
            );

            // Create the Delta sink with the inferred schema
            let partition_columns = self
                .table_config
                .partition_by
                .as_ref()
                .map(|p| p.partition_columns())
                .unwrap_or_default();
            let sink = DeltaSink::new(
                &self.sink_storage,
                &incoming_schema,
                partition_columns,
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
                    target = %self.table_key,
                    new_fields = new_field_names.len(),
                    field_names = ?new_field_names,
                    "Schema evolution: adding new fields"
                );
            }
            EvolutionAction::Overwrite { new_schema } => {
                info!(
                    target = %self.table_key,
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
        // On cold start, recover checkpoint from Delta log if table exists
        // We do this first to get the watermark before listing files
        if cold_start && let Some(delta_sink) = self.delta_sink.as_mut() {
            info!(target = %self.table_key, "Cold start - recovering checkpoint from Delta log");
            match self
                .checkpoint_coordinator
                .restore_from_delta_log(delta_sink)
                .await
            {
                Ok(true) => info!(target = %self.table_key, "Recovered checkpoint from Delta log"),
                Ok(false) => info!(target = %self.table_key, "No checkpoint found, starting fresh"),
                Err(e) => {
                    tracing::warn!(target = %self.table_key, error = %e, "Failed to recover checkpoint, starting fresh");
                }
            }
        }

        // Get committed paths from Delta log to avoid double-commits
        let committed_paths = if let Some(delta_sink) = &self.delta_sink {
            delta_sink.get_committed_paths()
        } else {
            std::collections::HashSet::new()
        };

        // Get current watermark
        let watermark = self.checkpoint_coordinator.watermark().await;

        // List uncommitted files
        let uncommitted = self
            .file_reader
            .list_uncommitted_files(watermark.as_deref(), &committed_paths)
            .await?;

        if uncommitted.is_empty() {
            return Ok(None);
        }

        // Read parquet metadata for each file
        let mut files = Vec::with_capacity(uncommitted.len());
        for incoming in &uncommitted {
            match self.file_reader.read_parquet_metadata(incoming).await {
                Ok(finished_file) => files.push(finished_file),
                Err(e) => {
                    tracing::warn!(
                        target = %self.table_key,
                        path = %incoming.path,
                        error = %e,
                        "Failed to read parquet metadata, skipping file"
                    );
                }
            }
        }

        if files.is_empty() {
            return Ok(None);
        }

        info!(
            target = %self.table_key,
            files = files.len(),
            "Found files to commit"
        );

        // Ensure delta sink exists (creates table with inferred schema if needed)
        self.ensure_delta_sink(&files).await?;

        Ok(Some(PreparedState { files }))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let PreparedState { files } = state;

        // Emit pending files metric
        emit!(PendingFiles {
            target: self.table_key.id().to_string(),
            count: files.len(),
        });

        let delta_sink = self
            .delta_sink
            .as_mut()
            .context(DeltaSinkNotInitializedSnafu)?;

        // Track the highest path for watermark update
        let highest_path = files
            .iter()
            .map(|f| f.filename.as_str())
            .max()
            .map(String::from);

        // Commit files to Delta Lake
        let committed_count = self
            .checkpoint_coordinator
            .commit_files(
                delta_sink,
                &files,
                self.table_config.delta_checkpoint_interval,
            )
            .await;

        if committed_count > 0 {
            // Update watermark to highest committed path
            if let Some(path) = highest_path {
                self.checkpoint_coordinator.update_watermark(path).await;
            }

            let records_committed: usize = files.iter().map(|f| f.record_count).sum();

            // Emit metrics
            emit!(FilesCommitted {
                target: self.table_key.id().to_string(),
                count: committed_count as u64,
            });
            emit!(RecordsCommitted {
                target: self.table_key.id().to_string(),
                count: records_committed as u64,
            });
            emit!(DeltaTableVersion {
                target: self.table_key.id().to_string(),
                version: delta_sink.version(),
            });

            info!(
                target = %self.table_key,
                files = committed_count,
                version = delta_sink.version(),
                "Committed files to Delta Lake"
            );
        }

        // Reset pending files gauge after processing
        emit!(PendingFiles {
            target: self.table_key.id().to_string(),
            count: 0,
        });

        Ok(IterationResult::ProcessedItems)
    }
}
