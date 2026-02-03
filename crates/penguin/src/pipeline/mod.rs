//! Pipeline for discovering parquet files and committing to Delta Lake.
//!
//! This module implements the main processing loop using the PollingProcessor
//! trait from blizzard-core. It supports running multiple Delta tables
//! concurrently with shared shutdown handling and optional global concurrency limits.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use snafu::{OptionExt, ResultExt};
use tokio::sync::Semaphore;
use tracing::info;

use blizzard_core::polling::{IterationResult, PollingProcessor, run_polling_loop};
use blizzard_core::{
    FinishedFile, PipelineContext, StoragePoolRef, StorageProvider, get_or_create_storage,
};

use crate::emit;
use crate::metrics::events::{DeltaTableVersion, FilesCommitted, PendingFiles, RecordsCommitted};

use crate::checkpoint::CheckpointCoordinator;
use crate::config::{Config, Mergeable, TableConfig, TableKey};
use crate::error::{DeltaSinkNotInitializedSnafu, PipelineError, StorageSnafu};
use crate::incoming::{IncomingConfig, IncomingReader};
use crate::schema::infer_schema_from_first_file;
use crate::schema::manager::SchemaManager;
use crate::sink::DeltaSink;

/// A penguin pipeline unit for committing parquet files to Delta Lake.
pub struct Pipeline {
    pub key: TableKey,
    pub config: TableConfig,
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
        let poll_interval = Duration::from_secs(self.config.poll_interval_secs);
        let poll_jitter_secs = self.context.poll_jitter_secs;

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
                self.context.global_semaphore,
                self.context.storage_pool,
            ) => result?,
        };

        info!(
            target = %self.key,
            poll_interval_secs = poll_interval.as_secs(),
            poll_jitter_secs,
            "Table processor initialized"
        );

        run_polling_loop(
            &mut processor,
            poll_interval,
            poll_jitter_secs,
            self.context.shutdown,
            self.key.id(),
        )
        .await?;

        Ok(())
    }
}

impl blizzard_core::Pipeline for Pipeline {
    type Key = TableKey;
    type Error = PipelineError;

    fn key(&self) -> &Self::Key {
        &self.key
    }

    async fn run(self) -> Result<(), Self::Error> {
        self.execute().await
    }
}

/// The penguin delta checkpointer pipeline processor.
///
/// Each table runs its own `Processor` instance, with optional
/// sharing of a global semaphore for cross-table concurrency control.
struct Processor {
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

impl Processor {
    async fn new(
        table_key: TableKey,
        table_config: TableConfig,
        global_semaphore: Option<Arc<Semaphore>>,
        storage_pool: Option<StoragePoolRef>,
    ) -> Result<Self, PipelineError> {
        // Create sink storage provider - use pooled if available
        let sink_storage = get_or_create_storage(
            &storage_pool,
            &table_config.table_uri,
            table_config.storage_options.clone(),
        )
        .await
        .context(StorageSnafu)?;

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

        // Table exists - use SchemaManager for validation and evolution
        let sink = self
            .delta_sink
            .as_mut()
            .context(DeltaSinkNotInitializedSnafu)?;

        let schema_manager = SchemaManager::new(
            sink.schema().cloned(),
            self.table_config.schema_evolution,
            self.table_key.id().to_string(),
        );

        // Validate and log the schema evolution action
        let action = schema_manager.validate_and_log(&incoming_schema)?;

        // Apply the evolution action
        sink.evolve_schema(action).await?;

        Ok(())
    }
}

#[async_trait]
impl PollingProcessor for Processor {
    type State = Vec<FinishedFile>;
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        // On cold start, recover checkpoint from table log if table exists
        // We do this first to get the watermark before listing files
        if cold_start && let Some(delta_sink) = self.delta_sink.as_mut() {
            info!(target = %self.table_key, "Cold start - recovering checkpoint from table log");
            match self
                .checkpoint_coordinator
                .restore_from_table_log(delta_sink)
                .await
            {
                Ok(true) => info!(target = %self.table_key, "Recovered checkpoint from table log"),
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

        Ok(Some(files))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let files = state;

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
