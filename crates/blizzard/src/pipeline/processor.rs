//! The Processor - core file processing logic.
//!
//! Implements the PollingProcessor trait for processing NDJSON files to Parquet.

use std::sync::Arc;

use async_trait::async_trait;
use deltalake::arrow::datatypes::SchemaRef;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use blizzard_core::polling::{IterationResult, PollingProcessor};
use blizzard_core::{PartitionExtractor, StoragePoolRef, StorageProviderRef};

use super::create_storage;
use super::download::Downloader;
use super::sink::Sink;
use super::tasks::{DownloadTask, UploadTask};
use super::tracker::{HashMapTracker, StateTracker, WatermarkTracker};
use crate::checkpoint::CheckpointManager;
use crate::config::{PipelineConfig, PipelineKey};
use crate::dlq::{DeadLetterQueue, FailureTracker};
use crate::error::PipelineError;
use crate::parquet::ParquetWriterConfig;
use crate::source::{NdjsonReader, NdjsonReaderConfig, infer_schema_from_source};

/// Resolve the Arrow schema from explicit config or by inference from source files.
async fn resolve_schema(
    config: &PipelineConfig,
    storage: &StorageProviderRef,
    pipeline_key: &PipelineKey,
) -> Result<SchemaRef, PipelineError> {
    if config.schema.should_infer() {
        let prefixes = config.source.date_prefixes();
        Ok(infer_schema_from_source(
            storage,
            config.source.compression,
            prefixes.as_deref(),
            pipeline_key.as_ref(),
        )
        .await?)
    } else {
        Ok(config.schema.to_arrow_schema())
    }
}

/// Runtime dependencies shared across processing iterations.
///
/// Groups the components needed for reading source files and writing output,
/// reducing field count in the main processor struct.
pub(super) struct ProcessorContext {
    /// Storage provider for reading source files.
    pub source_storage: StorageProviderRef,
    /// Storage provider for writing destination files.
    pub destination_storage: StorageProviderRef,
    /// Arrow schema (from config or inferred).
    pub schema: SchemaRef,
    /// NDJSON reader with schema validation.
    pub reader: Arc<NdjsonReader>,
    /// Extracts partition values from source paths.
    pub partition_extractor: PartitionExtractor,
}

/// Encapsulates the state and logic for a single processing iteration.
///
/// Created fresh for each iteration, isolating per-iteration components
/// (writer, downloader, download task) from the long-lived processor state.
struct Iteration {
    sink: Sink,
    downloader: Downloader,
    download_task: DownloadTask,
}

impl Iteration {
    /// Create a new iteration with all per-iteration components.
    fn new(
        pending_files: Vec<String>,
        ctx: &ProcessorContext,
        config: &PipelineConfig,
        shutdown: CancellationToken,
        key: &str,
    ) -> Result<Self, PipelineError> {
        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(config.sink.file_size_mb)
            .with_row_group_size_bytes(config.sink.row_group_size_bytes)
            .with_compression(config.sink.compression);

        // Spawn upload task for concurrent uploads
        let upload_task = UploadTask::spawn(
            ctx.destination_storage.clone(),
            shutdown.clone(),
            config.sink.max_concurrent_uploads,
            key.to_string(),
        );

        let sink = Sink::new(
            ctx.schema.clone(),
            writer_config,
            upload_task,
            ctx.partition_extractor.clone(),
            key.to_string(),
        )?;

        let download_task = DownloadTask::spawn(
            pending_files,
            ctx.source_storage.clone(),
            shutdown,
            config.source.max_concurrent_files,
            key.to_string(),
        );

        let max_in_flight = config.source.max_concurrent_files * 2;
        let downloader = Downloader::new(ctx.reader.clone(), max_in_flight, key.to_string());

        Ok(Self {
            sink,
            downloader,
            download_task,
        })
    }

    /// Run the iteration: download, parse, and write files.
    async fn run(
        mut self,
        state_tracker: &mut dyn StateTracker,
        failure_tracker: &mut FailureTracker,
        shutdown: CancellationToken,
    ) -> Result<(IterationResult, Sink), PipelineError> {
        let result = self
            .downloader
            .run(
                self.download_task,
                &mut self.sink,
                state_tracker,
                failure_tracker,
                shutdown,
            )
            .await;

        // Return the sink so the processor can finalize it
        Ok((result?, self.sink))
    }
}

/// The blizzard file loader pipeline processor.
///
/// Each pipeline runs its own `Processor` instance.
pub(super) struct Processor {
    /// Identifier for this pipeline (used in logging and metrics).
    key: PipelineKey,
    /// Configuration for this specific pipeline.
    config: PipelineConfig,
    /// Runtime dependencies for reading and writing.
    ctx: ProcessorContext,
    /// Tracks which source files have been processed.
    state_tracker: Box<dyn StateTracker>,
    /// Tracks failures and manages DLQ.
    failure_tracker: FailureTracker,
    /// Shutdown signal for graceful termination.
    shutdown: CancellationToken,
}

impl Processor {
    pub async fn new(
        key: PipelineKey,
        config: PipelineConfig,
        storage_pool: Option<StoragePoolRef>,
        shutdown: CancellationToken,
    ) -> Result<Self, PipelineError> {
        // Create source storage provider - use pooled if available
        let source_storage = create_storage(&storage_pool, &config.source).await?;

        // Create destination storage provider for uploads
        let destination_storage = create_storage(&storage_pool, &config.sink).await?;

        // Create state tracker based on configuration
        let state_tracker: Box<dyn StateTracker> = if config.source.use_watermark {
            // Checkpoint manager needs its own storage provider (not pooled)
            let checkpoint_storage = create_storage(&None, &config.sink).await?;
            let checkpoint_manager =
                CheckpointManager::new(checkpoint_storage, key.id().to_string());
            Box::new(WatermarkTracker::new(checkpoint_manager))
        } else {
            Box::new(HashMapTracker::new())
        };

        // Get schema - either from explicit config or by inference
        let schema = resolve_schema(&config, &source_storage, &key).await?;

        // Create NDJSON reader
        let reader_config =
            NdjsonReaderConfig::new(config.source.batch_size, config.source.compression);
        let reader = Arc::new(NdjsonReader::new(
            schema.clone(),
            reader_config,
            key.id().to_string(),
        ));

        // Set up DLQ if configured
        let dlq = DeadLetterQueue::from_config(&config.error_handling).await?;

        // Create partition extractor from config
        let partition_columns = config
            .sink
            .partition_by
            .as_ref()
            .map(|p| p.partition_columns())
            .unwrap_or_default();
        let partition_extractor = PartitionExtractor::new(partition_columns);

        // Build the processor context
        let ctx = ProcessorContext {
            source_storage,
            destination_storage,
            schema,
            reader,
            partition_extractor,
        };

        Ok(Self {
            failure_tracker: FailureTracker::new(
                config.error_handling.max_failures,
                dlq.map(Arc::new),
                key.id().to_string(),
            ),
            key,
            config,
            ctx,
            state_tracker,
            shutdown,
        })
    }
}

#[async_trait]
impl PollingProcessor for Processor {
    type State = Vec<String>;
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        let prefixes = self.config.source.date_prefixes();

        if cold_start {
            match self.state_tracker.init().await? {
                Some(msg) => info!(target = %self.key, "{}", msg),
                None => info!(
                    target = %self.key,
                    mode = self.state_tracker.mode_name(),
                    "Cold start - beginning fresh processing"
                ),
            }
        }

        let pending_files = self
            .state_tracker
            .list_pending(
                &self.ctx.source_storage,
                prefixes.as_deref(),
                self.key.as_ref(),
            )
            .await?;

        if pending_files.is_empty() {
            return Ok(None);
        }

        info!(target = %self.key, files = pending_files.len(), "Found files to process");

        Ok(Some(pending_files))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let iteration = Iteration::new(
            state,
            &self.ctx,
            &self.config,
            self.shutdown.clone(),
            self.key.id(),
        )?;

        let (result, sink) = iteration
            .run(
                self.state_tracker.as_mut(),
                &mut self.failure_tracker,
                self.shutdown.clone(),
            )
            .await?;

        // Finalize iteration: flush sink, DLQ, and save state
        sink.finalize().await?;
        self.failure_tracker.finalize_dlq().await;

        if let Err(e) = self.state_tracker.save().await {
            warn!(target = %self.key, error = %e, "Failed to save state");
        } else {
            debug!(target = %self.key, mode = self.state_tracker.mode_name(), "Saved state");
        }

        Ok(result)
    }
}
