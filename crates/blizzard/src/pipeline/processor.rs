//! The Processor - core file processing logic.
//!
//! Implements the PollingProcessor trait for processing NDJSON files to Parquet.

use std::sync::Arc;

use async_trait::async_trait;
use deltalake::arrow::datatypes::SchemaRef;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use snafu::ResultExt;

use blizzard_core::emit;
use blizzard_core::metrics::events::FilesDiscovered;
use blizzard_core::polling::{IterationResult, PollingProcessor};
use blizzard_core::{
    PartitionExtractor, StoragePoolRef, StorageProviderRef, get_or_create_storage,
};

use super::download::{Downloader, IncrementalCheckpointConfig};
use super::sink::Sink;
use super::tasks::{DownloadTask, UploadTask};
use super::tracker::{HashMapTracker, StateTracker, WatermarkTracker};
use crate::checkpoint::CheckpointManager;
use crate::config::{PipelineConfig, PipelineKey};
use crate::dlq::{DeadLetterQueue, FailureTracker};
use crate::error::PipelineError;
use crate::error::StorageSnafu;
use crate::parquet::ParquetWriterConfig;
use crate::source::{FileReader, NdjsonReader, NdjsonReaderConfig, infer_schema_from_source};

/// Builder for constructing a [`Processor`] with configurable dependencies.
///
/// Separates the construction of processor dependencies from the processor itself,
/// providing clearer initialization logic.
///
/// # Example
///
/// ```ignore
/// let processor = ProcessorBuilder::new(key, config, shutdown)
///     .with_storage_pool(pool)
///     .build()
///     .await?;
/// ```
pub(super) struct ProcessorBuilder {
    key: PipelineKey,
    config: PipelineConfig,
    shutdown: CancellationToken,
    storage_pool: Option<StoragePoolRef>,
}

impl ProcessorBuilder {
    /// Create a new builder with required parameters.
    pub fn new(key: PipelineKey, config: PipelineConfig, shutdown: CancellationToken) -> Self {
        Self {
            key,
            config,
            shutdown,
            storage_pool: None,
        }
    }

    /// Set the storage pool for connection reuse.
    pub fn with_storage_pool(mut self, pool: Option<StoragePoolRef>) -> Self {
        self.storage_pool = pool;
        self
    }

    /// Build the processor, creating all dependencies.
    pub async fn build(self) -> Result<Processor, PipelineError> {
        let Self {
            key,
            config,
            shutdown,
            storage_pool,
        } = self;

        // Create source storage provider
        let source_storage = get_or_create_storage(
            &storage_pool,
            &config.source.path,
            config.source.storage_options.clone(),
        )
        .await
        .context(StorageSnafu)?;

        // Create destination storage provider
        let destination_storage = get_or_create_storage(
            &storage_pool,
            &config.sink.table_uri,
            config.sink.storage_options.clone(),
        )
        .await
        .context(StorageSnafu)?;

        // Create state tracker
        let state_tracker = create_state_tracker(&config, &key).await?;

        // Get schema - either from explicit config or by inference
        let schema = resolve_schema(&config, &source_storage, &key).await?;

        // Create reader
        let reader_config =
            NdjsonReaderConfig::new(config.source.batch_size, config.source.compression);
        let reader: Arc<dyn FileReader> = Arc::new(NdjsonReader::new(
            schema.clone(),
            reader_config,
            key.id().to_string(),
        ));

        // Set up DLQ if configured
        let dlq = DeadLetterQueue::from_config(&config.error_handling)
            .await?
            .map(Arc::new);

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

        Ok(Processor {
            failure_tracker: FailureTracker::new(
                config.error_handling.max_failures,
                dlq,
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

/// Create the appropriate state tracker based on configuration.
async fn create_state_tracker(
    config: &PipelineConfig,
    key: &PipelineKey,
) -> Result<Box<dyn StateTracker>, PipelineError> {
    if config.source.use_watermark {
        // Checkpoint manager needs its own storage provider (not pooled)
        let checkpoint_storage = get_or_create_storage(
            &None,
            &config.sink.table_uri,
            config.sink.storage_options.clone(),
        )
        .await
        .context(StorageSnafu)?;
        let checkpoint_manager = CheckpointManager::new(checkpoint_storage, key.id().to_string());
        Ok(Box::new(WatermarkTracker::new(checkpoint_manager)))
    } else {
        Ok(Box::new(HashMapTracker::new()))
    }
}

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
    /// File reader for parsing source files.
    pub reader: Arc<dyn FileReader>,
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
    checkpoint_config: IncrementalCheckpointConfig,
    total_files: usize,
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
        let total_files = pending_files.len();

        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(config.sink.file_size_mb)
            .with_row_group_size_bytes(config.sink.row_group_size_bytes)
            .with_compression(config.sink.compression);

        // Spawn upload task for concurrent uploads
        let upload_task = UploadTask::spawn(
            ctx.destination_storage.clone(),
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

        // Create incremental checkpoint config from source settings
        let checkpoint_config = IncrementalCheckpointConfig::new(
            &config.source.checkpoint,
            config.source.use_watermark,
        );

        Ok(Self {
            sink,
            downloader,
            download_task,
            checkpoint_config,
            total_files,
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
                &self.checkpoint_config,
                self.total_files,
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
    /// Create a new processor with the given configuration.
    ///
    /// This is a convenience method that uses [`ProcessorBuilder`] internally.
    /// For more control over dependency injection, use the builder directly.
    pub async fn new(
        key: PipelineKey,
        config: PipelineConfig,
        storage_pool: Option<StoragePoolRef>,
        shutdown: CancellationToken,
    ) -> Result<Self, PipelineError> {
        ProcessorBuilder::new(key, config, shutdown)
            .with_storage_pool(storage_pool)
            .build()
            .await
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
                Some(msg) => info!(target = %self.key, "{msg}"),
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

        emit!(FilesDiscovered {
            count: pending_files.len() as u64,
            target: self.key.id().to_string(),
        });

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
