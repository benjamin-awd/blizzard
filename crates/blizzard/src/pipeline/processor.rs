//! The Processor - core file processing logic.
//!
//! Implements the PollingProcessor trait for processing NDJSON files to Parquet.
//! Supports multiple sources merging into a single sink.

use std::sync::Arc;

use async_trait::async_trait;
use deltalake::arrow::datatypes::SchemaRef;
use indexmap::IndexMap;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use snafu::ResultExt;

use blizzard_core::emit;
use blizzard_core::metrics::events::FilesDiscovered;
use blizzard_core::polling::{IterationResult, PollingProcessor};
use blizzard_core::{
    PartitionExtractor, StoragePoolRef, StorageProviderRef, get_or_create_storage,
};

use super::download::{Downloader, IncrementalCheckpointConfig, ProcessingContext};
use super::sink::Sink;
use super::tasks::{DownloadTask, UploadTask};
use super::tracker::{HashMapTracker, MultiSourceTracker, SourcedFile, WatermarkTracker};
use crate::checkpoint::CheckpointManager;
use crate::config::{PipelineConfig, PipelineKey, SourceConfig};
use crate::dlq::{DeadLetterQueue, FailureTracker};
use crate::error::{ConfigError, PipelineError, StorageSnafu};
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

        // Create per-source storage providers
        let mut source_storages = IndexMap::new();
        for (source_name, source_config) in &config.sources {
            let storage = get_or_create_storage(
                &storage_pool,
                &source_config.path,
                source_config.storage_options.clone(),
            )
            .await
            .context(StorageSnafu)?;
            source_storages.insert(source_name.clone(), storage);
        }

        // Create destination storage provider
        let destination_storage = get_or_create_storage(
            &storage_pool,
            &config.sink.table_uri,
            config.sink.storage_options.clone(),
        )
        .await
        .context(StorageSnafu)?;

        // Create multi-source tracker
        let multi_tracker = create_multi_source_tracker(&config, &key).await?;

        // Get schema - either from explicit config or by inference from first source
        let first_source = config
            .sources
            .values()
            .next()
            .ok_or_else(|| PipelineError::Config {
                source: ConfigError::Internal {
                    message: "No sources configured".to_string(),
                },
            })?;
        let first_storage = source_storages.values().next().unwrap();
        let schema = resolve_schema(&config, first_source, first_storage, &key).await?;

        // Create per-source readers (compression may differ between sources)
        let mut readers = IndexMap::new();
        for (source_name, source_config) in &config.sources {
            let reader_config =
                NdjsonReaderConfig::new(source_config.batch_size, source_config.compression);
            let reader: Arc<dyn FileReader> = Arc::new(NdjsonReader::new(
                schema.clone(),
                reader_config,
                key.id().to_string(),
            ));
            readers.insert(source_name.clone(), reader);
        }

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
            source_storages,
            destination_storage,
            schema,
            readers,
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
            multi_tracker,
            shutdown,
        })
    }
}

/// Create a multi-source tracker with per-source trackers based on configuration.
async fn create_multi_source_tracker(
    config: &PipelineConfig,
    key: &PipelineKey,
) -> Result<MultiSourceTracker, PipelineError> {
    let mut trackers: IndexMap<String, Box<dyn super::tracker::StateTracker>> = IndexMap::new();

    for (source_name, source_config) in &config.sources {
        let tracker: Box<dyn super::tracker::StateTracker> = if source_config.use_watermark {
            // Checkpoint manager needs its own storage provider (not pooled)
            let checkpoint_storage = get_or_create_storage(
                &None,
                &config.sink.table_uri,
                config.sink.storage_options.clone(),
            )
            .await
            .context(StorageSnafu)?;
            let checkpoint_manager = CheckpointManager::new(
                checkpoint_storage,
                key.id().to_string(),
                source_name.clone(),
            );
            Box::new(WatermarkTracker::new(checkpoint_manager))
        } else {
            Box::new(HashMapTracker::new())
        };
        trackers.insert(source_name.clone(), tracker);
    }

    Ok(MultiSourceTracker::new(trackers, key.id().to_string()))
}

/// Resolve the Arrow schema from explicit config or by inference from source files.
async fn resolve_schema(
    config: &PipelineConfig,
    source_config: &SourceConfig,
    storage: &StorageProviderRef,
    pipeline_key: &PipelineKey,
) -> Result<SchemaRef, PipelineError> {
    if config.schema.should_infer() {
        let prefixes = source_config.date_prefixes();
        Ok(infer_schema_from_source(
            storage,
            source_config.compression,
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
    /// Storage providers for reading source files, keyed by source name.
    pub source_storages: IndexMap<String, StorageProviderRef>,
    /// Storage provider for writing destination files.
    pub destination_storage: StorageProviderRef,
    /// Arrow schema (from config or inferred).
    pub schema: SchemaRef,
    /// File readers for parsing source files, keyed by source name (compression may differ).
    pub readers: IndexMap<String, Arc<dyn FileReader>>,
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
        pending_files: Vec<SourcedFile>,
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

        // Get max_concurrent_files from first source (use consistent value across sources)
        let max_concurrent_files = config
            .sources
            .values()
            .next()
            .map(|s| s.max_concurrent_files)
            .unwrap_or(4);

        let download_task = DownloadTask::spawn(
            pending_files,
            ctx.source_storages.clone(),
            shutdown,
            max_concurrent_files,
            key.to_string(),
        );

        let max_in_flight = max_concurrent_files * 2;
        let downloader = Downloader::new(ctx.readers.clone(), max_in_flight, key.to_string());

        // Get checkpoint config from first source that uses watermark
        let checkpoint_config = config
            .sources
            .values()
            .find(|s| s.use_watermark)
            .map(|s| IncrementalCheckpointConfig::new(&s.checkpoint, true))
            .unwrap_or_else(|| IncrementalCheckpointConfig {
                interval_files: 100,
                interval: std::time::Duration::from_secs(30),
                enabled: false,
            });

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
        multi_tracker: &mut MultiSourceTracker,
        failure_tracker: &mut FailureTracker,
        shutdown: CancellationToken,
    ) -> Result<(IterationResult, Sink), PipelineError> {
        let mut ctx = ProcessingContext {
            sink: &mut self.sink,
            multi_tracker,
            failure_tracker,
        };
        let result = self
            .downloader
            .run(
                self.download_task,
                &mut ctx,
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
/// Each pipeline runs its own `Processor` instance. Supports multiple
/// sources merging into a single sink.
pub(super) struct Processor {
    /// Identifier for this pipeline (used in logging and metrics).
    key: PipelineKey,
    /// Configuration for this specific pipeline.
    config: PipelineConfig,
    /// Runtime dependencies for reading and writing.
    ctx: ProcessorContext,
    /// Tracks which source files have been processed (per-source).
    multi_tracker: MultiSourceTracker,
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
    type State = Vec<SourcedFile>;
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        if cold_start {
            self.multi_tracker.init_all().await?;
        }

        // Build config references for list_all_pending
        let configs: IndexMap<String, &SourceConfig> = self
            .config
            .sources
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect();

        let pending_files = self
            .multi_tracker
            .list_all_pending(&self.ctx.source_storages, &configs)
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
                &mut self.multi_tracker,
                &mut self.failure_tracker,
                self.shutdown.clone(),
            )
            .await?;

        // Finalize iteration: flush sink, DLQ, and save state
        sink.finalize().await?;
        self.failure_tracker.finalize_dlq().await;

        if let Err(e) = self.multi_tracker.save_all().await {
            warn!(target = %self.key, error = %e, "Failed to save state");
        } else {
            debug!(target = %self.key, "Saved state");
        }

        Ok(result)
    }
}
