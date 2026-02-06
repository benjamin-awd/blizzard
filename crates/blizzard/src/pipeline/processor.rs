//! The Processor - core file processing logic.
//!
//! Implements the PollingProcessor trait for processing NDJSON files to Parquet.
//! Supports multiple sources merging into a single sink.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use deltalake::arrow::datatypes::SchemaRef;
use indexmap::IndexMap;
use tokio::sync::Semaphore;
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
use crate::config::{MB, PipelineConfig, PipelineKey, SourceConfig};
use crate::dlq::{DeadLetterQueue, FailureTracker};
use crate::error::{ConfigError, PipelineError, StorageSnafu};
use crate::parquet::{ParquetWriterConfig, RollingPolicy};
use crate::source::{FileReader, NdjsonReader, NdjsonReaderConfig, infer_schema_from_source};

/// Resolves all configuration and creates dependencies for pipeline execution.
///
/// Handles all pre-runtime configuration decisions:
/// - Storage provider creation
/// - Schema resolution (explicit or inferred)
/// - Reader creation
/// - Tracker initialization
/// - DLQ setup
///
/// # Example
///
/// ```ignore
/// let resolver = ConfigResolver::new(key, &config, storage_pool);
/// let resolved = resolver.resolve().await?;
/// let orchestrator = PipelineOrchestrator::new(key, config, resolved, shutdown);
/// ```
pub(super) struct ConfigResolver<'a> {
    key: PipelineKey,
    config: &'a PipelineConfig,
    storage_pool: Option<StoragePoolRef>,
}

impl<'a> ConfigResolver<'a> {
    /// Create a new resolver with the given configuration.
    pub fn new(
        key: PipelineKey,
        config: &'a PipelineConfig,
        storage_pool: Option<StoragePoolRef>,
    ) -> Self {
        Self {
            key,
            config,
            storage_pool,
        }
    }

    /// Resolve all configuration and create dependencies.
    ///
    /// This is the main entry point that orchestrates all dependency creation.
    pub async fn resolve(self) -> Result<ResolvedConfig, PipelineError> {
        let source_storages = self.create_source_storages().await?;
        let destination_storage = self.create_destination_storage().await?;
        let multi_tracker = self.create_multi_source_tracker().await?;

        let schema = self.resolve_schema(&source_storages).await?;
        let readers = self.create_readers(&schema);
        let partition_extractor = self.create_partition_extractor();
        let dlq = self.create_dlq().await?;
        let failure_tracker = self.create_failure_tracker(dlq);

        let ctx = ProcessorContext {
            source_storages,
            destination_storage,
            schema,
            readers,
            partition_extractor,
        };

        Ok(ResolvedConfig {
            ctx,
            multi_tracker,
            failure_tracker,
        })
    }

    /// Create storage providers for each configured source.
    async fn create_source_storages(
        &self,
    ) -> Result<IndexMap<String, StorageProviderRef>, PipelineError> {
        let mut source_storages = IndexMap::new();
        for (source_name, source_config) in &self.config.sources {
            let storage = get_or_create_storage(
                &self.storage_pool,
                &source_config.path,
                source_config.storage_options.clone(),
            )
            .await
            .context(StorageSnafu)?;
            source_storages.insert(source_name.clone(), storage);
        }
        Ok(source_storages)
    }

    /// Create storage provider for the destination.
    async fn create_destination_storage(&self) -> Result<StorageProviderRef, PipelineError> {
        get_or_create_storage(
            &self.storage_pool,
            &self.config.sink.table_uri,
            self.config.sink.storage_options.clone(),
        )
        .await
        .context(StorageSnafu)
    }

    /// Create multi-source tracker with per-source trackers based on configuration.
    async fn create_multi_source_tracker(&self) -> Result<MultiSourceTracker, PipelineError> {
        let mut trackers: IndexMap<String, Box<dyn super::tracker::StateTracker>> = IndexMap::new();

        for (source_name, source_config) in &self.config.sources {
            let tracker: Box<dyn super::tracker::StateTracker> = if source_config.use_watermark {
                // Checkpoint manager needs its own storage provider (not pooled)
                let checkpoint_storage = get_or_create_storage(
                    &None,
                    &self.config.sink.table_uri,
                    self.config.sink.storage_options.clone(),
                )
                .await
                .context(StorageSnafu)?;
                let checkpoint_manager = CheckpointManager::new(
                    checkpoint_storage,
                    self.key.id().to_string(),
                    source_name.clone(),
                );
                Box::new(WatermarkTracker::new(checkpoint_manager))
            } else {
                Box::new(HashMapTracker::new())
            };
            trackers.insert(source_name.clone(), tracker);
        }

        Ok(MultiSourceTracker::new(trackers, self.key.id().to_string()))
    }

    /// Resolve the Arrow schema from explicit config or by inference from source files.
    async fn resolve_schema(
        &self,
        source_storages: &IndexMap<String, StorageProviderRef>,
    ) -> Result<SchemaRef, PipelineError> {
        if self.config.schema.should_infer() {
            let first_source =
                self.config
                    .sources
                    .values()
                    .next()
                    .ok_or_else(|| PipelineError::Config {
                        source: ConfigError::Internal {
                            message: "No sources configured".to_string(),
                        },
                    })?;
            let first_storage = source_storages.values().next().unwrap();
            let prefixes = first_source.date_prefixes();
            Ok(infer_schema_from_source(
                first_storage,
                first_source.compression,
                prefixes.as_deref(),
                self.key.as_ref(),
                self.config.schema.should_coerce_conflicts_to_utf8(),
            )
            .await?)
        } else {
            Ok(self.config.schema.to_arrow_schema())
        }
    }

    /// Create per-source readers (compression may differ between sources).
    fn create_readers(&self, schema: &SchemaRef) -> IndexMap<String, Arc<dyn FileReader>> {
        let mut readers = IndexMap::new();
        let coerce_objects = self.config.schema.should_coerce_conflicts_to_utf8();

        for (source_name, source_config) in &self.config.sources {
            let mut reader_config =
                NdjsonReaderConfig::new(source_config.batch_size, source_config.compression);
            if coerce_objects {
                reader_config = reader_config.coerce_objects_to_strings();
            }
            let reader: Arc<dyn FileReader> = Arc::new(NdjsonReader::new(
                schema.clone(),
                reader_config,
                self.key.id().to_string(),
            ));
            readers.insert(source_name.clone(), reader);
        }
        readers
    }

    /// Create partition extractor from sink configuration.
    fn create_partition_extractor(&self) -> PartitionExtractor {
        let partition_columns = self
            .config
            .sink
            .partition_by
            .as_ref()
            .map(|p| p.partition_columns())
            .unwrap_or_default();
        PartitionExtractor::new(partition_columns)
    }

    /// Create DLQ if configured.
    async fn create_dlq(&self) -> Result<Option<Arc<DeadLetterQueue>>, PipelineError> {
        Ok(DeadLetterQueue::from_config(&self.config.error_handling)
            .await?
            .map(Arc::new))
    }

    /// Create failure tracker with optional DLQ.
    fn create_failure_tracker(&self, dlq: Option<Arc<DeadLetterQueue>>) -> FailureTracker {
        FailureTracker::new(
            self.config.error_handling.max_failures,
            dlq,
            self.key.id().to_string(),
        )
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

/// Container for all resolved configuration and dependencies.
///
/// Produced by [`ConfigResolver::resolve()`] and consumed by [`PipelineOrchestrator::new()`].
pub(super) struct ResolvedConfig {
    /// Runtime I/O dependencies.
    pub ctx: ProcessorContext,
    /// Per-source state trackers.
    pub multi_tracker: MultiSourceTracker,
    /// Failure tracking and DLQ management.
    pub failure_tracker: FailureTracker,
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
        global_semaphore: Option<Arc<Semaphore>>,
        key: &str,
    ) -> Result<Self, PipelineError> {
        let total_files = pending_files.len();

        // Build rolling policies from config
        let mut rolling_policies = vec![RollingPolicy::SizeLimit(config.sink.file_size_mb * MB)];
        if let Some(secs) = config.sink.rollover_timeout_secs {
            rolling_policies.push(RollingPolicy::RolloverDuration(Duration::from_secs(secs)));
        }

        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(config.sink.file_size_mb)
            .with_row_group_size_bytes(config.sink.row_group_size_bytes)
            .with_compression(config.sink.compression)
            .with_rolling_policies(rolling_policies);

        // Spawn upload task for concurrent uploads
        let upload_task = UploadTask::spawn(
            ctx.destination_storage.clone(),
            config.sink.max_concurrent_uploads,
            global_semaphore.clone(),
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
            ctx.source_storages.clone(),
            shutdown,
            config.max_concurrent_files,
            global_semaphore,
            key.to_string(),
        );

        let max_in_flight = config.max_concurrent_files * 2;
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

/// Runtime orchestrator for the pipeline polling loop.
///
/// Handles the prepare/process cycle for file processing. Created by
/// [`Processor::new()`] after configuration resolution.
pub(super) struct PipelineOrchestrator {
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
    /// Optional global semaphore for cross-pipeline concurrency limiting.
    global_semaphore: Option<Arc<Semaphore>>,
}

impl PipelineOrchestrator {
    /// Create a new orchestrator from resolved configuration.
    fn new(
        key: PipelineKey,
        config: PipelineConfig,
        resolved: ResolvedConfig,
        global_semaphore: Option<Arc<Semaphore>>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            key,
            config,
            ctx: resolved.ctx,
            multi_tracker: resolved.multi_tracker,
            failure_tracker: resolved.failure_tracker,
            shutdown,
            global_semaphore,
        }
    }
}

/// Public API for creating pipeline processors.
///
/// Acts as a facade that coordinates configuration resolution and
/// orchestrator creation.
pub(super) struct Processor;

impl Processor {
    /// Create a new processor with the given configuration.
    ///
    /// Resolves all configuration and dependencies, then returns an
    /// orchestrator that implements [`PollingProcessor`].
    #[allow(clippy::new_ret_no_self)]
    pub async fn new(
        key: PipelineKey,
        config: PipelineConfig,
        storage_pool: Option<StoragePoolRef>,
        global_semaphore: Option<Arc<Semaphore>>,
        shutdown: CancellationToken,
    ) -> Result<impl PollingProcessor<State = Vec<SourcedFile>, Error = PipelineError>, PipelineError>
    {
        let resolver = ConfigResolver::new(key.clone(), &config, storage_pool);
        let resolved = resolver.resolve().await?;
        Ok(PipelineOrchestrator::new(
            key,
            config,
            resolved,
            global_semaphore,
            shutdown,
        ))
    }
}

#[async_trait]
impl PollingProcessor for PipelineOrchestrator {
    type State = Vec<SourcedFile>;
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        let configs: IndexMap<String, &SourceConfig> = self
            .config
            .sources
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect();

        if cold_start {
            self.multi_tracker.init_all(&configs).await?;
        }

        let pending_files = self
            .multi_tracker
            .list_all_pending(&self.ctx.source_storages, &configs)
            .await?;

        if pending_files.is_empty() {
            // Mark all trackers as idle when no new files are found.
            // This distinguishes between "no files exist" vs "files filtered by watermark".
            self.multi_tracker.mark_all_idle();
            return Ok(None);
        }

        emit!(FilesDiscovered {
            count: pending_files.len() as u64,
            target: self.key.id().to_string(),
        });

        // Build per-source file counts for logging
        let mut source_counts: IndexMap<&str, usize> = IndexMap::new();
        for file in &pending_files {
            *source_counts.entry(file.source_name.as_str()).or_default() += 1;
        }
        let breakdown: Vec<_> = source_counts
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();

        info!(
            target = %self.key,
            files = pending_files.len(),
            breakdown = %breakdown.join(", "),
            "Found files to process"
        );

        Ok(Some(pending_files))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let iteration = Iteration::new(
            state,
            &self.ctx,
            &self.config,
            self.shutdown.clone(),
            self.global_semaphore.clone(),
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

    async fn finalize(&mut self) -> Result<(), Self::Error> {
        let table_uri = &self.config.sink.table_uri;
        let checkpoint_dir = crate::checkpoint::CHECKPOINT_DIR;
        info!(
            target = %self.key,
            checkpoint_path = %format!("{table_uri}/{checkpoint_dir}/"),
            "Saving checkpoint on shutdown"
        );
        if let Err(e) = self.multi_tracker.save_all().await {
            warn!(target = %self.key, error = %e, "Failed to save checkpoint on shutdown");
        }
        Ok(())
    }
}
