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

use super::download::{
    Downloader, IncrementalCheckpointConfig, ProcessingContext, SinkWorkerChannels,
};
use super::sink::Sink;
use super::tasks::{DiscoveryTask, DownloadTask, ProcessedFile, UploadTask, run_sink_worker};
use super::tracker::{HashMapTracker, MultiSourceTracker, WatermarkTracker};
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
/// (workers, downloader, download task) from the long-lived processor state.
struct Iteration {
    /// Channels for communicating with sink workers.
    worker_channels: SinkWorkerChannels,
    /// Join handles for sink worker tasks (for waiting on finalization).
    worker_handles: Vec<tokio::task::JoinHandle<()>>,
    downloader: Downloader,
    download_task: DownloadTask,
    discovery_handle: tokio::task::JoinHandle<Result<usize, PipelineError>>,
    checkpoint_config: IncrementalCheckpointConfig,
}

impl Iteration {
    /// Create a new iteration with all per-iteration components.
    ///
    /// Takes a `DiscoveryTask` whose channel is passed to the `DownloadTask`,
    /// forming the pipeline: discovery → download → parse → N sink workers.
    fn new(
        discovery_task: DiscoveryTask,
        ctx: &ProcessorContext,
        config: &PipelineConfig,
        shutdown: CancellationToken,
        global_semaphore: Option<Arc<Semaphore>>,
        key: &str,
    ) -> Result<Self, PipelineError> {
        let sink_parallelism = config.sink_parallelism;

        // Build rolling policies from config
        let mut rolling_policies = vec![RollingPolicy::SizeLimit(config.sink.file_size_mb * MB)];
        if let Some(secs) = config.sink.rollover_timeout_secs {
            rolling_policies.push(RollingPolicy::RolloverDuration(Duration::from_secs(secs)));
        }

        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(config.sink.file_size_mb)
            .with_row_group_size_bytes(config.sink.row_group_size_bytes)
            .with_compression(config.sink.compression)
            .with_rolling_policies(rolling_policies.clone());

        // Spawn N sink workers, each with its own Sink (own ParquetWriter + UploadTask)
        let (result_tx, result_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut file_txs = Vec::with_capacity(sink_parallelism);
        let mut worker_handles = Vec::with_capacity(sink_parallelism);

        for _ in 0..sink_parallelism {
            let upload_task = UploadTask::spawn(
                ctx.destination_storage.clone(),
                config.sink.max_concurrent_uploads,
                global_semaphore.clone(),
                key.to_string(),
            );

            let sink = Sink::new(
                ctx.schema.clone(),
                writer_config.clone(),
                upload_task,
                ctx.partition_extractor.clone(),
                key.to_string(),
            )?;

            // Bounded channel with capacity 1: workers pull files on demand,
            // providing natural backpressure to the distributor.
            let (file_tx, file_rx) = tokio::sync::mpsc::channel::<ProcessedFile>(1);
            let worker_result_tx = result_tx.clone();

            let handle = tokio::spawn(run_sink_worker(sink, file_rx, worker_result_tx));

            file_txs.push(file_tx);
            worker_handles.push(handle);
        }

        // Drop the original result_tx — workers hold the clones
        drop(result_tx);

        let worker_channels = SinkWorkerChannels {
            file_txs,
            result_rx,
        };

        // Feed discovery channel into download task
        let download_task = DownloadTask::spawn(
            discovery_task.rx,
            ctx.source_storages.clone(),
            shutdown,
            config.max_concurrent_files,
            global_semaphore,
            key.to_string(),
        );

        let max_in_flight = config.max_concurrent_files.min(8);
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
            worker_channels,
            worker_handles,
            downloader,
            download_task,
            discovery_handle: discovery_task.handle,
            checkpoint_config,
        })
    }

    /// Run the iteration: download, parse, and write files via sink workers.
    ///
    /// Returns the iteration result and the total number of files discovered.
    async fn run(
        self,
        multi_tracker: &mut MultiSourceTracker,
        failure_tracker: &mut FailureTracker,
        shutdown: CancellationToken,
    ) -> Result<(IterationResult, usize), PipelineError> {
        let mut ctx = ProcessingContext {
            multi_tracker,
            failure_tracker,
        };
        let result = self
            .downloader
            .run(
                self.download_task,
                &mut ctx,
                self.worker_channels,
                shutdown,
                &self.checkpoint_config,
            )
            .await?;

        // Wait for discovery to complete and get total files discovered.
        // Discovery should already be done by the time all downloads finish,
        // so this join is typically instant.
        let discovery_count = self
            .discovery_handle
            .await
            .map_err(|e| PipelineError::TaskJoin { source: e })??;

        // Wait for all sink workers to finalize (flush + upload remaining files).
        // Workers exit when their file_tx senders are dropped (which happens
        // when SinkWorkerChannels is dropped at end of Downloader::run).
        for handle in self.worker_handles {
            handle
                .await
                .map_err(|e| PipelineError::TaskJoin { source: e })?;
        }

        Ok((result, discovery_count))
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
    ) -> Result<impl PollingProcessor<State = (), Error = PipelineError>, PipelineError> {
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
    type State = ();
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        if cold_start {
            let configs: IndexMap<String, &SourceConfig> = self
                .config
                .sources
                .iter()
                .map(|(k, v)| (k.clone(), v))
                .collect();
            self.multi_tracker.init_all(&configs).await?;
        }

        // Always proceed to process — discovery runs there and returns
        // NoItems if nothing is found.
        Ok(Some(()))
    }

    async fn process(&mut self, _state: Self::State) -> Result<IterationResult, Self::Error> {
        let configs: IndexMap<String, &SourceConfig> = self
            .config
            .sources
            .iter()
            .map(|(k, v)| (k.clone(), v))
            .collect();

        // Take discovery snapshots from trackers before spawning
        let discovery_sources = self
            .multi_tracker
            .discovery_sources(&self.ctx.source_storages, &configs)?;

        // Spawn discovery task — streams files through a channel
        let discovery_task = DiscoveryTask::spawn(
            discovery_sources,
            self.shutdown.clone(),
            self.key.id().to_string(),
        );

        let iteration = Iteration::new(
            discovery_task,
            &self.ctx,
            &self.config,
            self.shutdown.clone(),
            self.global_semaphore.clone(),
            self.key.id(),
        )?;

        let (result, discovery_count) = iteration
            .run(
                &mut self.multi_tracker,
                &mut self.failure_tracker,
                self.shutdown.clone(),
            )
            .await?;

        // Emit discovery metric now that we know the total
        if discovery_count > 0 {
            emit!(FilesDiscovered {
                count: discovery_count as u64,
                target: self.key.id().to_string(),
            });
        } else {
            // No files discovered — mark trackers idle
            self.multi_tracker.mark_all_idle();
        }

        // Finalize iteration: DLQ and save state
        // (Sink finalization happens inside each worker task)
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
