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
use blizzard_core::metrics::UtilizationTimer;
use blizzard_core::metrics::events::FilesDiscovered;
use blizzard_core::polling::{IterationResult, PollingProcessor};
use blizzard_core::{
    PartitionExtractor, StoragePoolRef, StorageProviderRef, get_or_create_storage,
};

use super::coordinator::{
    Downloader, IncrementalCheckpointConfig, ProcessingContext, SinkWorkerChannels,
};
use super::sink::Sink;
use super::tasks::{
    DiscoveryTask, DownloadTask, MultipartConfig, ProcessedFile, UploadTask, run_sink_worker,
};
use super::tracker::{HashMapTracker, MultiSourceTracker, WatermarkTracker};
use crate::checkpoint::CheckpointManager;
use crate::config::{MB, PipelineConfig, PipelineKey};
use crate::dlq::{DeadLetterQueue, FailureTracker};
use crate::error::{ConfigError, PipelineError, StorageSnafu};
use crate::parquet::{ParquetWriterConfig, RollingPolicy};
use crate::source::{FileReader, NdjsonReader, NdjsonReaderConfig, infer_schema_from_source};

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
        orchestrator: &PipelineOrchestrator,
    ) -> Result<Self, PipelineError> {
        let config = &orchestrator.config;
        let key = orchestrator.key.id();
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
        let multipart_config = MultipartConfig::from_sink_config(&config.sink);

        for _ in 0..sink_parallelism {
            let upload_task = UploadTask::spawn(
                orchestrator.destination_storage.clone(),
                config.sink.max_concurrent_uploads,
                orchestrator.global_semaphore.clone(),
                key.to_string(),
                multipart_config.clone(),
            );

            let sink = Sink::new(
                orchestrator.schema.clone(),
                writer_config.clone(),
                upload_task,
                orchestrator.partition_extractor.clone(),
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
            orchestrator.source_storages.clone(),
            orchestrator.shutdown.clone(),
            config.max_concurrent_files,
            orchestrator.global_semaphore.clone(),
            key.to_string(),
        );

        let max_in_flight = config.sink_parallelism.saturating_add(2);
        let downloader =
            Downloader::new(orchestrator.readers.clone(), max_in_flight, key.to_string());

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
        util_timer: &mut UtilizationTimer,
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
                util_timer,
            )
            .await?;

        // Wait for discovery to complete and get total files discovered.
        // Discovery should already be done by the time all downloads finish,
        // so this join is typically instant.
        let discovery_count = self
            .discovery_handle
            .await
            .map_err(|e| PipelineError::TaskJoin { source: e })??;

        // Wait for all sink workers to finalize (flush + upload remaining files) in parallel.
        // Workers exit when their file_tx senders are dropped (which happens
        // when SinkWorkerChannels is dropped at end of Downloader::run).
        futures::future::try_join_all(
            self.worker_handles
                .into_iter()
                .map(|h| async { h.await.map_err(|e| PipelineError::TaskJoin { source: e }) }),
        )
        .await?;

        Ok((result, discovery_count))
    }
}

/// Runtime orchestrator for the pipeline polling loop.
///
/// Handles configuration resolution, the prepare/process cycle, and
/// all runtime state for file processing.
pub(super) struct PipelineOrchestrator {
    key: PipelineKey,
    config: PipelineConfig,
    source_storages: IndexMap<String, StorageProviderRef>,
    destination_storage: StorageProviderRef,
    schema: SchemaRef,
    readers: IndexMap<String, Arc<dyn FileReader>>,
    partition_extractor: PartitionExtractor,
    multi_tracker: MultiSourceTracker,
    failure_tracker: FailureTracker,
    shutdown: CancellationToken,
    global_semaphore: Option<Arc<Semaphore>>,
    util_timer: UtilizationTimer,
}

impl PipelineOrchestrator {
    /// Create a new orchestrator, resolving all configuration and dependencies.
    pub async fn new(
        key: PipelineKey,
        config: PipelineConfig,
        storage_pool: Option<StoragePoolRef>,
        global_semaphore: Option<Arc<Semaphore>>,
        shutdown: CancellationToken,
    ) -> Result<Self, PipelineError> {
        let source_storages = Self::create_source_storages(&config, &storage_pool).await?;
        let destination_storage = Self::create_destination_storage(&config, &storage_pool).await?;
        let multi_tracker = Self::create_multi_source_tracker(&key, &config, &storage_pool).await?;
        let schema = Self::resolve_schema(&key, &config, &source_storages).await?;
        let readers = Self::create_readers(&key, &config, &schema);
        let partition_extractor = Self::create_partition_extractor(&config);
        let dlq = Self::create_dlq(&config).await?;
        let failure_tracker = Self::create_failure_tracker(&key, &config, dlq);
        let util_timer = UtilizationTimer::new(key.id());

        Ok(Self {
            key,
            config,
            source_storages,
            destination_storage,
            schema,
            readers,
            partition_extractor,
            multi_tracker,
            failure_tracker,
            shutdown,
            global_semaphore,
            util_timer,
        })
    }

    async fn create_source_storages(
        config: &PipelineConfig,
        storage_pool: &Option<StoragePoolRef>,
    ) -> Result<IndexMap<String, StorageProviderRef>, PipelineError> {
        let mut source_storages = IndexMap::new();
        for (source_name, source_config) in &config.sources {
            let storage = get_or_create_storage(
                storage_pool,
                &source_config.path,
                source_config.storage_options.clone(),
            )
            .await
            .context(StorageSnafu {
                uri: source_config.path.clone(),
            })?;
            source_storages.insert(source_name.clone(), storage);
        }
        Ok(source_storages)
    }

    async fn create_destination_storage(
        config: &PipelineConfig,
        storage_pool: &Option<StoragePoolRef>,
    ) -> Result<StorageProviderRef, PipelineError> {
        get_or_create_storage(
            storage_pool,
            &config.sink.table_uri,
            config.sink.storage_options.clone(),
        )
        .await
        .context(StorageSnafu {
            uri: config.sink.table_uri.clone(),
        })
    }

    async fn create_multi_source_tracker(
        key: &PipelineKey,
        config: &PipelineConfig,
        storage_pool: &Option<StoragePoolRef>,
    ) -> Result<MultiSourceTracker, PipelineError> {
        let mut trackers: IndexMap<String, Box<dyn super::tracker::StateTracker>> = IndexMap::new();

        for (source_name, source_config) in &config.sources {
            let tracker: Box<dyn super::tracker::StateTracker> = if source_config.use_watermark {
                let checkpoint_storage = get_or_create_storage(
                    &None,
                    &config.sink.table_uri,
                    config.sink.storage_options.clone(),
                )
                .await
                .context(StorageSnafu {
                    uri: config.sink.table_uri.clone(),
                })?;
                let checkpoint_manager = CheckpointManager::new(
                    checkpoint_storage,
                    key.id().to_string(),
                    source_name.clone(),
                );
                Box::new(WatermarkTracker::new(checkpoint_manager))
            } else {
                Box::<HashMapTracker>::default()
            };
            trackers.insert(source_name.clone(), tracker);
        }

        // Ignore storage_pool for checkpoint storage — it needs its own provider
        let _ = storage_pool;

        Ok(MultiSourceTracker::new(trackers, key.id().to_string()))
    }

    async fn resolve_schema(
        key: &PipelineKey,
        config: &PipelineConfig,
        source_storages: &IndexMap<String, StorageProviderRef>,
    ) -> Result<SchemaRef, PipelineError> {
        use crate::config::SchemaConfig;
        match &config.schema {
            SchemaConfig::Infer {
                coerce_conflicts_to_utf8,
            } => {
                let first_source =
                    config
                        .sources
                        .values()
                        .next()
                        .ok_or_else(|| PipelineError::Config {
                            source: ConfigError::Internal {
                                message: "No sources configured".to_string(),
                            },
                        })?;
                let first_storage =
                    source_storages
                        .values()
                        .next()
                        .ok_or_else(|| PipelineError::Config {
                            source: ConfigError::Internal {
                                message: "No source storages available".to_string(),
                            },
                        })?;
                let prefixes = first_source.date_prefixes();
                Ok(infer_schema_from_source(
                    first_storage,
                    first_source.compression,
                    prefixes.as_deref(),
                    key.as_ref(),
                    *coerce_conflicts_to_utf8,
                )
                .await?)
            }
            SchemaConfig::Explicit { .. } => Ok(config.schema.to_arrow_schema()?),
        }
    }

    fn create_readers(
        key: &PipelineKey,
        config: &PipelineConfig,
        schema: &SchemaRef,
    ) -> IndexMap<String, Arc<dyn FileReader>> {
        let mut readers = IndexMap::new();
        let coerce_objects = config.schema.coerce_conflicts_to_utf8();

        for (source_name, source_config) in &config.sources {
            let mut reader_config =
                NdjsonReaderConfig::new(source_config.batch_size, source_config.compression);
            if coerce_objects {
                reader_config = reader_config.coerce_objects_to_strings();
            }
            let reader: Arc<dyn FileReader> = Arc::new(NdjsonReader::new(
                schema.clone(),
                reader_config,
                key.id().to_string(),
            ));
            readers.insert(source_name.clone(), reader);
        }
        readers
    }

    fn create_partition_extractor(config: &PipelineConfig) -> PartitionExtractor {
        let partition_columns = config
            .sink
            .partition_by
            .as_ref()
            .map(|p| p.partition_columns())
            .unwrap_or_default();
        PartitionExtractor::new(partition_columns)
    }

    async fn create_dlq(
        config: &PipelineConfig,
    ) -> Result<Option<Arc<DeadLetterQueue>>, PipelineError> {
        Ok(DeadLetterQueue::from_config(&config.error_handling)
            .await?
            .map(Arc::new))
    }

    fn create_failure_tracker(
        key: &PipelineKey,
        config: &PipelineConfig,
        dlq: Option<Arc<DeadLetterQueue>>,
    ) -> FailureTracker {
        FailureTracker::new(
            config.error_handling.max_failures,
            dlq,
            key.id().to_string(),
        )
    }
}

#[async_trait]
impl PollingProcessor for PipelineOrchestrator {
    type State = ();
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        if cold_start {
            self.multi_tracker.init_all(&self.config.sources).await?;
        }

        // Always proceed to process — discovery runs there and returns
        // NoItems if nothing is found.
        Ok(Some(()))
    }

    async fn process(&mut self, _state: Self::State) -> Result<IterationResult, Self::Error> {
        self.util_timer.stop_wait();

        // Take discovery snapshots from trackers before spawning
        let discovery_sources = self
            .multi_tracker
            .discovery_sources(&self.source_storages, &self.config.sources)?;

        // Spawn discovery task — streams files through a channel
        let discovery_task = DiscoveryTask::spawn(
            discovery_sources,
            self.shutdown.clone(),
            self.key.id().to_string(),
        );

        let iteration = Iteration::new(discovery_task, self)?;

        let (result, discovery_count) = iteration
            .run(
                &mut self.multi_tracker,
                &mut self.failure_tracker,
                self.shutdown.clone(),
                &mut self.util_timer,
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

        self.util_timer.start_wait();
        self.util_timer.maybe_update();

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
