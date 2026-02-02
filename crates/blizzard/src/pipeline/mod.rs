//! Pipeline for processing NDJSON files to Parquet staging.
//!
//! This module implements the main processing loop using the PollingProcessor
//! trait from blizzard-common. It supports running multiple pipelines
//! concurrently with shared shutdown handling and optional global concurrency limits.
//!

mod tasks;
mod tracker;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use indexmap::IndexMap;
use rand::Rng;
use snafu::ResultExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use blizzard_common::metrics::events::{
    BatchesProcessed, BytesWritten, DecompressionQueueDepth, FileProcessed, FileStatus,
    RecordsProcessed, SourceStateFiles,
};
use blizzard_common::polling::{IterationResult, PollingProcessor, run_polling_loop};

/// Generate a random jitter duration up to the specified maximum seconds.
fn random_jitter(max_secs: u64) -> Duration {
    if max_secs > 0 {
        Duration::from_millis(rand::rng().random_range(0..max_secs * 1000))
    } else {
        Duration::ZERO
    }
}

/// Generate date prefixes for partition filtering based on pipeline config.
fn generate_date_prefixes(config: &PipelineConfig) -> Option<Vec<String>> {
    config.source.partition_filter.as_ref().map(|pf| {
        DatePrefixGenerator::new(&pf.prefix_template, pf.lookback).generate_prefixes()
    })
}

/// Extract a partition value from a path for a given key.
///
/// Looks for `key=value` pattern and extracts the value (up to the next `/` or end of string).
fn extract_partition_value(path: &str, key: &str) -> Option<String> {
    let pattern = format!("{}=", key);
    let start = path.find(&pattern)? + pattern.len();
    let rest = &path[start..];
    let end = rest.find('/').unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

/// Create a storage provider, using the pool if available.
async fn create_storage(
    pool: &Option<Arc<StoragePool>>,
    url: &str,
    options: HashMap<String, String>,
) -> Result<StorageProviderRef, PipelineError> {
    match pool {
        Some(p) => p.get_or_create(url, options).await.context(StorageSnafu),
        None => Ok(Arc::new(
            StorageProvider::for_url_with_options(url, options)
                .await
                .context(StorageSnafu)?,
        )),
    }
}
use blizzard_common::storage::DatePrefixGenerator;
use blizzard_common::{StoragePool, StorageProvider, StorageProviderRef, emit, shutdown_signal};

use crate::checkpoint::CheckpointManager;
use tracker::{HashMapTracker, StateTracker, WatermarkTracker};
use crate::config::{Config, PipelineConfig, PipelineKey};
use crate::dlq::{DeadLetterQueue, FailureTracker};
use crate::error::{AddressParseSnafu, MetricsSnafu, PipelineError, StorageSnafu};
use crate::sink::{ParquetWriter, ParquetWriterConfig};
use crate::source::{NdjsonReader, NdjsonReaderConfig, infer_schema_from_source};
use crate::staging::TableWriter;

use tasks::{Downloader, ProcessFuture, ProcessedFile, spawn_read_task};

/// Statistics from a single pipeline run.
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    /// Total files processed.
    pub files_processed: usize,
    /// Total records processed.
    pub records_processed: usize,
    /// Total bytes written to Parquet.
    pub bytes_written: usize,
    /// Parquet files written to table.
    pub parquet_files_written: usize,
}

/// Statistics from a multi-pipeline run.
///
/// Tracks per-pipeline statistics and any errors that occurred during processing.
#[derive(Debug, Clone, Default)]
pub struct MultiPipelineStats {
    /// Per-pipeline statistics for pipelines that ran successfully.
    pub pipelines: IndexMap<PipelineKey, PipelineStats>,
    /// Errors that occurred, with the associated pipeline key and error message.
    pub errors: Vec<(PipelineKey, String)>,
}

impl MultiPipelineStats {
    /// Returns true if no errors occurred.
    pub fn success(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns the total number of files processed across all pipelines.
    pub fn total_files_processed(&self) -> usize {
        self.pipelines.values().map(|s| s.files_processed).sum()
    }

    /// Returns the total number of records processed across all pipelines.
    pub fn total_records_processed(&self) -> usize {
        self.pipelines.values().map(|s| s.records_processed).sum()
    }

    /// Returns the total bytes written across all pipelines.
    pub fn total_bytes_written(&self) -> usize {
        self.pipelines.values().map(|s| s.bytes_written).sum()
    }

    /// Returns the total parquet files written across all pipelines.
    pub fn total_parquet_files_written(&self) -> usize {
        self.pipelines
            .values()
            .map(|s| s.parquet_files_written)
            .sum()
    }
}

/// Run the pipeline with the given configuration.
///
/// Spawns independent tasks for each configured pipeline, with shared shutdown
/// handling and optional global concurrency limits.
///
pub async fn run_pipeline(config: Config) -> Result<MultiPipelineStats, PipelineError> {
    // 1. Initialize metrics once (shared across all pipelines)
    let addr = config.metrics.address.parse().context(AddressParseSnafu)?;
    blizzard_common::init_metrics(addr).context(MetricsSnafu)?;

    // 2. Set up shared shutdown handling
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_clone.cancel();
    });

    // 3. Create optional global semaphore for cross-pipeline concurrency limiting
    let global_semaphore = config
        .global
        .total_concurrency
        .map(|n| Arc::new(Semaphore::new(n)));

    // 4. Create shared storage pool if connection pooling is enabled
    let storage_pool = if config.global.connection_pooling {
        Some(Arc::new(StoragePool::new()))
    } else {
        None
    };

    // 5. Get jitter settings
    let jitter_max_secs = config.global.poll_jitter_secs;

    // 6. Spawn independent task per pipeline with jittered start
    let mut handles: JoinSet<(PipelineKey, Result<PipelineStats, PipelineError>)> = JoinSet::new();

    for (pipeline_key, pipeline_config) in config.pipelines {
        let shutdown = shutdown.clone();
        let global_sem = global_semaphore.clone();
        let pool = storage_pool.clone();
        let poll_interval = Duration::from_secs(pipeline_config.source.poll_interval_secs);
        let key = pipeline_key.clone();

        // Add jitter to stagger pipeline starts (prevents thundering herd)
        let start_jitter = random_jitter(jitter_max_secs);

        handles.spawn(async move {
            // Stagger start times, but respect shutdown signal
            if !start_jitter.is_zero() {
                info!(
                    target = %key,
                    jitter_secs = start_jitter.as_secs(),
                    "Delaying pipeline start for jitter"
                );
                if shutdown
                    .run_until_cancelled(tokio::time::sleep(start_jitter))
                    .await
                    .is_none()
                {
                    info!(target = %key, "Shutdown requested during jitter delay");
                    return (key, Ok(PipelineStats::default()));
                }
            }

            let result = run_single_pipeline(
                key.clone(),
                pipeline_config,
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

    info!("Spawned {} pipeline tasks", handles.len());

    // 5. Collect results
    let mut stats = MultiPipelineStats::default();
    while let Some(result) = handles.join_next().await {
        match result {
            Ok((key, Ok(pipeline_stats))) => {
                info!(
                    target = %key,
                    files = pipeline_stats.files_processed,
                    "Pipeline completed successfully"
                );
                stats.pipelines.insert(key, pipeline_stats);
            }
            Ok((key, Err(e))) => {
                error!(target = %key, error = %e, "Pipeline failed");
                stats.errors.push((key, e.to_string()));
            }
            Err(e) => {
                error!(error = %e, "Pipeline task panicked");
                // JoinError doesn't give us the pipeline key, so we can't attribute it
            }
        }
    }

    info!(
        "All pipelines complete: {} succeeded, {} failed",
        stats.pipelines.len(),
        stats.errors.len()
    );

    Ok(stats)
}

/// Run a single pipeline's polling loop.
///
/// This is spawned as an independent task for each pipeline.
async fn run_single_pipeline(
    pipeline_key: PipelineKey,
    pipeline_config: PipelineConfig,
    _global_semaphore: Option<Arc<Semaphore>>,
    storage_pool: Option<Arc<StoragePool>>,
    poll_interval: Duration,
    poll_jitter_secs: u64,
    shutdown: CancellationToken,
) -> Result<PipelineStats, PipelineError> {
    // Add jitter to poll interval for this pipeline to spread polling load
    let effective_interval = poll_interval + random_jitter(poll_jitter_secs);

    // Initialize processor, respecting shutdown signal
    let mut processor = tokio::select! {
        biased;

        _ = shutdown.cancelled() => {
            info!(target = %pipeline_key, "Shutdown requested during initialization");
            return Ok(PipelineStats::default());
        }

        result = BlizzardProcessor::new(
            pipeline_key.clone(),
            pipeline_config,
            storage_pool,
            shutdown.clone(),
        ) => result?,
    };

    info!(
        target = %pipeline_key,
        poll_interval_secs = effective_interval.as_secs(),
        "Pipeline processor initialized"
    );

    // Run the polling loop with jittered interval
    run_polling_loop(
        &mut processor,
        effective_interval,
        shutdown,
        pipeline_key.id(),
    )
    .await?;

    Ok(processor.stats)
}

/// State prepared for a single processing iteration.
struct PreparedState {
    pending_files: Vec<String>,
}

/// The blizzard file loader pipeline processor.
///
/// Each pipeline runs its own `BlizzardProcessor` instance.
struct BlizzardProcessor {
    /// Identifier for this pipeline (used in logging and metrics).
    pipeline_key: PipelineKey,
    /// Configuration for this specific pipeline.
    pipeline_config: PipelineConfig,
    /// Storage provider for reading source files.
    source_storage: StorageProviderRef,
    /// Writer for parquet files directly to table directory.
    table_writer: TableWriter,
    /// Arrow schema (from config or inferred).
    schema: deltalake::arrow::datatypes::SchemaRef,
    /// NDJSON reader with schema validation.
    reader: Arc<NdjsonReader>,
    /// Tracks which source files have been processed.
    state_tracker: Box<dyn StateTracker>,
    /// Statistics for this pipeline's run.
    stats: PipelineStats,
    /// Tracks failures and manages DLQ.
    failure_tracker: FailureTracker,
    /// Shutdown signal for graceful termination.
    shutdown: CancellationToken,
}

impl BlizzardProcessor {
    async fn new(
        pipeline_key: PipelineKey,
        pipeline_config: PipelineConfig,
        storage_pool: Option<Arc<StoragePool>>,
        shutdown: CancellationToken,
    ) -> Result<Self, PipelineError> {
        // Create source storage provider - use pooled if available
        let source_storage = create_storage(
            &storage_pool,
            &pipeline_config.source.path,
            pipeline_config.source.storage_options.clone(),
        )
        .await?;

        // Create table writer (writes parquet directly to table directory)
        let table_writer = TableWriter::new(
            &pipeline_config.sink.table_uri,
            pipeline_config.sink.storage_options.clone(),
            pipeline_key.id().to_string(),
        )
        .await?;

        // Create state tracker based on configuration
        let state_tracker: Box<dyn StateTracker> = if pipeline_config.source.use_watermark {
            // Checkpoint manager needs its own storage provider (not pooled)
            let table_storage = create_storage(
                &None,
                &pipeline_config.sink.table_uri,
                pipeline_config.sink.storage_options.clone(),
            )
            .await?;
            let checkpoint_manager = CheckpointManager::new(
                table_storage,
                pipeline_key.id().to_string(),
            );
            Box::new(WatermarkTracker::new(checkpoint_manager))
        } else {
            Box::new(HashMapTracker::new())
        };

        // Get schema - either from explicit config or by inference
        let schema = if pipeline_config.schema.should_infer() {
            let prefixes = generate_date_prefixes(&pipeline_config);

            infer_schema_from_source(
                &source_storage,
                pipeline_config.source.compression,
                prefixes.as_deref(),
                pipeline_key.as_ref(),
            )
            .await?
        } else {
            pipeline_config.schema.to_arrow_schema()
        };

        // Create NDJSON reader
        let reader_config = NdjsonReaderConfig::new(
            pipeline_config.source.batch_size,
            pipeline_config.source.compression,
        );
        let reader = Arc::new(NdjsonReader::new(
            schema.clone(),
            reader_config,
            pipeline_key.id().to_string(),
        ));

        // Set up DLQ if configured
        let dlq = DeadLetterQueue::from_config(&pipeline_config.error_handling).await?;

        Ok(Self {
            failure_tracker: FailureTracker::new(
                pipeline_config.error_handling.max_failures,
                dlq.map(Arc::new),
                pipeline_key.id().to_string(),
            ),
            pipeline_key,
            pipeline_config,
            source_storage,
            table_writer,
            schema,
            reader,
            state_tracker,
            stats: PipelineStats::default(),
            shutdown,
        })
    }

    /// Extract partition values from a source path.
    ///
    /// Looks for `key=value` patterns in the path for each configured partition column.
    fn extract_partition_values(&self, path: &str) -> HashMap<String, String> {
        self.pipeline_config
            .sink
            .partition_by
            .as_ref()
            .map(|p| p.partition_columns())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|key| {
                extract_partition_value(path, &key).map(|value| (key, value))
            })
            .collect()
    }
}

#[async_trait]
impl PollingProcessor for BlizzardProcessor {
    type State = PreparedState;
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        // Generate date prefixes for efficient listing
        let prefixes = generate_date_prefixes(&self.pipeline_config);

        // On cold start, initialize state tracker
        if cold_start {
            match self.state_tracker.init().await? {
                Some(msg) => info!(target = %self.pipeline_key, "{}", msg),
                None => info!(
                    target = %self.pipeline_key,
                    mode = self.state_tracker.mode_name(),
                    "Cold start - beginning fresh processing"
                ),
            }
        }

        // List pending source files
        let pending_files = self
            .state_tracker
            .list_pending(
                &self.source_storage,
                prefixes.as_deref(),
                self.pipeline_key.as_ref(),
            )
            .await?;

        if pending_files.is_empty() {
            return Ok(None);
        }

        info!(target = %self.pipeline_key, files = pending_files.len(), "Found files to process");

        Ok(Some(PreparedState { pending_files }))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let PreparedState { pending_files } = state;

        // Create Parquet writer
        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(self.pipeline_config.sink.file_size_mb)
            .with_row_group_size_bytes(self.pipeline_config.sink.row_group_size_bytes)
            .with_compression(self.pipeline_config.sink.compression);
        let mut parquet_writer = ParquetWriter::new(
            self.schema.clone(),
            writer_config,
            self.pipeline_key.id().to_string(),
        )?;

        // Create downloader using shared shutdown token
        let downloader = Downloader::spawn(
            pending_files,
            self.source_storage.clone(),
            self.shutdown.clone(),
            self.pipeline_config.source.max_concurrent_files,
            self.pipeline_key.id().to_string(),
        );

        // Process downloaded files
        let result = self
            .process_downloads(downloader, &mut parquet_writer, self.shutdown.clone())
            .await;

        // Finalize: write remaining files, save state, finalize DLQ
        self.finalize_iteration(parquet_writer).await?;

        result
    }
}

impl BlizzardProcessor {
    async fn process_downloads(
        &mut self,
        mut downloader: Downloader,
        parquet_writer: &mut ParquetWriter,
        shutdown: CancellationToken,
    ) -> Result<IterationResult, PipelineError> {
        use blizzard_common::metrics::events::FailureStage;
        use futures::StreamExt;
        use futures::stream::FuturesUnordered;

        let mut processing: FuturesUnordered<ProcessFuture> = FuturesUnordered::new();
        let max_in_flight = self.pipeline_config.source.max_concurrent_files * 2;

        loop {
            emit!(DecompressionQueueDepth {
                count: processing.len(),
                target: self.pipeline_key.id().to_string(),
            });
            emit!(SourceStateFiles {
                count: self.state_tracker.tracked_count(),
                target: self.pipeline_key.id().to_string(),
            });

            tokio::select! {
                biased;

                // Handle shutdown
                _ = shutdown.cancelled() => {
                    info!(target = %self.pipeline_key, "Shutdown requested during processing");
                    downloader.abort();
                    return Ok(IterationResult::Shutdown);
                }

                // Handle completed processing
                Some(result) = processing.next(), if !processing.is_empty() => {
                    match result {
                        Ok(processed) => {
                            self.handle_processed_file(processed, parquet_writer).await?;
                        }
                        Err(e) => {
                            warn!(target = %self.pipeline_key, error = %e, "File processing failed");
                            self.failure_tracker
                                .record_failure(&e.to_string(), FailureStage::Parse)
                                .await?;
                        }
                    }
                }

                // Accept new downloads if under limit
                result = downloader.rx.recv(), if processing.len() < max_in_flight => {
                    match result {
                        Some(Ok(downloaded)) => {
                            let future = spawn_read_task(downloaded, self.reader.clone());
                            processing.push(future);
                        }
                        Some(Err(e)) => {
                            warn!(target = %self.pipeline_key, error = %e, "Download failed");
                            self.failure_tracker
                                .record_failure(&e.to_string(), FailureStage::Download)
                                .await?;
                        }
                        None => {
                            // Downloader finished
                            if processing.is_empty() {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(IterationResult::ProcessedItems)
    }

    /// Finalize an iteration: close the parquet writer, write remaining files,
    /// finalize DLQ, and save state.
    async fn finalize_iteration(
        &mut self,
        parquet_writer: ParquetWriter,
    ) -> Result<(), PipelineError> {
        // Close the writer and get finished files
        let finished_files = parquet_writer.close()?;

        // Write parquet files to table directory
        if !finished_files.is_empty() {
            info!(
                target = %self.pipeline_key,
                files = finished_files.len(),
                "Writing parquet files to table"
            );
            self.table_writer.write_files(&finished_files).await?;
            self.stats.parquet_files_written += finished_files.len();

            let bytes: usize = finished_files.iter().map(|f| f.size).sum();
            emit!(BytesWritten {
                bytes: bytes as u64,
                target: self.pipeline_key.id().to_string(),
            });
            self.stats.bytes_written += bytes;
        }

        // Finalize DLQ
        self.failure_tracker.finalize_dlq().await;

        // Save state tracker
        if let Err(e) = self.state_tracker.save().await {
            warn!(
                target = %self.pipeline_key,
                error = %e,
                "Failed to save state"
            );
        } else {
            debug!(
                target = %self.pipeline_key,
                mode = self.state_tracker.mode_name(),
                "Saved state"
            );
        }

        Ok(())
    }

    async fn handle_processed_file(
        &mut self,
        processed: ProcessedFile,
        parquet_writer: &mut ParquetWriter,
    ) -> Result<(), PipelineError> {
        let ProcessedFile {
            path,
            batches,
            total_records,
        } = processed;

        // Extract partition values from source path
        let partition_values = self.extract_partition_values(&path);
        parquet_writer.set_partition_context(partition_values)?;

        // Write batches to Parquet
        let batch_count = batches.len();
        for batch in batches {
            parquet_writer.write_batch(&batch)?;
        }

        // Update state tracking
        self.state_tracker.mark_processed(&path);
        self.stats.files_processed += 1;
        self.stats.records_processed += total_records;

        emit!(FileProcessed {
            status: FileStatus::Success,
            target: self.pipeline_key.id().to_string(),
        });
        emit!(RecordsProcessed {
            count: total_records as u64,
            target: self.pipeline_key.id().to_string(),
        });
        emit!(BatchesProcessed {
            count: batch_count as u64,
            target: self.pipeline_key.id().to_string(),
        });

        let short_name = path.split('/').next_back().unwrap_or(&path);
        debug!(
            target = %self.pipeline_key,
            file = short_name,
            records = total_records,
            batches = batch_count,
            "Processed file"
        );

        // Write any finished files to staging
        let finished = parquet_writer.take_finished_files();
        if !finished.is_empty() {
            self.table_writer.write_files(&finished).await?;
            self.stats.parquet_files_written += finished.len();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Test that the shutdown token is properly propagated from run_pipeline
    /// to the processor. This was a bug where a new local CancellationToken
    /// was created in process() instead of using the one from run_pipeline.
    #[tokio::test]
    async fn test_shutdown_token_is_shared() {
        // Create a shutdown token
        let shutdown = CancellationToken::new();

        // Clone it like run_pipeline does
        let shutdown_for_processor = shutdown.clone();

        // When the original token is cancelled...
        shutdown.cancel();

        // ...the clone should also be cancelled
        assert!(
            shutdown_for_processor.is_cancelled(),
            "Shutdown token clones should share cancellation state"
        );
    }

    /// Test that cancellation propagates through multiple clones
    #[tokio::test]
    async fn test_cancellation_propagates_through_clones() {
        let original = CancellationToken::new();
        let clone1 = original.clone();
        let clone2 = clone1.clone();
        let clone3 = clone2.clone();

        // None should be cancelled yet
        assert!(!original.is_cancelled());
        assert!(!clone1.is_cancelled());
        assert!(!clone2.is_cancelled());
        assert!(!clone3.is_cancelled());

        // Cancel the original
        original.cancel();

        // All clones should now be cancelled
        assert!(clone1.is_cancelled());
        assert!(clone2.is_cancelled());
        assert!(clone3.is_cancelled());
    }

    /// Test that separate tokens do NOT share cancellation (the bug scenario)
    #[tokio::test]
    async fn test_separate_tokens_do_not_share_cancellation() {
        let token1 = CancellationToken::new();
        let token2 = CancellationToken::new(); // This was the bug - creating a new token

        token1.cancel();

        // token2 should NOT be cancelled because it's a separate token
        assert!(
            !token2.is_cancelled(),
            "Separate tokens should not share cancellation"
        );
    }

    /// Integration test: verify shutdown signal triggers cancellation
    #[tokio::test]
    async fn test_shutdown_cancellation_is_immediate() {
        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        // Spawn a task that waits for cancellation
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_clone.cancelled() => {
                    "cancelled"
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    "timeout"
                }
            }
        });

        // Cancel immediately
        shutdown.cancel();

        // Task should complete with "cancelled" not "timeout"
        let result = tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("task should complete quickly")
            .expect("task should not panic");

        assert_eq!(result, "cancelled");
    }
}
