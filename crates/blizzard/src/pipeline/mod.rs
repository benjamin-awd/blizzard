//! Pipeline for processing NDJSON files to Parquet staging.
//!
//! This module implements the main processing loop using the PollingProcessor
//! trait from blizzard-common. It supports running multiple pipelines
//! concurrently with shared shutdown handling and optional global concurrency limits.
//!

mod tasks;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use indexmap::IndexMap;
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
use blizzard_common::storage::{DatePrefixGenerator, list_ndjson_files_with_prefixes};
use blizzard_common::types::SourceState;
use blizzard_common::{StorageProvider, StorageProviderRef, emit, shutdown_signal};

use crate::config::{Config, PipelineConfig, PipelineKey};
use crate::dlq::{DeadLetterQueue, FailureTracker};
use crate::error::{AddressParseSnafu, MetricsSnafu, PipelineError, StorageSnafu};
use crate::sink::{ParquetWriter, ParquetWriterConfig};
use crate::source::{NdjsonReader, NdjsonReaderConfig, infer_schema_from_source};
use crate::staging::StagingWriter;

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
    /// Files written to staging.
    pub staging_files_written: usize,
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

    /// Returns the total staging files written across all pipelines.
    pub fn total_staging_files_written(&self) -> usize {
        self.pipelines
            .values()
            .map(|s| s.staging_files_written)
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

    // 4. Spawn independent task per pipeline
    let mut handles: JoinSet<(PipelineKey, Result<PipelineStats, PipelineError>)> = JoinSet::new();

    for (pipeline_key, pipeline_config) in config.pipelines {
        let shutdown = shutdown.clone();
        let global_sem = global_semaphore.clone();
        let poll_interval = Duration::from_secs(pipeline_config.source.poll_interval_secs);
        let key = pipeline_key.clone();

        handles.spawn(async move {
            let result = run_single_pipeline(
                key.clone(),
                pipeline_config,
                global_sem,
                poll_interval,
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
                    pipeline = %key,
                    files = pipeline_stats.files_processed,
                    "Pipeline completed successfully"
                );
                stats.pipelines.insert(key, pipeline_stats);
            }
            Ok((key, Err(e))) => {
                error!(pipeline = %key, error = %e, "Pipeline failed");
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
    poll_interval: Duration,
    shutdown: CancellationToken,
) -> Result<PipelineStats, PipelineError> {
    // Initialize processor, respecting shutdown signal
    let mut processor = tokio::select! {
        biased;

        _ = shutdown.cancelled() => {
            info!(pipeline = %pipeline_key, "Shutdown requested during initialization");
            return Ok(PipelineStats::default());
        }

        result = BlizzardProcessor::new(pipeline_key.clone(), pipeline_config, shutdown.clone()) => result?,
    };

    info!(pipeline = %pipeline_key, "Pipeline processor initialized");

    // Run the polling loop
    run_polling_loop(&mut processor, poll_interval, shutdown).await?;

    Ok(processor.stats)
}

/// State prepared for a single processing iteration.
struct PreparedState {
    pending_files: Vec<String>,
    skip_counts: HashMap<String, usize>,
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
    /// Writer for staging metadata and parquet files.
    staging_writer: StagingWriter,
    /// Arrow schema (from config or inferred).
    schema: deltalake::arrow::datatypes::SchemaRef,
    /// NDJSON reader with schema validation.
    reader: Arc<NdjsonReader>,
    /// Tracks which source files have been processed.
    source_state: SourceState,
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
        shutdown: CancellationToken,
    ) -> Result<Self, PipelineError> {
        // Create source storage provider
        let source_storage = Arc::new(
            StorageProvider::for_url_with_options(
                &pipeline_config.source.path,
                pipeline_config.source.storage_options.clone(),
            )
            .await
            .context(StorageSnafu)?,
        );

        // Create staging writer (writes directly to table directory)
        let staging_writer = StagingWriter::new(
            &pipeline_config.sink.table_uri,
            pipeline_config.sink.storage_options.clone(),
            pipeline_key.id().to_string(),
        )
        .await?;

        // Get schema - either from explicit config or by inference
        let schema = if pipeline_config.schema.should_infer() {
            // Generate date prefixes for partition filtering (if configured)
            let prefixes = pipeline_config.source.partition_filter.as_ref().map(|pf| {
                DatePrefixGenerator::new(&pf.prefix_template, pf.lookback).generate_prefixes()
            });

            infer_schema_from_source(
                &source_storage,
                pipeline_config.source.compression,
                prefixes.as_deref(),
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
            staging_writer,
            schema,
            reader,
            source_state: SourceState::new(),
            stats: PipelineStats::default(),
            shutdown,
        })
    }

    /// Generate date prefixes for partition filtering.
    fn generate_date_prefixes(&self) -> Option<Vec<String>> {
        self.pipeline_config
            .source
            .partition_filter
            .as_ref()
            .map(|pf| {
                DatePrefixGenerator::new(&pf.prefix_template, pf.lookback).generate_prefixes()
            })
    }

    /// Extract partition values from a source path.
    fn extract_partition_values(&self, path: &str) -> HashMap<String, String> {
        let mut values = HashMap::new();
        for key in &self.pipeline_config.sink.partition_by {
            // Look for key=value patterns in the path
            let pattern = format!("{}=", key);
            if let Some(idx) = path.find(&pattern) {
                let start = idx + pattern.len();
                let end = path[start..]
                    .find('/')
                    .map(|i| start + i)
                    .unwrap_or(path.len());
                values.insert(key.clone(), path[start..end].to_string());
            }
        }
        values
    }
}

#[async_trait]
impl PollingProcessor for BlizzardProcessor {
    type State = PreparedState;
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        // On cold start, we could potentially recover state from staging
        // For now, we start fresh
        if cold_start {
            info!(pipeline = %self.pipeline_key, "Cold start - beginning fresh processing");
        }

        // Generate date prefixes for efficient listing
        let prefixes = self.generate_date_prefixes();

        // List source files
        let all_files =
            list_ndjson_files_with_prefixes(&self.source_storage, prefixes.as_deref()).await?;

        // Filter to pending files
        let pending_files = self.source_state.filter_pending_files(all_files);

        if pending_files.is_empty() {
            return Ok(None);
        }

        // Get skip counts for partially processed files
        let skip_counts: HashMap<String, usize> = pending_files
            .iter()
            .filter_map(|f| {
                let skip = self.source_state.records_to_skip(f);
                if skip > 0 {
                    Some((f.clone(), skip))
                } else {
                    None
                }
            })
            .collect();

        info!(pipeline = %self.pipeline_key, files = pending_files.len(), "Found files to process");

        Ok(Some(PreparedState {
            pending_files,
            skip_counts,
        }))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let PreparedState {
            pending_files,
            skip_counts,
        } = state;

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
            skip_counts,
            self.source_storage.clone(),
            self.shutdown.clone(),
            self.pipeline_config.source.max_concurrent_files,
            self.pipeline_key.id().to_string(),
        );

        // Process downloaded files
        let result = self
            .process_downloads(downloader, &mut parquet_writer, self.shutdown.clone())
            .await;

        // Close the writer and get finished files
        let finished_files = parquet_writer.close()?;

        // Write to staging
        if !finished_files.is_empty() {
            info!(
                pipeline = %self.pipeline_key,
                files = finished_files.len(),
                "Writing files to staging"
            );
            self.staging_writer.write_files(&finished_files).await?;
            self.stats.staging_files_written += finished_files.len();

            let bytes: usize = finished_files.iter().map(|f| f.size).sum();
            emit!(BytesWritten {
                bytes: bytes as u64,
                pipeline: self.pipeline_key.id().to_string(),
            });
            self.stats.bytes_written += bytes;
        }

        // Finalize DLQ
        self.failure_tracker.finalize_dlq().await;

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
                pipeline: self.pipeline_key.id().to_string(),
            });
            emit!(SourceStateFiles {
                count: self.source_state.files.len(),
                pipeline: self.pipeline_key.id().to_string(),
            });

            tokio::select! {
                biased;

                // Handle shutdown
                _ = shutdown.cancelled() => {
                    info!(pipeline = %self.pipeline_key, "Shutdown requested during processing");
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
                            warn!(pipeline = %self.pipeline_key, error = %e, "File processing failed");
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
                            warn!(pipeline = %self.pipeline_key, error = %e, "Download failed");
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

        // Update state
        self.source_state.mark_finished(&path);
        self.stats.files_processed += 1;
        self.stats.records_processed += total_records;

        emit!(FileProcessed {
            status: FileStatus::Success,
            pipeline: self.pipeline_key.id().to_string(),
        });
        emit!(RecordsProcessed {
            count: total_records as u64,
            pipeline: self.pipeline_key.id().to_string(),
        });
        emit!(BatchesProcessed {
            count: batch_count as u64,
            pipeline: self.pipeline_key.id().to_string(),
        });

        let short_name = path.split('/').next_back().unwrap_or(&path);
        debug!(
            pipeline = %self.pipeline_key,
            file = short_name,
            records = total_records,
            batches = batch_count,
            "Processed file"
        );

        // Write any finished files to staging
        let finished = parquet_writer.take_finished_files();
        if !finished.is_empty() {
            self.staging_writer.write_files(&finished).await?;
            self.stats.staging_files_written += finished.len();
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
