//! Main processing pipeline.
//!
//! Connects the source, sink, and checkpoint components into a
//! streaming pipeline with backpressure and graceful shutdown.
//!
//! # Architecture
//!
//! Uses a producer-consumer pattern to separate I/O from CPU work:
//! - **Tokio tasks**: Download compressed files concurrently (I/O bound)
//! - **Tokio's blocking thread pool**: Decompress and parse files in parallel (CPU bound)
//!
//! This enables full CPU utilization during gzip decompression.
//!
//! # Atomic Checkpointing
//!
//! Checkpoints are stored atomically in Delta Lake using `Txn` actions.
//! Each Delta commit includes the checkpoint state, ensuring file commits
//! and checkpoint state are always consistent.

mod tasks;

use deltalake::arrow::array::RecordBatch;
use futures::stream::{FuturesUnordered, StreamExt};
use snafu::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::checkpoint::CheckpointCoordinator;
use crate::config::{CompressionFormat, Config, MB};
use crate::dlq::DeadLetterQueue;
use crate::emit;
use crate::error::{
    DeltaSnafu, DlqSnafu, MaxFailuresExceededSnafu, ParquetSnafu, PipelineError,
    PipelineStorageSnafu, ReaderSnafu, TaskJoinSnafu,
};
use crate::metrics::UtilizationTimer;
use crate::metrics::events::{
    BatchesProcessed, BytesWritten, DecompressionQueueDepth, FailureStage, FileFailed,
    FileProcessed, FileStatus, PendingBatches, RecordsProcessed,
};
use crate::sink::FinishedFile;
use crate::sink::delta::DeltaSink;
use crate::sink::parquet::{ParquetWriter, ParquetWriterConfig, RollingPolicy};
use crate::source::{NdjsonReader, NdjsonReaderConfig};
use crate::storage::{StorageProvider, StorageProviderRef, list_ndjson_files};

use tasks::{DownloadedFile, UploaderConfig, run_downloader, run_uploader};

/// Future type for file processing operations.
type ProcessFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<ProcessedFile, PipelineError>> + Send>,
>;

/// Statistics about the pipeline run.
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    pub files_processed: usize,
    pub records_processed: usize,
    pub bytes_written: usize,
    pub parquet_files_written: usize,
    pub delta_commits: usize,
    pub checkpoints_saved: usize,
}

/// Result of processing a downloaded file.
struct ProcessedFile {
    path: String,
    batches: Vec<RecordBatch>,
    total_records: usize,
}

/// Result of a single processing iteration.
enum IterationResult {
    /// Files were processed successfully.
    ProcessedFiles,
    /// No files were available to process.
    NoFiles,
    /// Shutdown was requested.
    Shutdown,
}

/// Initialized pipeline state ready for processing.
struct InitializedState {
    pending_files: Vec<String>,
    source_state: crate::source::SourceState,
    schema: Arc<deltalake::arrow::datatypes::Schema>,
    writer: ParquetWriter,
    delta_sink: DeltaSink,
    max_concurrent: usize,
    compression: CompressionFormat,
    batch_size: usize,
    dlq: Option<Arc<DeadLetterQueue>>,
}

/// Main processing pipeline.
pub struct Pipeline {
    config: Config,
    source_storage: StorageProviderRef,
    sink_storage: StorageProviderRef,
    checkpoint_coordinator: Arc<CheckpointCoordinator>,
    stats: PipelineStats,
    shutdown: CancellationToken,
}

impl Pipeline {
    /// Create a new pipeline from configuration.
    pub async fn new(config: Config, shutdown: CancellationToken) -> Result<Self, PipelineError> {
        // Create storage providers
        let source_storage = Arc::new(
            StorageProvider::for_url_with_options(
                &config.source.path,
                config.source.storage_options.clone(),
            )
            .await
            .context(PipelineStorageSnafu)?,
        );

        let sink_storage = Arc::new(
            StorageProvider::for_url_with_options(
                &config.sink.path,
                config.sink.storage_options.clone(),
            )
            .await
            .context(PipelineStorageSnafu)?,
        );

        let checkpoint_coordinator = Arc::new(CheckpointCoordinator::new());

        Ok(Self {
            config,
            source_storage,
            sink_storage,
            checkpoint_coordinator,
            stats: PipelineStats::default(),
            shutdown,
        })
    }

    /// Run the pipeline.
    ///
    /// Uses a producer-consumer pattern to maximize parallelism:
    /// - **Producer**: Tokio tasks download compressed files concurrently (I/O bound)
    /// - **Consumer**: Tokio's blocking thread pool decompresses and parses files (CPU bound)
    ///
    /// The pipeline continuously polls for new files at the configured interval.
    pub async fn run(&mut self) -> Result<PipelineStats, PipelineError> {
        info!("Starting pipeline");

        let poll_interval = Duration::from_secs(self.config.source.poll_interval_secs);
        let mut first_iteration = true;

        loop {
            // Race initialization against shutdown signal
            let shutdown = self.shutdown.clone();
            let state = tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    info!("Shutdown requested during initialization");
                    return Ok(self.stats.clone());
                }

                result = async {
                    if first_iteration {
                        first_iteration = false;
                        self.prepare_first_iteration().await
                    } else {
                        self.prepare_next_iteration().await
                    }
                } => result?,
            };

            let result = match state {
                Some(s) => self.process_files(s).await?,
                None => {
                    info!("No files to process");
                    IterationResult::NoFiles
                }
            };

            // Exit on shutdown, otherwise wait and poll again
            match result {
                IterationResult::Shutdown => break,
                IterationResult::NoFiles => {
                    info!(
                        "No new files, waiting {}s before next poll",
                        poll_interval.as_secs()
                    );
                }
                IterationResult::ProcessedFiles => {
                    info!(
                        "Iteration complete, waiting {}s before next poll",
                        poll_interval.as_secs()
                    );
                }
            }

            // Wait for poll interval or shutdown
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    info!("Shutdown requested during poll wait");
                    break;
                }
                _ = tokio::time::sleep(poll_interval) => {}
            }
        }

        info!("Pipeline completed: {:?}", self.stats);
        Ok(self.stats.clone())
    }

    /// Process files in a single iteration.
    ///
    /// This contains the main processing loop: downloading, decompressing,
    /// parsing, writing, and uploading files.
    async fn process_files(
        &mut self,
        state: InitializedState,
    ) -> Result<IterationResult, PipelineError> {
        // Unpack initialized state
        let InitializedState {
            pending_files,
            source_state,
            schema,
            mut writer,
            delta_sink,
            max_concurrent,
            compression,
            batch_size,
            dlq,
        } = state;

        // Track failure count for max_failures limit
        let mut failure_count = 0usize;
        let max_failures = self.config.error_handling.max_failures;

        // Create the NDJSON reader for decompression and parsing
        let reader_config = NdjsonReaderConfig::new(batch_size, compression);
        let reader = Arc::new(NdjsonReader::new(schema.clone(), reader_config));

        // Channel for downloaded files ready for CPU processing
        // Buffer size matches max_concurrent to allow downloads to stay ahead
        let (download_tx, mut download_rx) =
            mpsc::channel::<Result<DownloadedFile, crate::error::StorageError>>(max_concurrent);

        // Channel for finished parquet files ready for upload
        // Larger buffer to allow more queuing and avoid blocking the writer
        let max_concurrent_uploads = self.config.sink.max_concurrent_uploads;
        let buffer_size = max_concurrent_uploads * 4;
        let (upload_tx, upload_rx) = mpsc::channel::<FinishedFile>(buffer_size);

        // Spawn background uploader task with concurrent file uploads
        let sink_storage = self.sink_storage.clone();
        let upload_shutdown = self.shutdown.clone();
        let checkpoint_coordinator = self.checkpoint_coordinator.clone();
        let uploader_config = UploaderConfig {
            part_size: self.config.sink.part_size_mb * MB,
            min_multipart_size: self.config.sink.min_multipart_size_mb * MB,
            max_concurrent_uploads,
            max_concurrent_parts: self.config.sink.max_concurrent_parts,
        };

        let upload_handle = tokio::spawn(run_uploader(
            upload_rx,
            delta_sink,
            sink_storage,
            checkpoint_coordinator,
            upload_shutdown,
            uploader_config,
            dlq.clone(),
        ));

        // Spawn download tasks concurrently using FuturesUnordered
        let storage = self.source_storage.clone();
        let source_state_clone = source_state.clone();
        let pending_files_clone = pending_files.clone();
        let shutdown = self.shutdown.clone();

        let download_handle = tokio::spawn(run_downloader(
            pending_files_clone,
            source_state_clone,
            storage,
            download_tx,
            shutdown,
            max_concurrent,
        ));

        // Consumer: Process downloaded files with parallel decompression
        // Use FuturesUnordered to process multiple files concurrently
        let mut files_remaining = pending_files.len();
        let mut processing: FuturesUnordered<ProcessFuture> = FuturesUnordered::new();
        let mut channel_open = true;
        let mut util_timer = UtilizationTimer::new("processor");
        let mut shutdown_requested = false;

        // Helper to spawn a read task (decompress + parse)
        let spawn_read = |downloaded: DownloadedFile, reader: Arc<NdjsonReader>| {
            let path = downloaded.path.clone();
            let path_for_result = path.clone();
            let task: ProcessFuture = Box::pin(async move {
                let result = tokio::task::spawn_blocking(move || {
                    reader.read(downloaded.compressed_data, downloaded.skip_records, &path)
                })
                .await
                .context(TaskJoinSnafu)?
                .context(ReaderSnafu)?;
                Ok(ProcessedFile {
                    path: path_for_result,
                    batches: result.batches,
                    total_records: result.total_records,
                })
            });
            task
        };

        loop {
            // Update utilization state: waiting if no processing tasks
            if processing.is_empty() {
                util_timer.start_wait();
            }
            util_timer.maybe_update();

            if self.shutdown.is_cancelled() {
                info!("Shutdown requested, stopping processing");
                shutdown_requested = true;
                // Note: checkpoint state will be committed with final files
                break;
            }

            // If no processing tasks and channel closed, we're done
            if processing.is_empty() && !channel_open {
                break;
            }

            // Use select! to simultaneously:
            // 1. Receive new downloads (if we have capacity)
            // 2. Wait for processing tasks to complete
            let has_capacity = processing.len() < max_concurrent && channel_open;

            tokio::select! {
                // Only poll for new downloads if we have processing capacity
                result = download_rx.recv(), if has_capacity => {
                    match result {
                        Some(Ok(downloaded)) => {
                            // Transition to working state when we have processing tasks
                            if processing.is_empty() {
                                util_timer.stop_wait();
                            }
                            processing.push(spawn_read(downloaded, reader.clone()));
                            emit!(DecompressionQueueDepth {
                                count: processing.len()
                            });
                        }
                        Some(Err(e)) => {
                            if e.is_not_found() {
                                warn!("Skipping file with download error (not found): {}", e);
                                files_remaining -= 1;
                                emit!(FileProcessed { status: FileStatus::Skipped });
                            } else {
                                // Record failure and continue processing
                                let error_str = e.to_string();
                                warn!("Skipping failed file (download error): {}", e);
                                files_remaining -= 1;
                                failure_count += 1;
                                emit!(FileProcessed { status: FileStatus::Failed });
                                emit!(FileFailed { stage: FailureStage::Download });

                                // Record to DLQ if configured
                                if let Some(dlq) = &dlq {
                                    dlq.record_failure("unknown", &error_str, FailureStage::Download).await;
                                }

                                // Check max_failures limit
                                if max_failures > 0 && failure_count >= max_failures {
                                    error!("Max failures ({}) reached, stopping pipeline", failure_count);
                                    // Finalize DLQ before returning
                                    if let Some(dlq) = &dlq
                                        && let Err(dlq_err) = dlq.finalize().await
                                    {
                                        error!("Failed to finalize DLQ: {}", dlq_err);
                                    }
                                    return MaxFailuresExceededSnafu { count: failure_count }.fail();
                                }
                            }
                        }
                        None => {
                            channel_open = false;
                        }
                    }
                }

                // Wait for processing tasks to complete
                result = processing.next(), if !processing.is_empty() => {
                    if let Some(result) = result {
                        emit!(DecompressionQueueDepth {
                            count: processing.len()
                        });
                        match result {
                            Ok(processed) => {
                                let short_name = processed.path.split('/').next_back().unwrap_or(&processed.path);

                                // Track pending batches before writing
                                emit!(PendingBatches {
                                    count: processed.batches.len()
                                });

                                // Write all batches from this file
                                for batch in &processed.batches {
                                    debug!("[batch] {}: {} rows", short_name, batch.num_rows());
                                    writer.write_batch(batch).context(ParquetSnafu)?;
                                    self.stats.records_processed += batch.num_rows();
                                    emit!(RecordsProcessed { count: batch.num_rows() as u64 });
                                    emit!(BatchesProcessed { count: 1 });
                                }
                                emit!(PendingBatches { count: 0 });

                                // Update checkpoint state
                                self.checkpoint_coordinator
                                    .update_source_state(&processed.path, processed.total_records, true)
                                    .await;

                                self.stats.files_processed += 1;
                                files_remaining -= 1;
                                emit!(FileProcessed { status: FileStatus::Success });
                                debug!(
                                    "[-] Finished file (remaining: {}): {} ({} records)",
                                    files_remaining, short_name, processed.total_records
                                );

                                // Queue finished parquet files for background upload
                                let finished = writer.take_finished_files();
                                for file in finished {
                                    self.stats.parquet_files_written += 1;
                                    self.stats.bytes_written += file.size;
                                    emit!(BytesWritten { bytes: file.size as u64 });
                                    if upload_tx.send(file).await.is_err() {
                                        error!("Upload channel closed unexpectedly");
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                if e.is_not_found() {
                                    warn!("Skipping file with processing error (not found): {}", e);
                                    files_remaining -= 1;
                                    emit!(FileProcessed { status: FileStatus::Skipped });
                                } else {
                                    // Determine failure stage based on error type.
                                    // Note: With streaming decompression, gzip errors surface
                                    // through the JSON reader and are classified as Parse failures.
                                    // Only zstd decoder creation errors are caught as Decompress.
                                    let stage = match &e {
                                        PipelineError::Reader {
                                            source:
                                                crate::error::ReaderError::ZstdDecompression { .. },
                                        } => FailureStage::Decompress,
                                        _ => FailureStage::Parse,
                                    };

                                    // Record failure and continue processing
                                    let error_str = e.to_string();
                                    warn!("Skipping failed file (processing error): {}", e);
                                    files_remaining -= 1;
                                    failure_count += 1;
                                    emit!(FileProcessed { status: FileStatus::Failed });
                                    emit!(FileFailed { stage });

                                    // Record to DLQ if configured
                                    if let Some(dlq) = &dlq {
                                        dlq.record_failure("unknown", &error_str, stage).await;
                                    }

                                    // Check max_failures limit
                                    if max_failures > 0 && failure_count >= max_failures {
                                        error!("Max failures ({}) reached, stopping pipeline", failure_count);
                                        // Finalize DLQ before returning
                                        if let Some(dlq) = &dlq
                                            && let Err(dlq_err) = dlq.finalize().await
                                        {
                                            error!("Failed to finalize DLQ: {}", dlq_err);
                                        }
                                        return MaxFailuresExceededSnafu { count: failure_count }.fail();
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Note: checkpoints are committed atomically with file uploads
            // The uploader task handles checkpoint timing internally
        }

        // Drop the receiver to unblock the download task (it will see channel closed)
        drop(download_rx);

        // Cancel the download task - don't wait for it since it may be blocked
        download_handle.abort();

        // Send remaining parquet files to uploader
        info!("Final flush - closing writer");
        let finished_files = writer.close().context(ParquetSnafu)?;
        for file in finished_files {
            self.stats.parquet_files_written += 1;
            self.stats.bytes_written += file.size;
            emit!(BytesWritten {
                bytes: file.size as u64
            });
            if upload_tx.send(file).await.is_err() {
                error!("Upload channel closed unexpectedly during final flush");
            }
        }

        // Close upload channel and wait for uploader to finish
        drop(upload_tx);
        info!("Waiting for uploads to complete...");
        let (delta_sink, files_uploaded, bytes_uploaded) =
            upload_handle.await.context(TaskJoinSnafu)?;
        info!(
            "All uploads complete: {} files, {} bytes",
            files_uploaded, bytes_uploaded
        );
        self.stats.delta_commits += files_uploaded;

        // Note: final checkpoint is committed atomically with the last file batch
        // in the uploader task

        // Finalize DLQ if configured
        if let Some(dlq) = &dlq
            && let Err(dlq_err) = dlq.finalize().await
        {
            error!("Failed to finalize DLQ: {}", dlq_err);
        }

        if failure_count > 0 {
            warn!(
                "Iteration completed with {} failures (recorded to DLQ if configured)",
                failure_count
            );
        }

        info!(
            "Iteration complete, Delta table version: {}",
            delta_sink.version()
        );
        info!("Checkpoint version: {}", delta_sink.checkpoint_version());

        if shutdown_requested {
            Ok(IterationResult::Shutdown)
        } else {
            Ok(IterationResult::ProcessedFiles)
        }
    }

    /// Prepare the pipeline for the first iteration (cold start).
    ///
    /// Performs full initialization: checkpoint restoration from Delta log,
    /// Delta sink creation, file listing, and writer setup.
    /// Returns `None` if there are no files to process.
    ///
    /// This method is designed to be cancellation-safe - it can be dropped at any
    /// `.await` point without leaving the system in an inconsistent state.
    async fn prepare_first_iteration(&mut self) -> Result<Option<InitializedState>, PipelineError> {
        // Create the Arrow schema from config
        let schema = self.config.to_arrow_schema();

        // Create Delta sink
        let mut delta_sink = DeltaSink::new(self.sink_storage.clone(), &schema)
            .await
            .context(DeltaSnafu)?;

        // Recover checkpoint from Delta transaction log
        if let Some((checkpoint, checkpoint_version)) = delta_sink
            .recover_checkpoint_from_log()
            .await
            .context(DeltaSnafu)?
        {
            info!(
                "Recovered checkpoint v{} from Delta log, delta_version: {}, files tracked: {}",
                checkpoint_version,
                checkpoint.delta_version,
                checkpoint.source_state.files.len()
            );

            // Restore checkpoint coordinator state
            self.checkpoint_coordinator
                .restore_from_state(checkpoint)
                .await;
        } else {
            info!("No checkpoint found in Delta log, starting fresh");
        }

        // List source files
        let source_files = self.list_source_files().await?;
        info!("Found {} source files", source_files.len());

        // Get current source state
        let source_state = self.checkpoint_coordinator.get_source_state().await;

        // Filter to unprocessed files
        let pending_files: Vec<String> = source_state
            .pending_files(&source_files)
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        info!("{} files remaining to process", pending_files.len());

        if pending_files.is_empty() {
            return Ok(None);
        }

        // Build writer configuration
        let rolling_policies = self.build_rolling_policies();
        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(self.config.sink.file_size_mb)
            .with_row_group_size_bytes(self.config.sink.row_group_size_bytes)
            .with_compression(self.config.sink.compression)
            .with_rolling_policies(rolling_policies);

        let writer = ParquetWriter::new(schema.clone(), writer_config);

        // Initialize DLQ if configured
        let dlq = DeadLetterQueue::from_config(&self.config.error_handling)
            .await
            .context(DlqSnafu)?
            .map(Arc::new);

        Ok(Some(InitializedState {
            pending_files,
            source_state,
            schema,
            writer,
            delta_sink,
            max_concurrent: self.config.source.max_concurrent_files,
            compression: self.config.source.compression,
            batch_size: self.config.source.batch_size,
            dlq,
        }))
    }

    /// Prepare the pipeline for subsequent iterations (polling mode).
    ///
    /// Creates a fresh DeltaSink for commits but skips checkpoint recovery,
    /// using the in-memory state from CheckpointCoordinator instead.
    /// This avoids redundant Delta log reads and ensures we don't lose state
    /// if the last batch had fewer files than the checkpoint commit threshold.
    ///
    /// Returns `None` if there are no new files to process.
    async fn prepare_next_iteration(&mut self) -> Result<Option<InitializedState>, PipelineError> {
        info!("Preparing next iteration (using in-memory checkpoint state)");

        // Create the Arrow schema from config
        let schema = self.config.to_arrow_schema();

        // Create fresh Delta sink (needed for commits)
        let delta_sink = DeltaSink::new(self.sink_storage.clone(), &schema)
            .await
            .context(DeltaSnafu)?;

        // Skip checkpoint recovery - use in-memory state from coordinator
        // This preserves state from files processed but not yet committed

        // List source files (may have new files since last iteration)
        let source_files = self.list_source_files().await?;
        info!("Found {} source files", source_files.len());

        // Get current source state from in-memory coordinator
        let source_state = self.checkpoint_coordinator.get_source_state().await;

        // Filter to unprocessed files
        let pending_files: Vec<String> = source_state
            .pending_files(&source_files)
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        info!("{} files remaining to process", pending_files.len());

        if pending_files.is_empty() {
            return Ok(None);
        }

        // Build writer configuration
        let rolling_policies = self.build_rolling_policies();
        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(self.config.sink.file_size_mb)
            .with_row_group_size_bytes(self.config.sink.row_group_size_bytes)
            .with_compression(self.config.sink.compression)
            .with_rolling_policies(rolling_policies);

        let writer = ParquetWriter::new(schema.clone(), writer_config);

        // Initialize DLQ if configured
        let dlq = DeadLetterQueue::from_config(&self.config.error_handling)
            .await
            .context(DlqSnafu)?
            .map(Arc::new);

        Ok(Some(InitializedState {
            pending_files,
            source_state,
            schema,
            writer,
            delta_sink,
            max_concurrent: self.config.source.max_concurrent_files,
            compression: self.config.source.compression,
            batch_size: self.config.source.batch_size,
            dlq,
        }))
    }

    /// Build rolling policies from configuration.
    fn build_rolling_policies(&self) -> Vec<RollingPolicy> {
        let mut policies = Vec::new();

        // Always include size-based rolling
        let size_bytes = self.config.sink.file_size_mb * MB;
        policies.push(RollingPolicy::SizeLimit(size_bytes));

        // Add inactivity timeout if configured
        if let Some(secs) = self.config.sink.inactivity_timeout_secs {
            policies.push(RollingPolicy::InactivityDuration(Duration::from_secs(secs)));
            debug!("Added inactivity rolling policy: {} seconds", secs);
        }

        // Add rollover timeout if configured
        if let Some(secs) = self.config.sink.rollover_timeout_secs {
            policies.push(RollingPolicy::RolloverDuration(Duration::from_secs(secs)));
            debug!("Added rollover rolling policy: {} seconds", secs);
        }

        policies
    }

    /// List source NDJSON files.
    async fn list_source_files(&self) -> Result<Vec<String>, PipelineError> {
        list_ndjson_files(&self.source_storage)
            .await
            .context(PipelineStorageSnafu)
    }
}

/// Run the pipeline with the given configuration.
pub async fn run_pipeline(config: Config) -> Result<PipelineStats, PipelineError> {
    let shutdown = CancellationToken::new();

    // Set up signal handler for graceful shutdown
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            shutdown_signal().await;
            shutdown.cancel();
        }
    });

    let mut pipeline = Pipeline::new(config, shutdown).await?;
    pipeline.run().await
}

/// Wait for a shutdown signal (SIGINT, SIGTERM, or SIGQUIT on Unix).
#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set up SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set up SIGTERM handler");
    let mut sigquit = signal(SignalKind::quit()).expect("Failed to set up SIGQUIT handler");

    tokio::select! {
        _ = sigint.recv() => {
            info!(message = "Signal received.", signal = "SIGINT");
        }
        _ = sigterm.recv() => {
            info!(message = "Signal received.", signal = "SIGTERM");
        }
        _ = sigquit.recv() => {
            info!(message = "Signal received.", signal = "SIGQUIT");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_stats_default() {
        let stats = PipelineStats::default();
        assert_eq!(stats.files_processed, 0);
        assert_eq!(stats.records_processed, 0);
    }
}
