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

mod signal;
mod tasks;

use futures::stream::{FuturesUnordered, StreamExt};
use snafu::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::checkpoint::CheckpointCoordinator;
use crate::config::{CompressionFormat, Config};
use crate::dlq::{DeadLetterQueue, FailureTracker};
use crate::emit;
use crate::error::{
    DeltaSnafu, DlqSnafu, ParquetSnafu, PipelineError, PipelineStorageSnafu, StorageError,
};
use crate::metrics::UtilizationTimer;
use crate::metrics::events::{
    BatchesProcessed, BytesWritten, DecompressionQueueDepth, FailureStage, FileProcessed,
    FileStatus, PendingBatches, RecordsProcessed,
};
use crate::sink::delta::DeltaSink;
use crate::sink::parquet::{ParquetWriter, ParquetWriterConfig};
use crate::source::{NdjsonReader, NdjsonReaderConfig};
use crate::storage::{StorageProvider, StorageProviderRef, list_ndjson_files};

use tasks::{DownloadedFile, Downloader, ProcessFuture, ProcessedFile, Uploader, spawn_read_task};

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

/// Result of a single processing iteration.
enum IterationResult {
    /// Files were processed successfully.
    ProcessedFiles,
    /// No files were available to process.
    NoFiles,
    /// Shutdown was requested.
    Shutdown,
}

/// Mutable state for the file processing loop.
struct ProcessingState {
    /// Number of files still to be processed.
    files_remaining: usize,
    /// In-flight decompression/parsing futures.
    processing: FuturesUnordered<ProcessFuture>,
    /// Whether the download channel is still open.
    channel_open: bool,
    /// Tracks processor utilization for metrics.
    util_timer: UtilizationTimer,
    /// Whether a shutdown was requested during processing.
    shutdown_requested: bool,
}

impl ProcessingState {
    fn new(files_count: usize) -> Self {
        Self {
            files_remaining: files_count,
            processing: FuturesUnordered::new(),
            channel_open: true,
            util_timer: UtilizationTimer::new("processor"),
            shutdown_requested: false,
        }
    }

    /// Returns true if we should continue the processing loop.
    fn should_continue(&self) -> bool {
        // Continue if channel is open or there are still processing tasks
        self.channel_open || !self.processing.is_empty()
    }

    /// Returns true if we have capacity to receive more downloads.
    fn has_capacity(&self, max_concurrent: usize) -> bool {
        self.processing.len() < max_concurrent && self.channel_open
    }

    /// Update utilization metrics at the start of each loop iteration.
    fn update_utilization(&mut self) {
        if self.processing.is_empty() {
            self.util_timer.start_wait();
        }
        self.util_timer.maybe_update();
    }

    /// Transition to working state when processing starts.
    fn start_processing(&mut self) {
        if self.processing.is_empty() {
            self.util_timer.stop_wait();
        }
    }

    /// Queue a downloaded file for decompression.
    fn queue_for_decompression(&mut self, downloaded: DownloadedFile, reader: &Arc<NdjsonReader>) {
        self.start_processing();
        self.processing
            .push(spawn_read_task(downloaded, reader.clone()));
        emit!(DecompressionQueueDepth {
            count: self.processing.len()
        });
    }

    /// Record a download error, skipping not-found errors.
    async fn record_download_error(
        &mut self,
        error: StorageError,
        failures: &mut FailureTracker,
    ) -> Result<(), PipelineError> {
        self.files_remaining -= 1;
        if error.is_not_found() {
            warn!("Skipping file with download error (not found): {}", error);
            emit!(FileProcessed {
                status: FileStatus::Skipped
            });
        } else {
            warn!("Skipping failed file (download error): {}", error);
            failures
                .record_failure(&error.to_string(), FailureStage::Download)
                .await?;
        }
        Ok(())
    }

    /// Handle a download channel result.
    async fn handle_download(
        &mut self,
        result: Option<Result<DownloadedFile, StorageError>>,
        reader: &Arc<NdjsonReader>,
        failures: &mut FailureTracker,
    ) -> Result<(), PipelineError> {
        match result {
            Some(Ok(downloaded)) => {
                self.queue_for_decompression(downloaded, reader);
            }
            Some(Err(e)) => {
                self.record_download_error(e, failures).await?;
            }
            None => {
                self.channel_open = false;
            }
        }
        Ok(())
    }

    /// Handle a processing error, classifying the failure stage.
    async fn handle_processing_error(
        &mut self,
        error: PipelineError,
        failures: &mut FailureTracker,
    ) -> Result<(), PipelineError> {
        self.files_remaining -= 1;
        if error.is_not_found() {
            warn!("Skipping file with processing error (not found): {}", error);
            emit!(FileProcessed {
                status: FileStatus::Skipped
            });
        } else {
            // Determine failure stage based on error type.
            // Note: With streaming decompression, gzip errors surface
            // through the JSON reader and are classified as Parse failures.
            // Only zstd decoder creation errors are caught as Decompress.
            let stage = match &error {
                PipelineError::Reader {
                    source: crate::error::ReaderError::ZstdDecompression { .. },
                } => FailureStage::Decompress,
                _ => FailureStage::Parse,
            };

            warn!("Skipping failed file (processing error): {}", error);
            failures.record_failure(&error.to_string(), stage).await?;
        }
        Ok(())
    }
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
                    let cold_start = first_iteration;
                    first_iteration = false;
                    self.prepare(cold_start).await
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

        // Track failures with DLQ support
        let mut failures =
            FailureTracker::new(self.config.error_handling.max_failures, dlq.clone());

        // Create the NDJSON reader for decompression and parsing
        let reader_config = NdjsonReaderConfig::new(batch_size, compression);
        let reader = Arc::new(NdjsonReader::new(schema.clone(), reader_config));

        // Spawn background uploader task
        let uploader = Uploader::spawn(
            delta_sink,
            self.sink_storage.clone(),
            self.checkpoint_coordinator.clone(),
            self.shutdown.clone(),
            &self.config.sink,
            dlq.clone(),
        );

        // Consumer: Process downloaded files with parallel decompression
        let files_count = pending_files.len();

        // Spawn background downloader task (consumes pending_files and source_state)
        let mut downloader = Downloader::spawn(
            pending_files,
            source_state,
            self.source_storage.clone(),
            self.shutdown.clone(),
            max_concurrent,
        );

        let mut state = ProcessingState::new(files_count);

        loop {
            state.update_utilization();

            if self.shutdown.is_cancelled() {
                info!("Shutdown requested, stopping processing");
                state.shutdown_requested = true;
                break;
            }

            if !state.should_continue() {
                break;
            }

            let has_capacity = state.has_capacity(max_concurrent);

            tokio::select! {
                // Only poll for new downloads if we have processing capacity
                result = downloader.rx.recv(), if has_capacity => {
                    state.handle_download(result, &reader, &mut failures).await?;
                }

                // Wait for processing tasks to complete
                Some(result) = state.processing.next(), if !state.processing.is_empty() => {
                    emit!(DecompressionQueueDepth {
                        count: state.processing.len()
                    });
                    match result {
                        Ok(processed) => {
                            self.handle_processed_file(processed, &mut state, &mut writer, &uploader).await?;
                        }
                        Err(e) => {
                            state.handle_processing_error(e, &mut failures).await?;
                        }
                    }
                }
            }

            // Note: checkpoints are committed atomically with file uploads
            // The uploader task handles checkpoint timing internally
        }

        // Abort downloader - we don't need to wait for it
        downloader.abort();

        // Send remaining parquet files to uploader
        info!("Final flush - closing writer");
        let finished_files = writer.close().context(ParquetSnafu)?;
        for file in finished_files {
            self.stats.parquet_files_written += 1;
            self.stats.bytes_written += file.size;
            emit!(BytesWritten {
                bytes: file.size as u64
            });
            if uploader.tx.send(file).await.is_err() {
                error!("Upload channel closed unexpectedly during final flush");
            }
        }

        // Wait for uploader to finish
        info!("Waiting for uploads to complete...");
        let result = uploader.finish().await?;
        info!(
            "All uploads complete: {} files, {} bytes",
            result.files_uploaded, result.bytes_uploaded
        );
        self.stats.delta_commits += result.files_uploaded;

        // Note: final checkpoint is committed atomically with the last file batch
        // in the uploader task

        // Finalize DLQ if configured
        failures.finalize_dlq().await;

        if failures.has_failures() {
            warn!(
                "Iteration completed with {} failures (recorded to DLQ if configured)",
                failures.count()
            );
        }

        info!(
            "Iteration complete, Delta table version: {}",
            result.delta_sink.version()
        );
        info!(
            "Checkpoint version: {}",
            result.delta_sink.checkpoint_version()
        );

        if state.shutdown_requested {
            Ok(IterationResult::Shutdown)
        } else {
            Ok(IterationResult::ProcessedFiles)
        }
    }

    /// Handle a successfully processed file: write batches, update checkpoint, queue uploads.
    async fn handle_processed_file(
        &mut self,
        processed: ProcessedFile,
        state: &mut ProcessingState,
        writer: &mut ParquetWriter,
        uploader: &Uploader,
    ) -> Result<(), PipelineError> {
        let short_name = processed.short_name();

        // Track pending batches before writing
        emit!(PendingBatches {
            count: processed.batches.len()
        });

        // Write all batches from this file
        for batch in &processed.batches {
            debug!("[batch] {}: {} rows", short_name, batch.num_rows());
            writer.write_batch(batch).context(ParquetSnafu)?;
            self.stats.records_processed += batch.num_rows();
            emit!(RecordsProcessed {
                count: batch.num_rows() as u64
            });
            emit!(BatchesProcessed { count: 1 });
        }
        emit!(PendingBatches { count: 0 });

        // Update checkpoint state
        self.checkpoint_coordinator
            .update_source_state(&processed.path, processed.total_records, true)
            .await;

        self.stats.files_processed += 1;
        state.files_remaining -= 1;
        emit!(FileProcessed {
            status: FileStatus::Success
        });
        debug!(
            "[-] Finished file (remaining: {}): {} ({} records)",
            state.files_remaining, short_name, processed.total_records
        );

        // Queue finished parquet files for background upload
        for file in writer.take_finished_files() {
            self.stats.parquet_files_written += 1;
            self.stats.bytes_written += file.size;
            emit!(BytesWritten {
                bytes: file.size as u64
            });
            if uploader.tx.send(file).await.is_err() {
                error!("Upload channel closed unexpectedly");
                break;
            }
        }

        Ok(())
    }

    /// Prepares the pipeline for a processing iteration.
    ///
    /// # State Recovery Logic
    ///
    /// **Cold Start:** Recovers state from the Delta transaction log to identify
    ///     previously committed files.
    /// **Warm Start:** Uses the in-memory `CheckpointCoordinator`. This prevents
    ///     state regression, as the in-memory state tracks progress that hasn't yet
    ///     been flushed to a persistent checkpoint.
    ///
    /// Returns `None` if no files are available.
    async fn prepare(
        &mut self,
        cold_start: bool,
    ) -> Result<Option<InitializedState>, PipelineError> {
        let schema = self.config.to_arrow_schema();

        let mut delta_sink = DeltaSink::new(self.sink_storage.clone(), &schema)
            .await
            .context(DeltaSnafu)?;

        if cold_start {
            self.checkpoint_coordinator
                .restore_from_delta_log(&mut delta_sink)
                .await
                .context(DeltaSnafu)?;
        } else {
            info!("Preparing next iteration (using in-memory checkpoint state)");
        }

        let source_files = self.list_source_files().await?;
        let source_file_count = source_files.len();
        info!("Found {} source files", source_file_count);

        let source_state = self.checkpoint_coordinator.get_source_state().await;

        // Filter to pending files, consuming source_files to avoid re-allocation
        let pending_files = source_state.filter_pending_files(source_files);
        info!("{} files remaining to process", pending_files.len());

        if pending_files.is_empty() {
            return Ok(None);
        }

        let rolling_policies = self.config.sink.rolling_policies();
        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(self.config.sink.file_size_mb)
            .with_row_group_size_bytes(self.config.sink.row_group_size_bytes)
            .with_compression(self.config.sink.compression)
            .with_rolling_policies(rolling_policies);

        let writer = ParquetWriter::new(schema.clone(), writer_config).context(ParquetSnafu)?;

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
            signal::shutdown_signal().await;
            shutdown.cancel();
        }
    });

    let mut pipeline = Pipeline::new(config, shutdown).await?;
    pipeline.run().await
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
