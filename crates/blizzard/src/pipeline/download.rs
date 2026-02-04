//! Download processing orchestration.
//!
//! Coordinates the download -> parse -> write pipeline with backpressure
//! and failure handling.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::time::Interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use blizzard_core::emit;
use blizzard_core::metrics::UtilizationTimer;
use blizzard_core::metrics::events::{
    DecompressionQueueDepth, FailureStage, FileProcessed, FileStatus, SourceStateFiles,
};
use blizzard_core::polling::IterationResult;

use super::sink::Sink;
use super::tasks::{DownloadTask, ProcessFuture, ProcessedFile, spawn_read_task};
use super::tracker::StateTracker;
use crate::config::CheckpointConfig;
use crate::dlq::FailureTracker;
use crate::error::PipelineError;
use crate::source::FileReader;

/// Configuration for incremental checkpoint saves during download processing.
#[derive(Debug, Clone)]
pub(super) struct IncrementalCheckpointConfig {
    /// Number of files to process before saving a checkpoint.
    pub interval_files: usize,
    /// Interval for time-based checkpoint saves.
    pub interval: Duration,
    /// Whether incremental checkpointing is enabled.
    pub enabled: bool,
}

impl IncrementalCheckpointConfig {
    /// Create config from pipeline checkpoint settings.
    pub fn new(checkpoint: &CheckpointConfig, use_watermark: bool) -> Self {
        Self {
            interval_files: checkpoint.interval_files,
            interval: Duration::from_secs(checkpoint.interval_secs),
            enabled: use_watermark,
        }
    }
}

/// Orchestrates the download -> parse -> write pipeline.
///
/// Manages concurrent downloads and parsing with backpressure,
/// coordinating between the file downloader, reader, and sink writer.
pub(super) struct Downloader {
    reader: Arc<dyn FileReader>,
    max_in_flight: usize,
    pipeline_key: String,
}

impl Downloader {
    pub fn new(reader: Arc<dyn FileReader>, max_in_flight: usize, pipeline_key: String) -> Self {
        Self {
            reader,
            max_in_flight,
            pipeline_key,
        }
    }

    /// Run the download processing loop.
    ///
    /// Consumes downloads from the downloader, spawns read tasks, writes results
    /// to the sink, and tracks state/failures.
    ///
    /// When `checkpoint_config.enabled` is true, saves checkpoints periodically
    /// based on file count and time interval to prevent progress loss on crash.
    pub async fn run(
        &self,
        mut download_task: DownloadTask,
        sink: &mut Sink,
        state_tracker: &mut dyn StateTracker,
        failure_tracker: &mut FailureTracker,
        shutdown: CancellationToken,
        checkpoint_config: &IncrementalCheckpointConfig,
    ) -> Result<IterationResult, PipelineError> {
        let mut processing: FuturesUnordered<ProcessFuture> = FuturesUnordered::new();
        let mut files_since_save: usize = 0;
        let mut util_timer = UtilizationTimer::new("processor");

        // Create checkpoint interval timer if enabled
        let mut checkpoint_interval: Option<Interval> = if checkpoint_config.enabled {
            let mut interval = tokio::time::interval(checkpoint_config.interval);
            interval.reset(); // Don't fire immediately
            Some(interval)
        } else {
            None
        };

        loop {
            emit!(DecompressionQueueDepth {
                count: processing.len(),
                target: self.pipeline_key.clone(),
            });
            emit!(SourceStateFiles {
                count: state_tracker.tracked_count(),
                target: self.pipeline_key.clone(),
            });

            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    info!(target = %self.pipeline_key, "Shutdown requested during processing");
                    download_task.abort();
                    return Ok(IterationResult::Shutdown);
                }

                Some(result) = processing.next(), if !processing.is_empty() => {
                    util_timer.maybe_update();

                    // Update utilization state: waiting if no active processing
                    if processing.is_empty() {
                        util_timer.start_wait();
                    }

                    self.handle_processed_file(
                        result,
                        sink,
                        state_tracker,
                        failure_tracker,
                    ).await?;

                    // Track files for incremental checkpoint
                    if checkpoint_config.enabled {
                        files_since_save += 1;
                        if files_since_save >= checkpoint_config.interval_files {
                            self.try_incremental_save(state_tracker, &mut files_since_save).await;
                        }
                    }
                }

                // Time-based checkpoint save
                _ = Self::tick_checkpoint(&mut checkpoint_interval), if checkpoint_config.enabled && files_since_save > 0 => {
                    self.try_incremental_save(state_tracker, &mut files_since_save).await;
                }

                result = download_task.rx.recv(), if processing.len() < self.max_in_flight => {
                    match result {
                        Some(Ok(downloaded)) => {
                            // Transition to working state when we have processing tasks
                            if processing.is_empty() {
                                util_timer.stop_wait();
                            }
                            let future = spawn_read_task(downloaded, self.reader.clone());
                            processing.push(future);
                        }
                        Some(Err(e)) => {
                            warn!(target = %self.pipeline_key, error = %e, "Download failed");
                            failure_tracker
                                .record_failure(&e.to_string(), FailureStage::Download)
                                .await?;
                        }
                        None => {
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

    /// Tick the checkpoint interval timer if it exists.
    async fn tick_checkpoint(interval: &mut Option<Interval>) {
        match interval {
            Some(i) => i.tick().await,
            None => std::future::pending().await,
        };
    }

    /// Attempt an incremental checkpoint save, logging on failure but not propagating errors.
    async fn try_incremental_save(
        &self,
        state_tracker: &mut dyn StateTracker,
        files_since_save: &mut usize,
    ) {
        match state_tracker.save().await {
            Ok(()) => {
                debug!(
                    target = %self.pipeline_key,
                    files_since_last_save = *files_since_save,
                    "Incremental checkpoint saved"
                );
                *files_since_save = 0;
            }
            Err(e) => {
                // Log warning but don't reset counter - will retry on next trigger
                warn!(
                    target = %self.pipeline_key,
                    error = %e,
                    "Incremental checkpoint save failed"
                );
            }
        }
    }

    async fn handle_processed_file(
        &self,
        result: Result<ProcessedFile, PipelineError>,
        sink: &mut Sink,
        state_tracker: &mut dyn StateTracker,
        failure_tracker: &mut FailureTracker,
    ) -> Result<(), PipelineError> {
        match result {
            Ok(ProcessedFile { path, batches }) => {
                sink.write_file_batches(&path, batches).await?;
                state_tracker.mark_processed(&path);
                emit!(FileProcessed {
                    status: FileStatus::Success,
                    target: self.pipeline_key.clone(),
                });
            }
            Err(e) => {
                warn!(target = %self.pipeline_key, error = %e, "File processing failed");
                failure_tracker
                    .record_failure(&e.to_string(), FailureStage::Parse)
                    .await?;
            }
        }
        Ok(())
    }
}
