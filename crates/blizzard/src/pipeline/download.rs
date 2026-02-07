//! Download processing orchestration.
//!
//! Coordinates the download -> parse -> write pipeline with backpressure
//! and failure handling. Supports multiple sources merging into parallel sinks.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
use tokio::sync::mpsc;
use tokio::time::Interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use blizzard_core::emit;
use blizzard_core::metrics::UtilizationTimer;
use blizzard_core::metrics::events::{
    DecompressionQueueDepth, FailureStage, FileProcessed, FileStatus, PendingFiles,
    SourceStateFiles,
};
use blizzard_core::polling::IterationResult;

use super::tasks::{
    CompletedFile, CompletionTracker, DownloadTask, ProcessedFile, spawn_read_task,
};
use super::tracker::MultiSourceTracker;
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

/// Context holding mutable references needed during download processing.
pub(super) struct ProcessingContext<'a> {
    pub multi_tracker: &'a mut MultiSourceTracker,
    pub failure_tracker: &'a mut FailureTracker,
}

/// Channels for communicating with sink workers.
pub(super) struct SinkWorkerChannels {
    /// Senders to distribute files to workers (one per worker).
    pub file_txs: Vec<mpsc::Sender<ProcessedFile>>,
    /// Receiver for completion results from all workers.
    pub result_rx: mpsc::UnboundedReceiver<Result<CompletedFile, (CompletedFile, PipelineError)>>,
}

/// Orchestrates the download -> parse -> write pipeline.
///
/// Manages concurrent downloads and parsing with backpressure,
/// coordinating between the file downloader, reader, and sink workers.
/// Supports multiple sources with different compression formats.
pub(super) struct Downloader {
    /// Per-source readers (compression may differ between sources).
    readers: IndexMap<String, Arc<dyn FileReader>>,
    max_in_flight: usize,
    pipeline_key: String,
}

impl Downloader {
    pub fn new(
        readers: IndexMap<String, Arc<dyn FileReader>>,
        max_in_flight: usize,
        pipeline_key: String,
    ) -> Self {
        Self {
            readers,
            max_in_flight,
            pipeline_key,
        }
    }

    /// Run the download processing loop.
    ///
    /// Consumes downloads from the downloader, spawns read tasks, distributes
    /// them to sink workers round-robin, and tracks state/failures.
    ///
    /// When `checkpoint_config.enabled` is true, saves checkpoints periodically
    /// based on file count and time interval to prevent progress loss on crash.
    pub async fn run(
        &self,
        mut download_task: DownloadTask,
        ctx: &mut ProcessingContext<'_>,
        mut workers: SinkWorkerChannels,
        shutdown: CancellationToken,
        checkpoint_config: &IncrementalCheckpointConfig,
    ) -> Result<IterationResult, PipelineError> {
        let mut pending: VecDeque<ProcessedFile> = VecDeque::new();
        let mut files_since_save: usize = 0;
        let mut files_downloaded: usize = 0;
        let mut files_processed: usize = 0;
        let mut util_timer = UtilizationTimer::new(&self.pipeline_key);
        let mut completion_tracker = CompletionTracker::new();

        // Track how many files have been spawned but not yet fully consumed.
        let mut files_in_flight: usize = 0;

        // Round-robin index for distributing files to workers
        let num_workers = workers.file_txs.len();
        let mut next_worker: usize = 0;

        // Emit initial pending files count (0 — discovery is still running)
        emit!(PendingFiles {
            count: 0,
            target: self.pipeline_key.clone(),
        });

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
                count: files_in_flight,
                target: self.pipeline_key.clone(),
            });
            emit!(SourceStateFiles {
                count: ctx.multi_tracker.tracked_count(),
                target: self.pipeline_key.clone(),
            });

            // Distribute pending files to workers (round-robin).
            if let Some(processed) = pending.pop_front() {
                util_timer.maybe_update();

                if files_in_flight == 0 {
                    util_timer.start_wait();
                }

                completion_tracker.assign(&processed.source_name, &processed.path);

                // Round-robin send to the next worker. The bounded channel
                // provides natural backpressure if this worker is busy.
                workers.file_txs[next_worker]
                    .send(processed)
                    .await
                    .map_err(|_| PipelineError::ChannelClosed)?;
                next_worker = (next_worker + 1) % num_workers;

                continue;
            }

            // No pending files — wait for downloads, completions, shutdown, or checkpoint
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    info!(target = %self.pipeline_key, "Shutdown requested during processing");
                    download_task.abort();
                    return Ok(IterationResult::Shutdown);
                }

                // Collect completions from sink workers
                Some(result) = workers.result_rx.recv() => {
                    files_in_flight = files_in_flight.saturating_sub(1);

                    match result {
                        Ok(completed) => {
                            if files_in_flight == 0 {
                                util_timer.stop_wait();
                            }

                            completion_tracker.mark_completed(&completed.path);

                            // Advance watermark for contiguous completions
                            for (source, path) in completion_tracker.drain_contiguous() {
                                ctx.multi_tracker.mark_processed(&source, &path);
                            }

                            emit!(FileProcessed {
                                status: FileStatus::Success,
                                target: self.pipeline_key.clone(),
                            });
                        }
                        Err((completed, error)) => {
                            warn!(
                                target = %self.pipeline_key,
                                path = %completed.path,
                                error = %error,
                                "Sink worker failed to process file"
                            );
                            // Mark completed to unblock contiguous drain —
                            // the file will be reprocessed on restart since
                            // its watermark was never advanced.
                            completion_tracker.mark_completed(&completed.path);
                            completion_tracker.drain_contiguous();

                            ctx.failure_tracker
                                .record_failure(&error.to_string(), FailureStage::Upload)
                                .await?;
                        }
                    }

                    files_processed += 1;
                    emit!(PendingFiles {
                        count: files_downloaded.saturating_sub(files_processed),
                        target: self.pipeline_key.clone(),
                    });

                    // Track files for incremental checkpoint
                    if checkpoint_config.enabled {
                        files_since_save += 1;
                        if files_since_save >= checkpoint_config.interval_files {
                            self.try_incremental_save(ctx.multi_tracker, &mut files_since_save)
                                .await;
                        }
                    }
                }

                // Time-based checkpoint save
                _ = Self::tick_checkpoint(&mut checkpoint_interval), if checkpoint_config.enabled && files_since_save > 0 => {
                    self.try_incremental_save(ctx.multi_tracker, &mut files_since_save).await;
                }

                result = download_task.rx.recv(), if files_in_flight < self.max_in_flight => {
                    match result {
                        Some(Ok(downloaded)) => {
                            // Transition to working state when we have processing tasks
                            if files_in_flight == 0 {
                                util_timer.stop_wait();
                            }
                            files_downloaded += 1;
                            files_in_flight += 1;
                            emit!(PendingFiles {
                                count: files_downloaded.saturating_sub(files_processed),
                                target: self.pipeline_key.clone(),
                            });
                            pending.push_back(spawn_read_task(downloaded, &self.readers));
                        }
                        Some(Err(e)) => {
                            warn!(target = %self.pipeline_key, error = %e, "Download failed");
                            ctx.failure_tracker
                                .record_failure(&e.to_string(), FailureStage::Download)
                                .await?;

                            // Count failed downloads for pending metric
                            files_downloaded += 1;
                            files_processed += 1;
                            emit!(PendingFiles {
                                count: files_downloaded.saturating_sub(files_processed),
                                target: self.pipeline_key.clone(),
                            });
                        }
                        None => {
                            if files_in_flight == 0 && pending.is_empty() {
                                break;
                            }
                        }
                    }
                }
            }
        }

        if files_processed == 0 {
            Ok(IterationResult::NoItems)
        } else {
            Ok(IterationResult::ProcessedItems)
        }
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
        multi_tracker: &mut MultiSourceTracker,
        files_since_save: &mut usize,
    ) {
        match multi_tracker.save_all().await {
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
}
