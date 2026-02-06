//! Download processing orchestration.
//!
//! Coordinates the download -> parse -> write pipeline with backpressure
//! and failure handling. Supports multiple sources merging into a single sink.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
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

use super::sink::Sink;
use super::tasks::{DownloadTask, ProcessedFile, spawn_read_task};
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
    pub sink: &'a mut Sink,
    pub multi_tracker: &'a mut MultiSourceTracker,
    pub failure_tracker: &'a mut FailureTracker,
}

/// Orchestrates the download -> parse -> write pipeline.
///
/// Manages concurrent downloads and parsing with backpressure,
/// coordinating between the file downloader, reader, and sink writer.
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
    /// Consumes downloads from the downloader, spawns read tasks, writes results
    /// to the sink, and tracks state/failures.
    ///
    /// When `checkpoint_config.enabled` is true, saves checkpoints periodically
    /// based on file count and time interval to prevent progress loss on crash.
    pub async fn run(
        &self,
        mut download_task: DownloadTask,
        ctx: &mut ProcessingContext<'_>,
        shutdown: CancellationToken,
        checkpoint_config: &IncrementalCheckpointConfig,
        total_files: usize,
    ) -> Result<IterationResult, PipelineError> {
        let mut pending: VecDeque<ProcessedFile> = VecDeque::new();
        let mut files_since_save: usize = 0;
        let mut files_processed: usize = 0;
        let mut util_timer = UtilizationTimer::new(&self.pipeline_key);

        // Track how many files have been spawned but not yet fully consumed.
        let mut files_in_flight: usize = 0;

        // Emit initial pending files count
        emit!(PendingFiles {
            count: total_files,
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

            // Process pending files before accepting new downloads
            if let Some(processed) = pending.pop_front() {
                util_timer.maybe_update();

                // Update utilization state: waiting if no active processing
                if files_in_flight == 0 {
                    util_timer.start_wait();
                }

                self.handle_processed_file(processed, ctx, &mut files_in_flight)
                    .await?;

                // Update pending files count
                files_processed += 1;
                emit!(PendingFiles {
                    count: total_files.saturating_sub(files_processed),
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
                continue;
            }

            // No pending files — wait for downloads, shutdown, or checkpoint
            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    info!(target = %self.pipeline_key, "Shutdown requested during processing");
                    download_task.abort();
                    return Ok(IterationResult::Shutdown);
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
                            files_in_flight += 1;
                            pending.push_back(spawn_read_task(downloaded, &self.readers));
                        }
                        Some(Err(e)) => {
                            warn!(target = %self.pipeline_key, error = %e, "Download failed");
                            ctx.failure_tracker
                                .record_failure(&e.to_string(), FailureStage::Download)
                                .await?;

                            // Update pending files count for failed download
                            files_processed += 1;
                            emit!(PendingFiles {
                                count: total_files.saturating_sub(files_processed),
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

    /// Handle a processed file result.
    ///
    /// Consumes batches from the streaming channel and writes them to the sink.
    ///
    /// # Watermark Advancement Atomicity
    ///
    /// The watermark is only advanced after successful sink writes. This is
    /// guaranteed by the `?` operator on `write_batch()` / `end_file()`:
    ///
    /// 1. All batch writes must succeed first
    /// 2. Only then does `mark_processed()` advance the watermark
    ///
    /// If any sink write fails, the error propagates and the watermark is
    /// never updated. On restart, the file will be reprocessed.
    async fn handle_processed_file(
        &self,
        processed: ProcessedFile,
        ctx: &mut ProcessingContext<'_>,
        files_in_flight: &mut usize,
    ) -> Result<(), PipelineError> {
        let ProcessedFile {
            source_name,
            path,
            mut batch_rx,
        } = processed;

        // IMPORTANT: Sink writes must succeed before watermark update.
        // The `?` ensures atomicity - if write fails, watermark stays put.
        let write_result = async {
            ctx.sink.start_file(&path)?;

            let mut batch_count: usize = 0;
            let mut total_records: usize = 0;

            while let Some(batch_result) = batch_rx.recv().await {
                let batch = batch_result.map_err(|e| PipelineError::Reader { source: e })?;
                total_records += batch.num_rows();
                batch_count += 1;
                ctx.sink.write_batch(&batch).await?;
            }

            ctx.sink.end_file(&path, batch_count, total_records).await?;
            Ok::<(), PipelineError>(())
        }
        .await;

        // Decrement before propagating errors so the slot is freed either way.
        *files_in_flight = files_in_flight.saturating_sub(1);

        write_result?;
        ctx.multi_tracker.mark_processed(&source_name, &path);
        emit!(FileProcessed {
            status: FileStatus::Success,
            target: self.pipeline_key.clone(),
        });
        Ok(())
    }
}
