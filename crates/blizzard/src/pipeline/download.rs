//! Download processing orchestration.
//!
//! Coordinates the download -> parse -> write pipeline with backpressure
//! and failure handling. Supports multiple sources merging into parallel sinks.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
use tokio::sync::mpsc::{self, error::TrySendError};
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
        util_timer: &mut UtilizationTimer,
    ) -> Result<IterationResult, PipelineError> {
        let mut pending: VecDeque<ProcessedFile> = VecDeque::new();
        let mut files_since_save: usize = 0;
        let mut files_downloaded: usize = 0;
        let mut files_processed: usize = 0;
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

            // Try to dispatch pending files to any worker with capacity.
            // Prefers round-robin order but skips full workers to avoid
            // head-of-line blocking where one slow worker stalls the pipeline.
            if let Some(processed) = pending.pop_front() {
                util_timer.maybe_update();

                if files_in_flight == 0 {
                    util_timer.start_wait();
                }

                let source_name = processed.source_name.clone();
                let path = processed.path.clone();
                let mut file = Some(processed);
                for i in 0..num_workers {
                    let idx = (next_worker + i) % num_workers;
                    // SAFETY: `file` is always Some here — it starts as Some and is
                    // only set to None via take() on successful send (which breaks).
                    // The Full branch restores it to Some immediately.
                    let to_send = file.take().expect("file should be Some in dispatch loop");
                    match workers.file_txs[idx].try_send(to_send) {
                        Ok(()) => {
                            completion_tracker.assign(&source_name, &path);
                            next_worker = (idx + 1) % num_workers;
                            break;
                        }
                        Err(TrySendError::Full(returned)) => {
                            file = Some(returned);
                        }
                        Err(TrySendError::Closed(_)) => {
                            return Err(PipelineError::ChannelClosed);
                        }
                    }
                }
                if file.is_none() {
                    // Successfully dispatched
                    continue;
                }
                // All workers full — push back and fall through to select!
                // to process completions or accept new downloads.
                // to process completions or accept new downloads.
                if let Some(returned) = file {
                    pending.push_front(returned);
                }
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
                            // Mark completed to unblock contiguous drain.
                            // The watermark is NOT advanced for this file, but
                            // subsequent successes may advance it past this point.
                            // The file goes to DLQ rather than being retried.
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::ControlFlow;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use deltalake::arrow::array::RecordBatch;
    use deltalake::arrow::datatypes::SchemaRef;
    use indexmap::IndexMap;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use blizzard_core::PartitionExtractor;
    use blizzard_core::metrics::UtilizationTimer;
    use blizzard_core::polling::IterationResult;
    use blizzard_core::storage::StorageProvider;

    use super::*;
    use crate::checkpoint::CheckpointManager;
    use crate::dlq::FailureTracker;
    use crate::error::ReaderError;
    use crate::parquet::ParquetWriterConfig;
    use crate::pipeline::sink::Sink;
    use crate::pipeline::tasks::{MultipartConfig, UploadTask, run_sink_worker};
    use crate::pipeline::tracker::{MultiSourceTracker, WatermarkTracker};
    use crate::source::FileReader;
    use crate::test_util::{test_batch, test_schema};

    /// A mock reader that fails for a specific file path.
    struct FailingReader {
        schema: SchemaRef,
        fail_path: String,
    }

    impl FileReader for FailingReader {
        fn read_batches(
            &self,
            _data: Bytes,
            path: &str,
            on_batch: &mut dyn FnMut(RecordBatch) -> ControlFlow<()>,
        ) -> Result<usize, ReaderError> {
            if path == self.fail_path {
                return Err(ReaderError::JsonDecode {
                    path: path.to_string(),
                    message: "simulated failure".to_string(),
                });
            }
            let batch = test_batch(10);
            let rows = batch.num_rows();
            let _ = on_batch(batch);
            Ok(rows)
        }

        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
    }

    /// Integration test: a mid-batch reader failure causes the watermark to
    /// advance past the failed file when subsequent files succeed.
    ///
    /// Scenario: files A, B, C are downloaded in order. B fails during read.
    /// After processing, the checkpoint watermark should be at C.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mid_batch_failure_advances_watermark_past_failed_file() {
        let source_dir = TempDir::new().unwrap();
        let dest_dir = TempDir::new().unwrap();
        let checkpoint_dir = TempDir::new().unwrap();

        // Create source files on disk (FailingReader ignores content)
        for name in &["a.ndjson.gz", "b.ndjson.gz", "c.ndjson.gz"] {
            std::fs::write(source_dir.path().join(name), b"dummy").unwrap();
        }

        // Set up storage for source downloads and destination uploads
        let source_storage = Arc::new(
            StorageProvider::for_url_with_options(
                source_dir.path().to_str().unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap(),
        );
        let dest_storage = Arc::new(
            StorageProvider::for_url_with_options(
                dest_dir.path().to_str().unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap(),
        );
        let checkpoint_storage = Arc::new(
            StorageProvider::for_url_with_options(
                checkpoint_dir.path().to_str().unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap(),
        );

        // Create _blizzard directory for checkpoints
        std::fs::create_dir_all(checkpoint_dir.path().join("_blizzard")).unwrap();

        // Build FailingReader that errors on b.ndjson.gz
        let reader: Arc<dyn FileReader> = Arc::new(FailingReader {
            schema: test_schema(),
            fail_path: "b.ndjson.gz".to_string(),
        });
        let mut readers = IndexMap::new();
        readers.insert("src".to_string(), reader);

        // Set up DownloadTask: feed files via a controlled discovery channel
        let mut storages = IndexMap::new();
        storages.insert("src".to_string(), source_storage as _);

        let (discovery_tx, discovery_rx) = mpsc::channel(16);
        let shutdown = CancellationToken::new();
        let download_task = DownloadTask::spawn(
            discovery_rx,
            storages,
            shutdown.clone(),
            4,
            None,
            "test".into(),
        );

        // Send files in order: a, b, c
        for name in &["a.ndjson.gz", "b.ndjson.gz", "c.ndjson.gz"] {
            discovery_tx
                .send(crate::pipeline::tracker::SourcedFile {
                    source_name: "src".to_string(),
                    path: name.to_string(),
                })
                .await
                .unwrap();
        }
        drop(discovery_tx); // Signal discovery complete

        // Set up 1 sink worker
        let upload_task = UploadTask::spawn(
            dest_storage,
            4,
            None,
            "test".to_string(),
            MultipartConfig {
                part_size: 10 * 1024 * 1024,
                min_multipart_size: 100 * 1024 * 1024,
                max_concurrent_parts: 8,
            },
        );
        let sink = Sink::new(
            test_schema(),
            ParquetWriterConfig::default(),
            upload_task,
            PartitionExtractor::all(),
            "test".to_string(),
        )
        .unwrap();

        let (file_tx, file_rx) = mpsc::channel(1);
        let (result_tx, result_rx) = mpsc::unbounded_channel();
        tokio::spawn(run_sink_worker(sink, file_rx, result_tx));

        let workers = SinkWorkerChannels {
            file_txs: vec![file_tx],
            result_rx,
        };

        // Set up WatermarkTracker backed by a real CheckpointManager
        let checkpoint_manager = CheckpointManager::new(
            checkpoint_storage.clone(),
            "test".to_string(),
            "src".to_string(),
        );
        let tracker: Box<dyn crate::pipeline::tracker::StateTracker> =
            Box::new(WatermarkTracker::new(checkpoint_manager));
        let mut trackers = IndexMap::new();
        trackers.insert("src".to_string(), tracker);
        let mut multi_tracker = MultiSourceTracker::new(trackers, "test".to_string());

        // Set up FailureTracker (unlimited failures, no DLQ)
        let mut failure_tracker = FailureTracker::new(0, None, "test".to_string());

        let mut ctx = ProcessingContext {
            multi_tracker: &mut multi_tracker,
            failure_tracker: &mut failure_tracker,
        };

        let checkpoint_config = IncrementalCheckpointConfig {
            interval_files: 1000,
            interval: Duration::from_secs(3600),
            enabled: false,
        };

        let downloader = Downloader::new(readers, 4, "test".to_string());
        let mut util_timer = UtilizationTimer::new("test");

        // Run the download processing loop
        let result = downloader
            .run(
                download_task,
                &mut ctx,
                workers,
                shutdown,
                &checkpoint_config,
                &mut util_timer,
            )
            .await
            .unwrap();

        assert_eq!(result, IterationResult::ProcessedItems);
        assert_eq!(ctx.failure_tracker.count(), 1, "B should have failed");

        // Save checkpoint and verify watermark
        ctx.multi_tracker.save_all().await.unwrap();
        let _ = ctx;

        // Load with a fresh CheckpointManager and verify watermark is at C
        let mut fresh_manager =
            CheckpointManager::new(checkpoint_storage, "test".to_string(), "src".to_string());
        let loaded = fresh_manager.load().await.unwrap();
        assert!(loaded, "Checkpoint should have been saved");
        assert_eq!(
            fresh_manager.watermark(),
            Some("c.ndjson.gz"),
            "Watermark should advance past the failed file B to C"
        );
    }
}
