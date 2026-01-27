//! Background upload task.

use futures::stream::{FuturesUnordered, StreamExt};
use snafu::ResultExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::checkpoint::CheckpointCoordinator;
use crate::config::{MB, SinkConfig};
use crate::dlq::DeadLetterQueue;
use crate::emit;
use crate::error::{PipelineError, StorageError, TaskJoinSnafu};
use crate::metrics::UtilizationTimer;
use crate::metrics::events::{
    ActiveUploads, FailureStage, FileFailed, PendingCommitFiles, UploadQueueBytes, UploadQueueDepth,
};
use crate::sink::FinishedFile;
use crate::sink::delta::DeltaSink;
use crate::storage::{StorageProvider, StorageProviderRef};

/// Future type for upload operations.
type UploadFuture = Pin<Box<dyn Future<Output = Result<UploadResult, StorageError>> + Send>>;

/// Result of uploading a file.
struct UploadResult {
    filename: String,
    size: usize,
    record_count: usize,
}

/// Configuration for the uploader task.
struct UploaderConfig {
    part_size: usize,
    min_multipart_size: usize,
    max_concurrent_uploads: usize,
    max_concurrent_parts: usize,
}

/// Handle to the background uploader task.
pub(in crate::pipeline) struct Uploader {
    pub tx: mpsc::Sender<FinishedFile>,
    handle: JoinHandle<(DeltaSink, usize, usize)>,
}

impl Uploader {
    /// Spawn the uploader task.
    pub fn spawn(
        delta_sink: DeltaSink,
        sink_storage: StorageProviderRef,
        checkpoint_coordinator: Arc<CheckpointCoordinator>,
        shutdown: CancellationToken,
        sink_config: &SinkConfig,
        dlq: Option<Arc<DeadLetterQueue>>,
    ) -> Self {
        let buffer_size = sink_config.max_concurrent_uploads * 4;
        let (tx, rx) = mpsc::channel(buffer_size);

        let config = UploaderConfig {
            part_size: sink_config.part_size_mb * MB,
            min_multipart_size: sink_config.min_multipart_size_mb * MB,
            max_concurrent_uploads: sink_config.max_concurrent_uploads,
            max_concurrent_parts: sink_config.max_concurrent_parts,
        };

        let handle = tokio::spawn(Self::run(
            rx,
            delta_sink,
            sink_storage,
            checkpoint_coordinator,
            shutdown,
            config,
            dlq,
        ));

        Self { tx, handle }
    }

    /// Finish uploading and wait for all uploads to complete.
    ///
    /// Returns the final DeltaSink state and upload statistics.
    pub async fn finish(self) -> Result<(DeltaSink, usize, usize), PipelineError> {
        drop(self.tx);
        self.handle.await.context(TaskJoinSnafu)
    }

    /// Run the uploader task that handles concurrent file uploads and Delta commits.
    ///
    /// Commits include atomic checkpoints - the checkpoint state is captured and
    /// committed alongside file Add actions in a single Delta transaction.
    async fn run(
        mut upload_rx: mpsc::Receiver<FinishedFile>,
        mut delta_sink: DeltaSink,
        sink_storage: StorageProviderRef,
        checkpoint_coordinator: Arc<CheckpointCoordinator>,
        shutdown: CancellationToken,
        config: UploaderConfig,
        dlq: Option<Arc<DeadLetterQueue>>,
    ) -> (DeltaSink, usize, usize) {
        let mut uploads: FuturesUnordered<UploadFuture> = FuturesUnordered::new();

        let mut active_uploads = 0;
        let max_concurrent_uploads = config.max_concurrent_uploads;
        let mut files_uploaded = 0usize;
        let mut bytes_uploaded = 0usize;
        let mut files_to_commit: Vec<FinishedFile> = Vec::new();
        let mut channel_open = true;
        let mut util_timer = UtilizationTimer::new("uploader");
        let mut upload_queue_bytes: usize = 0;

        const COMMIT_BATCH_SIZE: usize = 10;

        loop {
            // Check if we're done: channel closed, no pending uploads, no files to commit
            if !channel_open && uploads.is_empty() {
                break;
            }

            // Update utilization state: waiting if no active uploads
            if active_uploads == 0 {
                util_timer.start_wait();
            }
            util_timer.maybe_update();

            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    info!("[upload] Shutdown requested, stopping uploads");
                    break;
                }

                // Handle completed uploads
                Some(result) = uploads.next(), if !uploads.is_empty() => {
                    active_uploads -= 1;
                    emit!(ActiveUploads {
                        count: active_uploads
                    });
                    match result {
                        Ok(upload_result) => {
                            debug!(
                                "[upload] Completed {} (active: {})",
                                upload_result.filename, active_uploads
                            );
                            files_uploaded += 1;
                            bytes_uploaded += upload_result.size;

                            // Track upload queue memory (file bytes now released)
                            upload_queue_bytes = upload_queue_bytes.saturating_sub(upload_result.size);
                            emit!(UploadQueueBytes { bytes: upload_queue_bytes });
                            emit!(UploadQueueDepth { count: active_uploads });

                            files_to_commit.push(FinishedFile {
                                filename: upload_result.filename,
                                size: upload_result.size,
                                record_count: upload_result.record_count,
                                bytes: None,
                            });
                            emit!(PendingCommitFiles { count: files_to_commit.len() });

                            // Batch commit every N files with atomic checkpoint
                            if files_to_commit.len() >= COMMIT_BATCH_SIZE {
                                commit_files_with_checkpoint(
                                    &mut delta_sink,
                                    &mut files_to_commit,
                                    &checkpoint_coordinator,
                                )
                                .await;
                                emit!(PendingCommitFiles { count: files_to_commit.len() });
                            }
                        }
                        Err(e) => {
                            error!("[upload] Upload failed: {}", e);
                            emit!(FileFailed { stage: FailureStage::Upload });

                            // Record to DLQ if configured
                            if let Some(dlq) = &dlq {
                                dlq.record_failure("unknown", &e.to_string(), FailureStage::Upload).await;
                            }
                        }
                    }
                }

                // Accept new files if under concurrency limit
                result = upload_rx.recv(), if active_uploads < max_concurrent_uploads && channel_open => {
                    match result {
                        Some(file) => {
                            // Transition to working state when we have uploads
                            if active_uploads == 0 {
                                util_timer.stop_wait();
                            }
                            active_uploads += 1;
                            emit!(ActiveUploads {
                                count: active_uploads
                            });

                            // Track upload queue memory
                            upload_queue_bytes += file.size;
                            emit!(UploadQueueBytes { bytes: upload_queue_bytes });
                            emit!(UploadQueueDepth { count: active_uploads });

                            info!(
                                "[upload] Starting {} ({} bytes, {} records, active: {})",
                                file.filename, file.size, file.record_count, active_uploads
                            );
                            uploads.push(Box::pin(upload_file(
                                sink_storage.clone(),
                                file,
                                config.part_size,
                                config.min_multipart_size,
                                config.max_concurrent_parts,
                            )));
                        }
                        None => {
                            // Channel closed, but continue draining uploads
                            channel_open = false;
                            debug!("[upload] Channel closed, draining {} pending uploads", uploads.len());
                        }
                    }
                }
            }
        }

        // Drain any remaining pending uploads
        while let Some(result) = uploads.next().await {
            if let Ok(upload_result) = result {
                files_uploaded += 1;
                bytes_uploaded += upload_result.size;
                files_to_commit.push(FinishedFile {
                    filename: upload_result.filename,
                    size: upload_result.size,
                    record_count: upload_result.record_count,
                    bytes: None,
                });
            }
        }

        // Final commit with checkpoint
        commit_files_with_checkpoint(
            &mut delta_sink,
            &mut files_to_commit,
            &checkpoint_coordinator,
        )
        .await;

        // Reset gauges to 0 on completion
        emit!(ActiveUploads { count: 0 });
        emit!(UploadQueueBytes { bytes: 0 });
        emit!(UploadQueueDepth { count: 0 });
        emit!(PendingCommitFiles { count: 0 });

        info!(
            "Uploader finished: {} files, {} bytes",
            files_uploaded, bytes_uploaded
        );
        (delta_sink, files_uploaded, bytes_uploaded)
    }
}

/// Upload a single file to storage with parallel part uploads.
async fn upload_file(
    storage: Arc<StorageProvider>,
    file: FinishedFile,
    part_size: usize,
    min_multipart_size: usize,
    max_concurrent_parts: usize,
) -> Result<UploadResult, StorageError> {
    let Some(bytes) = file.bytes else {
        // No bytes means the file was already uploaded (e.g., from checkpoint recovery)
        return Ok(UploadResult {
            filename: file.filename,
            size: file.size,
            record_count: file.record_count,
        });
    };

    let path = object_store::path::Path::from(file.filename.as_str());
    storage
        .put_multipart_bytes_parallel(
            &path,
            bytes,
            part_size,
            min_multipart_size,
            max_concurrent_parts,
        )
        .await?;

    Ok(UploadResult {
        filename: file.filename,
        size: file.size,
        record_count: file.record_count,
    })
}

/// Commit files with atomic checkpoint.
///
/// Captures the current checkpoint state and commits it atomically
/// with the file Add actions in a single Delta transaction.
async fn commit_files_with_checkpoint(
    delta_sink: &mut DeltaSink,
    files_to_commit: &mut Vec<FinishedFile>,
    checkpoint_coordinator: &CheckpointCoordinator,
) -> usize {
    if files_to_commit.is_empty() {
        return 0;
    }

    let commit_files: Vec<FinishedFile> = files_to_commit
        .drain(..)
        .map(|f| FinishedFile {
            filename: f.filename,
            size: f.size,
            record_count: f.record_count,
            bytes: None, // Clear bytes for commit
        })
        .collect();

    let count = commit_files.len();

    // Capture current checkpoint state
    let checkpoint_state = checkpoint_coordinator.capture_state().await;

    // Commit with atomic checkpoint
    match delta_sink
        .commit_files_with_checkpoint(&commit_files, &checkpoint_state)
        .await
    {
        Ok(Some(version)) => {
            info!(
                "Committed {} files with checkpoint to Delta Lake, version {}",
                count, version
            );
            // Update coordinator with new delta version and mark checkpoint committed
            checkpoint_coordinator.update_delta_version(version).await;
            checkpoint_coordinator.mark_checkpoint_committed().await;
        }
        Ok(None) => {
            debug!("No commit needed (duplicate files)");
        }
        Err(e) => {
            error!("Failed to commit {} files to Delta: {}", count, e);
        }
    }
    count
}
