//! Background upload task.
//!
//! # Streaming Results Pattern
//!
//! This module uses bounded channels in both directions to provide proper backpressure:
//! - Input channel: bounded to `max_concurrent`, limits queued files
//! - Result channel: bounded to `max_concurrent`, limits pending results
//!
//! Callers must poll `try_recv()` to drain completed upload results during operation.
//! This prevents the result channel from filling up and blocking the upload task.
//!
//! # Graceful Shutdown
//!
//! The upload task intentionally does not listen to a shutdown token. This prevents
//! a race condition where the upload task could exit before the sink finishes sending
//! files, causing "Upload channel closed unexpectedly" errors.
//!
//! Instead, graceful shutdown follows this sequence:
//! 1. Shutdown signal is received by the downloader, which stops fetching new files
//! 2. The sink calls `finalize()`, which sends any remaining files and drops the sender
//! 3. The upload task sees the channel close and drains any in-flight uploads
//! 4. The upload task exits cleanly
//!
//! This design ensures all pending files are uploaded before shutdown completes.

use futures::stream::{FuturesUnordered, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use blizzard_core::emit;
use blizzard_core::metrics::UtilizationTimer;
use blizzard_core::metrics::events::{
    ActiveUploads, ParquetFileWritten, UploadQueueBytes, UploadQueueDepth, UploadResultsPending,
};
use blizzard_core::{FinishedFile, StorageProviderRef};

use crate::error::TableWriteError;

/// Future type for upload operations. Returns result and the file size for queue tracking.
type UploadFuture =
    Pin<Box<dyn Future<Output = (Result<UploadedFile, TableWriteError>, usize)> + Send>>;

/// Result of a successful file upload.
pub struct UploadedFile {
    pub filename: String,
    pub size: usize,
    pub record_count: usize,
}

/// Handle to the background uploader task.
pub struct UploadTask {
    tx: mpsc::Sender<FinishedFile>,
    rx: mpsc::Receiver<Result<UploadedFile, TableWriteError>>,
    handle: JoinHandle<()>,
}

impl UploadTask {
    /// Spawn the uploader task.
    ///
    /// See module-level docs for why this doesn't take a shutdown token.
    pub fn spawn(storage: StorageProviderRef, max_concurrent: usize, pipeline: String) -> Self {
        let (file_tx, file_rx) = mpsc::channel(max_concurrent);
        // Result channel is bounded - callers must poll try_recv() to drain results
        // during operation to prevent blocking the upload task.
        let (result_tx, result_rx) = mpsc::channel(max_concurrent);

        let handle = tokio::spawn(Self::run(
            file_rx,
            result_tx,
            storage,
            max_concurrent,
            pipeline,
        ));

        Self {
            tx: file_tx,
            rx: result_rx,
            handle,
        }
    }

    /// Queue a file for upload. Blocks when the channel is full (backpressure).
    pub async fn send(&self, file: FinishedFile) -> Result<(), TableWriteError> {
        self.tx
            .send(file)
            .await
            .map_err(|_| TableWriteError::UploadChannelClosed)
    }

    /// Poll for a completed upload result without blocking.
    ///
    /// Returns `Some(result)` if a result is available, `None` if the channel is empty.
    /// Callers should call this in a loop to drain all available results before sending
    /// new files to prevent the result channel from filling up.
    pub fn try_recv(&mut self) -> Option<Result<UploadedFile, TableWriteError>> {
        self.rx.try_recv().ok()
    }

    /// Finalize uploads: close sender, wait for all uploads to complete, collect results.
    ///
    /// Returns only the results that haven't been drained via `try_recv()`.
    pub async fn finalize(mut self) -> Vec<Result<UploadedFile, TableWriteError>> {
        // Drop sender to signal no more files
        drop(self.tx);

        // Collect remaining results
        let mut results = Vec::new();
        while let Some(result) = self.rx.recv().await {
            results.push(result);
        }

        // Reset pending results gauge now that we've drained them
        emit!(UploadResultsPending { count: 0 });

        // Wait for task to complete
        let _ = self.handle.await;

        results
    }

    /// Run the uploader task that manages concurrent file uploads.
    ///
    /// Exits only when the input channel is closed (sender dropped), ensuring
    /// all queued files are uploaded before shutdown. See module-level docs.
    async fn run(
        mut file_rx: mpsc::Receiver<FinishedFile>,
        result_tx: mpsc::Sender<Result<UploadedFile, TableWriteError>>,
        storage: StorageProviderRef,
        max_concurrent: usize,
        pipeline: String,
    ) {
        let mut uploads: FuturesUnordered<UploadFuture> = FuturesUnordered::new();
        let mut active_uploads: usize = 0;
        let mut queue_bytes: usize = 0;
        let mut results_pending: usize = 0;
        let mut util_timer = UtilizationTimer::new("uploader");

        // Buffer for a result waiting to be sent (when result channel is full)
        let mut pending_result: Option<Result<UploadedFile, TableWriteError>> = None;

        loop {
            tokio::select! {
                biased;

                // First priority: send any pending result (reserve capacity first, then send)
                Ok(permit) = result_tx.reserve(), if pending_result.is_some() => {
                    permit.send(pending_result.take().unwrap());
                    results_pending += 1;
                    emit!(UploadResultsPending { count: results_pending });
                }

                // Second priority: process completed uploads (to free up slots)
                Some((result, size)) = uploads.next(), if !uploads.is_empty() && pending_result.is_none() => {
                    util_timer.maybe_update();

                    active_uploads -= 1;
                    queue_bytes -= size;
                    emit!(ActiveUploads {
                        count: active_uploads,
                        target: pipeline.clone(),
                    });
                    emit!(UploadQueueBytes { bytes: queue_bytes });
                    emit!(UploadQueueDepth { count: active_uploads });

                    // Update utilization state: waiting if no active uploads
                    if active_uploads == 0 {
                        util_timer.start_wait();
                    }

                    match &result {
                        Ok(uploaded) => {
                            debug!(
                                "[upload] Completed {} ({} bytes, {} records)",
                                uploaded.filename, uploaded.size, uploaded.record_count
                            );
                        }
                        Err(e) => {
                            warn!("[upload] Upload failed: {e}");
                        }
                    }

                    // Buffer result to send (will be sent in next iteration)
                    pending_result = Some(result);
                }

                // Accept new files only when under max_concurrent limit and no pending result
                result = file_rx.recv(), if active_uploads < max_concurrent && pending_result.is_none() => {
                    let Some(file) = result else {
                        let remaining = uploads.len();
                        debug!("[upload] Input channel closed, draining {remaining} remaining uploads");
                        break;
                    };

                    let storage = storage.clone();
                    let pipeline_clone = pipeline.clone();
                    let size = file.size;

                    // Transition to working state when we have uploads
                    if active_uploads == 0 {
                        util_timer.stop_wait();
                    }
                    active_uploads += 1;
                    queue_bytes += size;
                    emit!(ActiveUploads {
                        count: active_uploads,
                        target: pipeline.clone(),
                    });
                    emit!(UploadQueueBytes { bytes: queue_bytes });
                    emit!(UploadQueueDepth { count: active_uploads });

                    debug!(
                        "[upload] Starting {} (active: {}/{})",
                        file.filename, active_uploads, max_concurrent
                    );

                    uploads.push(Box::pin(async move {
                        (upload_file(storage, file, pipeline_clone).await, size)
                    }));
                }
            }
        }

        // Send any pending result before draining
        if let Some(result) = pending_result
            && result_tx.send(result).await.is_ok()
        {
            results_pending += 1;
            emit!(UploadResultsPending {
                count: results_pending
            });
        }

        // Drain remaining uploads
        while let Some((result, size)) = uploads.next().await {
            active_uploads -= 1;
            queue_bytes -= size;
            emit!(ActiveUploads {
                count: active_uploads,
                target: pipeline.clone(),
            });
            emit!(UploadQueueBytes { bytes: queue_bytes });
            emit!(UploadQueueDepth {
                count: active_uploads
            });

            match &result {
                Ok(uploaded) => {
                    debug!(
                        "[upload] Completed {} ({} bytes)",
                        uploaded.filename, uploaded.size
                    );
                }
                Err(e) => {
                    warn!("[upload] Upload failed during drain: {e}");
                }
            }

            // Try to send, but don't fail if receiver is closed
            if result_tx.send(result).await.is_ok() {
                results_pending += 1;
                emit!(UploadResultsPending {
                    count: results_pending
                });
            }
        }

        // Reset gauges on completion (except results_pending, reset when finalize drains)
        emit!(ActiveUploads {
            count: 0,
            target: pipeline.clone(),
        });
        emit!(UploadQueueBytes { bytes: 0 });
        emit!(UploadQueueDepth { count: 0 });

        debug!("[upload] All uploads complete, {results_pending} results pending collection");
    }
}

/// Upload a file to storage asynchronously.
async fn upload_file(
    storage: StorageProviderRef,
    file: FinishedFile,
    pipeline: String,
) -> Result<UploadedFile, TableWriteError> {
    use object_store::PutPayload;
    use snafu::ResultExt;

    let start = Instant::now();
    let filename = file.filename.clone();
    let size = file.size;
    let record_count = file.record_count;

    if let Some(bytes) = file.bytes {
        storage
            .put_payload(
                &object_store::path::Path::from(filename.as_str()),
                PutPayload::from(bytes),
            )
            .await
            .context(crate::error::WriteSnafu)?;
    }

    emit!(ParquetFileWritten {
        bytes: size,
        target: pipeline,
    });

    info!(
        path = %filename,
        size = size,
        records = record_count,
        duration_ms = start.elapsed().as_millis(),
        "Uploaded parquet file to storage"
    );

    Ok(UploadedFile {
        filename,
        size,
        record_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use blizzard_core::StorageProvider;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_file(name: &str) -> FinishedFile {
        FinishedFile {
            filename: name.to_string(),
            size: 100,
            record_count: 10,
            bytes: Some(Bytes::from_static(b"test data")),
            partition_values: HashMap::new(),
            source_file: None,
        }
    }

    /// Test basic upload task functionality: send files, finalize, get results.
    ///
    /// This verifies the channel-based shutdown mechanism works correctly.
    /// The actual race condition fix is structural (no shutdown token parameter),
    /// so the real test is running the full pipeline with SIGINT.
    #[tokio::test]
    async fn test_upload_task_send_and_finalize() {
        let temp_dir = TempDir::new().unwrap();
        let dest_uri = temp_dir.path().to_str().unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(dest_uri, HashMap::new())
                .await
                .unwrap(),
        );

        let upload_task = UploadTask::spawn(storage, 4, "test".to_string());

        // Send a file - this should succeed
        upload_task.send(test_file("file1.parquet")).await.unwrap();

        // Send another file - this should also succeed
        upload_task.send(test_file("file2.parquet")).await.unwrap();

        // Finalize should complete successfully with all results
        let results = upload_task.finalize().await;
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    /// Test that all queued files are processed before the task exits.
    ///
    /// Verifies backpressure works when sending more files than max_concurrent.
    #[tokio::test]
    async fn test_upload_task_drains_all_files() {
        let temp_dir = TempDir::new().unwrap();
        let dest_uri = temp_dir.path().to_str().unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(dest_uri, HashMap::new())
                .await
                .unwrap(),
        );

        let upload_task = UploadTask::spawn(storage, 2, "test".to_string());

        // Send multiple files
        for i in 0..5 {
            upload_task
                .send(test_file(&format!("file{i}.parquet")))
                .await
                .unwrap();
        }

        // Finalize and verify all files were processed
        let results = upload_task.finalize().await;
        assert_eq!(results.len(), 5, "All 5 files should be processed");
        for (i, result) in results.iter().enumerate() {
            assert!(result.is_ok(), "File {i} should succeed");
        }
    }

    /// Test that streaming results with bounded channels doesn't deadlock.
    ///
    /// The upload task uses a pending_result buffer and biased select to ensure
    /// results are sent before accepting new files, preventing channel backpressure
    /// from causing deadlocks.
    #[tokio::test]
    async fn test_no_deadlock_with_bounded_channels() {
        use std::time::Duration;

        let temp_dir = TempDir::new().unwrap();
        let dest_uri = temp_dir.path().to_str().unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(dest_uri, HashMap::new())
                .await
                .unwrap(),
        );

        // Use max_concurrent=1 to maximize pressure on the result channel.
        let mut upload_task = UploadTask::spawn(storage, 1, "test".to_string());

        let file_count = 10;

        // Use a timeout to fail fast if deadlock occurs
        let send_result = tokio::time::timeout(Duration::from_secs(5), async {
            for i in 0..file_count {
                // Drain available results before sending (streaming pattern)
                while upload_task.try_recv().is_some() {}

                upload_task
                    .send(test_file(&format!("file{i}.parquet")))
                    .await
                    .unwrap();
            }
        })
        .await;

        assert!(
            send_result.is_ok(),
            "Sending files should not deadlock (timed out)"
        );

        // Finalize should drain remaining results
        let finalize_result = tokio::time::timeout(Duration::from_secs(5), async {
            upload_task.finalize().await
        })
        .await;

        assert!(
            finalize_result.is_ok(),
            "Finalize should not deadlock (timed out)"
        );

        // Note: some results may have been drained during sending,
        // so finalize returns only the remaining ones
        let results = finalize_result.unwrap();
        assert!(
            results.iter().all(|r| r.is_ok()),
            "All uploads should succeed"
        );
    }

    /// Test that try_recv drains results during operation.
    #[tokio::test]
    async fn test_try_recv_drains_results() {
        let temp_dir = TempDir::new().unwrap();
        let dest_uri = temp_dir.path().to_str().unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(dest_uri, HashMap::new())
                .await
                .unwrap(),
        );

        let mut upload_task = UploadTask::spawn(storage, 4, "test".to_string());

        // Send files
        for i in 0..4 {
            upload_task
                .send(test_file(&format!("file{i}.parquet")))
                .await
                .unwrap();
        }

        // Wait a bit for uploads to complete
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Drain results via try_recv
        let mut drained = 0;
        while upload_task.try_recv().is_some() {
            drained += 1;
        }

        // Finalize and count remaining
        let remaining = upload_task.finalize().await;

        assert_eq!(drained + remaining.len(), 4, "Total results should be 4");
    }
}
