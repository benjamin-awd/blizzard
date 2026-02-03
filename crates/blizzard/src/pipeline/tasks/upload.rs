//! Background upload task.
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
use blizzard_core::metrics::events::{ActiveUploads, ParquetFileWritten};
use blizzard_core::{FinishedFile, StorageProviderRef};

use crate::error::TableWriteError;

/// Future type for upload operations.
type UploadFuture = Pin<Box<dyn Future<Output = Result<UploadedFile, TableWriteError>> + Send>>;

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
    pub fn spawn(
        storage: StorageProviderRef,
        max_concurrent: usize,
        pipeline: String,
    ) -> Self {
        let (file_tx, file_rx) = mpsc::channel(max_concurrent);
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

    /// Finalize uploads: close sender, wait for all uploads to complete, collect results.
    pub async fn finalize(self) -> Vec<Result<UploadedFile, TableWriteError>> {
        // Drop sender to signal no more files
        drop(self.tx);

        // Collect all results
        let mut results = Vec::new();
        let mut rx = self.rx;
        while let Some(result) = rx.recv().await {
            results.push(result);
        }

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

        loop {
            tokio::select! {
                biased;

                // Process completed uploads (prioritize to free up slots)
                Some(result) = uploads.next(), if !uploads.is_empty() => {
                    active_uploads -= 1;
                    emit!(ActiveUploads {
                        count: active_uploads,
                        target: pipeline.clone(),
                    });

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

                    // Send result to consumer
                    if result_tx.send(result).await.is_err() {
                        debug!("[upload] Consumer closed, stopping uploads");
                        break;
                    }
                }

                // Accept new files only when under max_concurrent limit
                result = file_rx.recv(), if active_uploads < max_concurrent => {
                    let Some(file) = result else {
                        let remaining = uploads.len();
                        debug!("[upload] Input channel closed, draining {remaining} remaining uploads");
                        break;
                    };

                    let storage = storage.clone();
                    let pipeline_clone = pipeline.clone();

                    active_uploads += 1;
                    emit!(ActiveUploads {
                        count: active_uploads,
                        target: pipeline.clone(),
                    });

                    debug!(
                        "[upload] Starting {} (active: {}/{})",
                        file.filename, active_uploads, max_concurrent
                    );

                    uploads.push(Box::pin(upload_file(storage, file, pipeline_clone)));
                }
            }
        }

        // Drain remaining uploads
        while let Some(result) = uploads.next().await {
            active_uploads -= 1;
            emit!(ActiveUploads {
                count: active_uploads,
                target: pipeline.clone(),
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
            let _ = result_tx.send(result).await;
        }

        // Reset gauge on completion
        emit!(ActiveUploads {
            count: 0,
            target: pipeline.clone(),
        });

        debug!("[upload] All uploads complete");
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
}
