//! Background upload task.

use futures::stream::{FuturesUnordered, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
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
    pub fn spawn(
        storage: StorageProviderRef,
        shutdown: CancellationToken,
        max_concurrent: usize,
        pipeline: String,
    ) -> Self {
        let (file_tx, file_rx) = mpsc::channel(max_concurrent);
        let (result_tx, result_rx) = mpsc::channel(max_concurrent);

        let handle = tokio::spawn(Self::run(
            file_rx,
            result_tx,
            storage,
            shutdown,
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

    /// Abort the uploader task.
    pub fn abort(self) {
        drop(self.tx);
        drop(self.rx);
        self.handle.abort();
    }

    /// Run the uploader task that manages concurrent file uploads.
    async fn run(
        mut file_rx: mpsc::Receiver<FinishedFile>,
        result_tx: mpsc::Sender<Result<UploadedFile, TableWriteError>>,
        storage: StorageProviderRef,
        shutdown: CancellationToken,
        max_concurrent: usize,
        pipeline: String,
    ) {
        let mut uploads: FuturesUnordered<UploadFuture> = FuturesUnordered::new();
        let mut active_uploads: usize = 0;

        loop {
            tokio::select! {
                biased;

                // Check shutdown first
                _ = shutdown.cancelled() => {
                    debug!("[upload] Shutdown requested, stopping uploads");
                    break;
                }

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
                            warn!("[upload] Upload failed: {}", e);
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
                        debug!("[upload] Input channel closed, draining {} remaining uploads", uploads.len());
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
                    warn!("[upload] Upload failed during drain: {}", e);
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
