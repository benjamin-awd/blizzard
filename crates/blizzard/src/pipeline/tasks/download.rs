//! Background download task.

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use blizzard_core::StorageProviderRef;
use blizzard_core::emit;
use blizzard_core::error::StorageError;
use blizzard_core::metrics::UtilizationTimer;
use blizzard_core::metrics::events::{ActiveDownloads, FileDownloadCompleted};

/// Future type for download operations.
type DownloadFuture = Pin<Box<dyn Future<Output = Result<DownloadedFile, StorageError>> + Send>>;

/// Downloaded file ready for processing.
pub(in crate::pipeline) struct DownloadedFile {
    pub path: String,
    pub compressed_data: Bytes,
}

/// Handle to the background downloader task.
pub(in crate::pipeline) struct DownloadTask {
    pub rx: mpsc::Receiver<Result<DownloadedFile, StorageError>>,
    handle: JoinHandle<()>,
}

impl DownloadTask {
    /// Spawn the downloader task.
    pub fn spawn(
        pending_files: Vec<String>,
        storage: StorageProviderRef,
        shutdown: CancellationToken,
        max_concurrent: usize,
        pipeline: String,
    ) -> Self {
        let (tx, rx) = mpsc::channel(max_concurrent);

        let handle = tokio::spawn(Self::run(
            pending_files,
            storage,
            tx,
            shutdown,
            max_concurrent,
            pipeline,
        ));

        Self { rx, handle }
    }

    /// Abort the downloader task.
    pub fn abort(self) {
        drop(self.rx);
        self.handle.abort();
    }

    /// Run the downloader task that manages concurrent file downloads.
    async fn run(
        pending_files: Vec<String>,
        storage: StorageProviderRef,
        download_tx: mpsc::Sender<Result<DownloadedFile, StorageError>>,
        shutdown: CancellationToken,
        max_concurrent: usize,
        pipeline: String,
    ) {
        let mut downloads: FuturesUnordered<DownloadFuture> = FuturesUnordered::new();

        let mut pending_iter = pending_files.into_iter();
        let mut active_downloads = 0;
        let mut util_timer = UtilizationTimer::new("downloader");

        // Start initial downloads
        for file_path in pending_iter.by_ref().take(max_concurrent) {
            let storage = storage.clone();
            let pipeline_clone = pipeline.clone();
            // First download starts working state
            if active_downloads == 0 {
                util_timer.stop_wait();
            }
            active_downloads += 1;
            emit!(ActiveDownloads {
                count: active_downloads,
                target: pipeline.clone(),
            });
            debug!(
                "[download] Starting {} (active: {})",
                file_path, active_downloads
            );
            downloads.push(Box::pin(download_file(storage, file_path, pipeline_clone)));
        }

        // Process downloads and start new ones as they complete
        while let Some(result) = downloads.next().await {
            util_timer.maybe_update();

            if shutdown.is_cancelled() {
                debug!("[download] Shutdown requested, stopping downloads");
                break;
            }

            active_downloads -= 1;
            emit!(ActiveDownloads {
                count: active_downloads,
                target: pipeline.clone(),
            });

            // Update utilization state: waiting if no active downloads
            if active_downloads == 0 {
                util_timer.start_wait();
            }

            // Send result to consumer (decompress+parse)
            let should_continue = match &result {
                Ok(downloaded) => {
                    debug!(
                        "[download] Completed {} ({} bytes)",
                        downloaded.path,
                        downloaded.compressed_data.len()
                    );
                    true
                }
                Err(e) => {
                    // Skip 404 errors, propagate others
                    if e.is_not_found() {
                        warn!("[download] Skipping missing file: {}", e);
                        false // Don't send error, just skip
                    } else {
                        true // Send error to consumer
                    }
                }
            };

            if should_continue && download_tx.send(result).await.is_err() {
                debug!("[download] Consumer closed, stopping downloads");
                break;
            }

            // Start next download if available
            if let Some(next_file) = pending_iter.next() {
                let storage = storage.clone();
                let pipeline_clone = pipeline.clone();
                // Transition to working state when we have downloads
                if active_downloads == 0 {
                    util_timer.stop_wait();
                }
                active_downloads += 1;
                emit!(ActiveDownloads {
                    count: active_downloads,
                    target: pipeline.clone(),
                });
                debug!(
                    "[download] Starting {} (active: {})",
                    next_file, active_downloads
                );
                downloads.push(Box::pin(download_file(storage, next_file, pipeline_clone)));
            }
        }

        // Reset gauge to 0 on completion
        emit!(ActiveDownloads {
            count: 0,
            target: pipeline
        });
        debug!("[download] All downloads complete");
    }
}

/// Download a file's compressed data asynchronously.
async fn download_file(
    storage: StorageProviderRef,
    path: String,
    pipeline: String,
) -> Result<DownloadedFile, StorageError> {
    let start = Instant::now();
    let compressed_data = storage.get(path.as_str()).await?;
    emit!(FileDownloadCompleted {
        duration: start.elapsed(),
        target: pipeline,
    });
    Ok(DownloadedFile {
        path,
        compressed_data,
    })
}
