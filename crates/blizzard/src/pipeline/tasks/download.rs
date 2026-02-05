//! Background download task.

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use indexmap::IndexMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use blizzard_core::StorageProviderRef;
use blizzard_core::emit;
use blizzard_core::error::StorageError;
use blizzard_core::metrics::UtilizationTimer;
use blizzard_core::metrics::events::{ActiveDownloads, FileDownloadCompleted};

use super::super::tracker::SourcedFile;

/// Future type for download operations.
type DownloadFuture = Pin<Box<dyn Future<Output = Result<DownloadedFile, StorageError>> + Send>>;

/// Downloaded file ready for processing.
pub(in crate::pipeline) struct DownloadedFile {
    /// Name of the source this file came from.
    pub source_name: String,
    /// Path to the file within the source.
    pub path: String,
    /// Raw compressed data.
    pub compressed_data: Bytes,
}

/// Handle to the background downloader task.
pub(in crate::pipeline) struct DownloadTask {
    pub rx: mpsc::Receiver<Result<DownloadedFile, StorageError>>,
    handle: JoinHandle<()>,
}

impl DownloadTask {
    /// Spawn the downloader task.
    ///
    /// # Arguments
    /// * `pending_files` - Source-tagged files to download
    /// * `storages` - Per-source storage providers
    /// * `shutdown` - Cancellation token for graceful shutdown
    /// * `max_concurrent` - Maximum concurrent downloads
    /// * `global_semaphore` - Optional global semaphore for cross-pipeline concurrency limiting
    /// * `pipeline` - Pipeline identifier for metrics
    pub fn spawn(
        pending_files: Vec<SourcedFile>,
        storages: IndexMap<String, StorageProviderRef>,
        shutdown: CancellationToken,
        max_concurrent: usize,
        global_semaphore: Option<Arc<Semaphore>>,
        pipeline: String,
    ) -> Self {
        let (tx, rx) = mpsc::channel(max_concurrent);

        let handle = tokio::spawn(Self::run(
            pending_files,
            storages,
            tx,
            shutdown,
            max_concurrent,
            global_semaphore,
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
        pending_files: Vec<SourcedFile>,
        storages: IndexMap<String, StorageProviderRef>,
        download_tx: mpsc::Sender<Result<DownloadedFile, StorageError>>,
        shutdown: CancellationToken,
        max_concurrent: usize,
        global_semaphore: Option<Arc<Semaphore>>,
        pipeline: String,
    ) {
        let mut downloads: FuturesUnordered<DownloadFuture> = FuturesUnordered::new();

        let mut pending_iter = pending_files.into_iter();
        let mut active_downloads = 0;
        let mut util_timer = UtilizationTimer::new("downloader");

        // Start initial downloads
        for sourced_file in pending_iter.by_ref().take(max_concurrent) {
            let storage = match storages.get(&sourced_file.source_name) {
                Some(s) => s.clone(),
                None => {
                    warn!(
                        "[download] No storage for source '{}', skipping {}",
                        sourced_file.source_name, sourced_file.path
                    );
                    continue;
                }
            };
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
                "[download] Starting {}:{} (active: {})",
                sourced_file.source_name, sourced_file.path, active_downloads
            );
            let semaphore = global_semaphore.clone();
            downloads.push(Box::pin(async move {
                // Acquire global semaphore permit if configured
                let _permit = if let Some(ref sem) = semaphore {
                    Some(sem.acquire().await.expect("semaphore should not be closed"))
                } else {
                    None
                };
                download_file(
                    storage,
                    sourced_file.source_name,
                    sourced_file.path,
                    pipeline_clone,
                )
                .await
            }));
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
                        "[download] Completed {}:{} ({} bytes)",
                        downloaded.source_name,
                        downloaded.path,
                        downloaded.compressed_data.len()
                    );
                    true
                }
                Err(e) => {
                    // Skip 404 errors, propagate others
                    if e.is_not_found() {
                        warn!("[download] Skipping missing file: {e}");
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
                let storage = match storages.get(&next_file.source_name) {
                    Some(s) => s.clone(),
                    None => {
                        warn!(
                            "[download] No storage for source '{}', skipping {}",
                            next_file.source_name, next_file.path
                        );
                        continue;
                    }
                };
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
                    "[download] Starting {}:{} (active: {})",
                    next_file.source_name, next_file.path, active_downloads
                );
                let semaphore = global_semaphore.clone();
                downloads.push(Box::pin(async move {
                    // Acquire global semaphore permit if configured
                    let _permit = if let Some(ref sem) = semaphore {
                        Some(sem.acquire().await.expect("semaphore should not be closed"))
                    } else {
                        None
                    };
                    download_file(
                        storage,
                        next_file.source_name,
                        next_file.path,
                        pipeline_clone,
                    )
                    .await
                }));
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
    source_name: String,
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
        source_name,
        path,
        compressed_data,
    })
}
