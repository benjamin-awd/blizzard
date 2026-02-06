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
    /// Reads files from the discovery channel and downloads them concurrently,
    /// sending results to the consumer as they complete.
    ///
    /// # Arguments
    /// * `file_rx` - Channel receiving discovered files to download
    /// * `storages` - Per-source storage providers
    /// * `shutdown` - Cancellation token for graceful shutdown
    /// * `max_concurrent` - Maximum concurrent downloads
    /// * `global_semaphore` - Optional global semaphore for cross-pipeline concurrency limiting
    /// * `pipeline` - Pipeline identifier for metrics
    pub fn spawn(
        file_rx: mpsc::Receiver<SourcedFile>,
        storages: IndexMap<String, StorageProviderRef>,
        shutdown: CancellationToken,
        max_concurrent: usize,
        global_semaphore: Option<Arc<Semaphore>>,
        pipeline: String,
    ) -> Self {
        let (tx, rx) = mpsc::channel(max_concurrent);

        let handle = tokio::spawn(Self::run(
            file_rx,
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

    /// Start a download for a single file, returning whether it was started.
    fn start_download(
        sourced_file: SourcedFile,
        storages: &IndexMap<String, StorageProviderRef>,
        global_semaphore: &Option<Arc<Semaphore>>,
        pipeline: &str,
        active_downloads: &mut usize,
        downloads: &mut FuturesUnordered<DownloadFuture>,
    ) -> bool {
        let storage = match storages.get(&sourced_file.source_name) {
            Some(s) => s.clone(),
            None => {
                warn!(
                    "[download] No storage for source '{}', skipping {}",
                    sourced_file.source_name, sourced_file.path
                );
                return false;
            }
        };

        *active_downloads += 1;
        emit!(ActiveDownloads {
            count: *active_downloads,
            target: pipeline.to_string(),
        });
        debug!(
            "[download] Starting {}:{} (active: {})",
            sourced_file.source_name, sourced_file.path, *active_downloads
        );

        let semaphore = global_semaphore.clone();
        let pipeline_clone = pipeline.to_string();
        downloads.push(Box::pin(async move {
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
        true
    }

    /// Run the downloader task that manages concurrent file downloads.
    ///
    /// Uses `select!` to concurrently:
    /// - Accept new files from the discovery channel
    /// - Process completed downloads
    async fn run(
        mut file_rx: mpsc::Receiver<SourcedFile>,
        storages: IndexMap<String, StorageProviderRef>,
        download_tx: mpsc::Sender<Result<DownloadedFile, StorageError>>,
        shutdown: CancellationToken,
        max_concurrent: usize,
        global_semaphore: Option<Arc<Semaphore>>,
        pipeline: String,
    ) {
        let mut downloads: FuturesUnordered<DownloadFuture> = FuturesUnordered::new();
        let mut active_downloads: usize = 0;
        let mut discovery_done = false;

        loop {
            if shutdown.is_cancelled() {
                debug!("[download] Shutdown requested, stopping downloads");
                break;
            }

            // If discovery is done and no active downloads, we're finished
            if discovery_done && downloads.is_empty() {
                break;
            }

            tokio::select! {
                biased;

                // Accept new files from discovery (only if under concurrency limit and discovery active)
                Some(sourced_file) = file_rx.recv(), if !discovery_done && active_downloads < max_concurrent => {
                    Self::start_download(
                        sourced_file,
                        &storages,
                        &global_semaphore,
                        &pipeline,
                        &mut active_downloads,
                        &mut downloads,
                    );
                }

                // Process completed downloads
                Some(result) = downloads.next(), if !downloads.is_empty() => {
                    active_downloads -= 1;
                    emit!(ActiveDownloads {
                        count: active_downloads,
                        target: pipeline.clone(),
                    });

                    let should_send = match &result {
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
                            if e.is_not_found() {
                                warn!("[download] Skipping missing file: {e}");
                                false
                            } else {
                                true
                            }
                        }
                    };

                    if should_send && download_tx.send(result).await.is_err() {
                        debug!("[download] Consumer closed, stopping downloads");
                        break;
                    }
                }

                // Discovery channel closed — no more files coming
                else => {
                    discovery_done = true;
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;
    use tokio::sync::mpsc;

    async fn create_test_storage(temp_dir: &TempDir) -> StorageProviderRef {
        Arc::new(
            blizzard_core::storage::StorageProvider::for_url_with_options(
                temp_dir.path().to_str().unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap(),
        )
    }

    /// Helper to create a file on disk and return the SourcedFile.
    fn create_source_file(temp_dir: &TempDir, source: &str, path: &str) -> SourcedFile {
        let full_path = temp_dir.path().join(path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&full_path, format!("data for {path}")).unwrap();
        SourcedFile {
            source_name: source.to_string(),
            path: path.to_string(),
        }
    }

    /// Downloads start as files arrive from the discovery channel — files sent
    /// one at a time are each downloaded before the next is sent.
    #[tokio::test]
    async fn test_downloads_start_as_files_arrive() {
        let temp_dir = TempDir::new().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let mut storages = IndexMap::new();
        storages.insert("src".to_string(), storage);

        let file1 = create_source_file(&temp_dir, "src", "file1.ndjson.gz");
        let file2 = create_source_file(&temp_dir, "src", "file2.ndjson.gz");

        let (file_tx, file_rx) = mpsc::channel(16);
        let shutdown = CancellationToken::new();

        let mut task = DownloadTask::spawn(file_rx, storages, shutdown, 4, None, "test".into());

        // Send first file
        file_tx.send(file1).await.unwrap();

        // Should receive the first download before sending second
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), task.rx.recv()).await;
        let downloaded = result
            .expect("timed out waiting for first download")
            .expect("channel closed")
            .expect("download failed");
        assert_eq!(downloaded.path, "file1.ndjson.gz");

        // Send second file
        file_tx.send(file2).await.unwrap();

        let result = tokio::time::timeout(std::time::Duration::from_secs(2), task.rx.recv()).await;
        let downloaded = result
            .expect("timed out waiting for second download")
            .expect("channel closed")
            .expect("download failed");
        assert_eq!(downloaded.path, "file2.ndjson.gz");

        // Close discovery channel
        drop(file_tx);

        // Task should complete — no more downloads
        let final_result =
            tokio::time::timeout(std::time::Duration::from_secs(2), task.rx.recv()).await;
        assert!(
            final_result.expect("timed out").is_none(),
            "channel should close after discovery ends"
        );
    }

    /// When the discovery channel closes immediately (no files), the download
    /// task completes without sending anything.
    #[tokio::test]
    async fn test_empty_discovery_channel_completes_immediately() {
        let temp_dir = TempDir::new().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let mut storages = IndexMap::new();
        storages.insert("src".to_string(), storage);

        let (file_tx, file_rx) = mpsc::channel(16);
        let shutdown = CancellationToken::new();

        let mut task = DownloadTask::spawn(file_rx, storages, shutdown, 4, None, "test".into());

        // Close channel immediately — no files to discover
        drop(file_tx);

        // The download task should close its output channel
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), task.rx.recv()).await;
        assert!(
            result.expect("timed out").is_none(),
            "should close cleanly with no downloads"
        );
    }

    /// Skips 404 files without propagating errors to the consumer.
    #[tokio::test]
    async fn test_missing_file_skipped() {
        let temp_dir = TempDir::new().unwrap();
        let storage = create_test_storage(&temp_dir).await;
        let mut storages = IndexMap::new();
        storages.insert("src".to_string(), storage);

        // file2 exists on disk, file1 does not
        let file_missing = SourcedFile {
            source_name: "src".to_string(),
            path: "nonexistent.ndjson.gz".to_string(),
        };
        let file_ok = create_source_file(&temp_dir, "src", "exists.ndjson.gz");

        let (file_tx, file_rx) = mpsc::channel(16);
        let shutdown = CancellationToken::new();

        let mut task = DownloadTask::spawn(file_rx, storages, shutdown, 4, None, "test".into());

        file_tx.send(file_missing).await.unwrap();
        file_tx.send(file_ok).await.unwrap();
        drop(file_tx);

        // Should only receive the successful download (404 is skipped)
        let mut results = Vec::new();
        while let Some(result) = task.rx.recv().await {
            results.push(result);
        }

        assert_eq!(results.len(), 1);
        let downloaded = results.pop().unwrap().unwrap();
        assert_eq!(downloaded.path, "exists.ndjson.gz");
    }
}
