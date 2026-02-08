//! Background tasks for concurrent uploads and downloads.
//!
//! These tasks are spawned by the main pipeline to handle I/O-bound work
//! concurrently while the main loop processes files.

mod discovery;
mod download;
mod upload;

use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::sync::Arc;

use indexmap::IndexMap;
use tokio::sync::mpsc;
use tracing::warn;

use crate::error::{PipelineError, ReaderError};
use crate::source::FileReader;

pub(super) use discovery::DiscoveryTask;
pub(super) use download::{DownloadTask, DownloadedFile};
pub(super) use upload::UploadTask;

/// Result of spawning a read task: metadata + a channel of streaming batches.
pub(super) struct ProcessedFile {
    /// Name of the source this file came from.
    pub source_name: String,
    /// Path to the file within the source.
    pub path: String,
    /// Receiver for streamed record batches (or reader errors).
    pub batch_rx: mpsc::Receiver<Result<deltalake::arrow::array::RecordBatch, ReaderError>>,
}

/// Channel capacity for batch streaming (number of batches buffered).
const BATCH_CHANNEL_CAPACITY: usize = 4;

/// Spawn a blocking task to decompress and parse a downloaded file.
///
/// Creates a bounded channel and spawns the blocking reader which sends
/// batches through the callback. Returns the receiver immediately so the
/// downstream can start consuming batches before parsing finishes.
pub(super) fn spawn_read_task(
    downloaded: DownloadedFile,
    readers: &IndexMap<String, Arc<dyn FileReader>>,
) -> ProcessedFile {
    let DownloadedFile {
        source_name,
        path,
        compressed_data,
    } = downloaded;

    // Get the reader for this source
    let reader = match readers.get(&source_name) {
        Some(r) => r.clone(),
        None => {
            warn!(
                "No reader for source '{}', using first available reader",
                source_name
            );
            readers.values().next().unwrap().clone()
        }
    };

    let (batch_tx, batch_rx) = mpsc::channel(BATCH_CHANNEL_CAPACITY);

    // Clone path for use inside spawn_blocking; the original is returned in ProcessedFile
    let read_path = path.clone();

    // Spawn the blocking reader task that sends batches through the channel
    tokio::task::spawn_blocking(move || {
        let result = reader.read_batches(compressed_data, &read_path, &mut |batch| {
            match batch_tx.blocking_send(Ok(batch)) {
                Ok(()) => ControlFlow::Continue(()),
                Err(_) => ControlFlow::Break(()), // receiver dropped
            }
        });

        // If reading failed, send the error through the channel
        if let Err(e) = result {
            // Ignore send error — receiver may already be dropped
            let _ = batch_tx.blocking_send(Err(e));
        }
    });

    ProcessedFile {
        source_name,
        path,
        batch_rx,
    }
}

/// Result of processing a file through a sink worker.
pub(super) struct CompletedFile {
    pub path: String,
}

/// Tracks files assigned to sink workers and drains only contiguous completions.
///
/// Watermarks use lexicographic max, so out-of-order completion could skip files
/// on crash. This tracker ensures the watermark only advances to the highest
/// contiguous completion point.
pub(super) struct CompletionTracker {
    /// Assigned files in order: (source_name, path, completed).
    assigned: VecDeque<(String, String, bool)>,
}

impl CompletionTracker {
    pub fn new() -> Self {
        Self {
            assigned: VecDeque::new(),
        }
    }

    /// Record that a file has been assigned to a worker.
    pub fn assign(&mut self, source_name: &str, path: &str) {
        self.assigned
            .push_back((source_name.to_string(), path.to_string(), false));
    }

    /// Mark a file as completed by path.
    pub fn mark_completed(&mut self, path: &str) {
        for entry in self.assigned.iter_mut() {
            if entry.1 == path {
                entry.2 = true;
                return;
            }
        }
    }

    /// Drain contiguous completed files from the front.
    ///
    /// Returns an iterator of (source_name, path) for files that can safely
    /// have their watermarks advanced.
    pub fn drain_contiguous(&mut self) -> Vec<(String, String)> {
        let mut result = Vec::new();
        while let Some((_, _, true)) = self.assigned.front() {
            let (source, path, _) = self.assigned.pop_front().unwrap();
            result.push((source, path));
        }
        result
    }
}

/// Run a sink worker that processes files from its channel.
///
/// Each worker owns a `Sink` and processes files sequentially from its input
/// channel, sending completion results back through the result channel.
pub(super) async fn run_sink_worker(
    mut sink: super::sink::Sink,
    mut file_rx: mpsc::Receiver<ProcessedFile>,
    result_tx: mpsc::UnboundedSender<Result<CompletedFile, (CompletedFile, PipelineError)>>,
) {
    while let Some(processed) = file_rx.recv().await {
        let _source_name = processed.source_name;
        let path = processed.path;
        let mut batch_rx = processed.batch_rx;

        let write_result = async {
            sink.start_file(&path)?;

            let mut batch_count: usize = 0;
            let mut total_records: usize = 0;

            while let Some(result) = batch_rx.recv().await {
                let batch = result.map_err(|e| PipelineError::Reader { source: e })?;
                total_records += batch.num_rows();
                batch_count += 1;
                sink.write_batch(&batch).await?;
            }

            sink.end_file(&path, batch_count, total_records).await?;
            Ok::<(), PipelineError>(())
        }
        .await;

        let completed = CompletedFile { path: path.clone() };

        let msg = match write_result {
            Ok(()) => Ok(completed),
            Err(e) => Err((completed, e)),
        };

        if result_tx.send(msg).is_err() {
            break;
        }
    }

    // Finalize this worker's sink (flush remaining data, wait for uploads)
    if let Err(e) = sink.finalize().await {
        warn!(error = %e, "Sink worker finalization failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use deltalake::arrow::array::{Int64Array, RecordBatch, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn test_batch(num_rows: usize) -> RecordBatch {
        let ids: Vec<String> = (0..num_rows).map(|i| format!("id_{i}")).collect();
        let values: Vec<i64> = (0..num_rows).map(|i| i64::try_from(i).unwrap()).collect();
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    /// A mock reader that produces batches with a configurable delay between them.
    struct SlowReader {
        schema: SchemaRef,
        num_batches: usize,
        delay_per_batch: Duration,
        /// Counter incremented after each batch is successfully sent via callback.
        batches_delivered: Arc<AtomicUsize>,
    }

    impl FileReader for SlowReader {
        fn read_batches(
            &self,
            _data: Bytes,
            _path: &str,
            on_batch: &mut dyn FnMut(RecordBatch) -> ControlFlow<()>,
        ) -> Result<usize, ReaderError> {
            let mut total = 0;
            for _ in 0..self.num_batches {
                std::thread::sleep(self.delay_per_batch);
                let batch = test_batch(10);
                total += batch.num_rows();
                if on_batch(batch).is_break() {
                    break;
                }
                // Increment *after* on_batch returns (i.e. after blocking_send completes),
                // so this counter reflects batches that actually made it into the channel.
                self.batches_delivered.fetch_add(1, Ordering::SeqCst);
            }
            Ok(total)
        }

        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
    }

    fn make_downloaded(source: &str) -> DownloadedFile {
        DownloadedFile {
            source_name: source.to_string(),
            path: "test.ndjson".to_string(),
            compressed_data: Bytes::new(), // SlowReader ignores data
        }
    }

    /// Proves pipelining: the first batch arrives well before the reader finishes
    /// producing all batches.
    #[tokio::test]
    async fn test_streaming_delivers_batches_before_reader_finishes() {
        let batches_delivered = Arc::new(AtomicUsize::new(0));
        let reader: Arc<dyn FileReader> = Arc::new(SlowReader {
            schema: test_schema(),
            num_batches: 10,
            delay_per_batch: Duration::from_millis(50),
            batches_delivered: batches_delivered.clone(),
        });

        let mut readers = IndexMap::new();
        readers.insert("test".to_string(), reader);

        let processed = spawn_read_task(make_downloaded("test"), &readers);
        let mut batch_rx = processed.batch_rx;

        let start = Instant::now();

        // First batch should arrive after ~50ms, not after all 500ms
        let first = batch_rx.recv().await;
        assert!(first.is_some());
        let first_batch_time = start.elapsed();

        // The total reader time is ~500ms (10 * 50ms).
        // The first batch should arrive much sooner than that.
        assert!(
            first_batch_time < Duration::from_millis(300),
            "First batch took {first_batch_time:?}, expected < 300ms (streaming should deliver early)",
        );

        // Not all batches should have been delivered yet
        let sent_so_far = batches_delivered.load(Ordering::SeqCst);
        assert!(
            sent_so_far < 10,
            "Only {sent_so_far} batches delivered when first was received — proves pipelining",
        );

        // Drain remaining batches
        while batch_rx.recv().await.is_some() {}
    }

    /// Proves backpressure: when the consumer doesn't read, the reader blocks
    /// after filling the bounded channel.
    #[tokio::test]
    async fn test_streaming_backpressure_blocks_reader() {
        let batches_delivered = Arc::new(AtomicUsize::new(0));
        let reader: Arc<dyn FileReader> = Arc::new(SlowReader {
            schema: test_schema(),
            num_batches: 20,
            delay_per_batch: Duration::ZERO, // produce as fast as possible
            batches_delivered: batches_delivered.clone(),
        });

        let mut readers = IndexMap::new();
        readers.insert("test".to_string(), reader);

        let processed = spawn_read_task(make_downloaded("test"), &readers);
        let mut batch_rx = processed.batch_rx;

        // Give the reader time to fill the channel and block
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The reader should be blocked after filling the bounded channel.
        // At most BATCH_CHANNEL_CAPACITY sends complete (filling the buffer),
        // then the next blocking_send blocks.
        let sent = batches_delivered.load(Ordering::SeqCst);
        assert!(
            sent <= BATCH_CHANNEL_CAPACITY,
            "Reader delivered {sent} batches but channel capacity is {BATCH_CHANNEL_CAPACITY} \
             — backpressure should have blocked it",
        );
        assert!(
            sent < 20,
            "Reader delivered all 20 batches without backpressure",
        );

        // Now consume all batches — the reader should unblock and finish
        let mut received = 0;
        while batch_rx.recv().await.is_some() {
            received += 1;
        }
        assert_eq!(received, 20);
        assert_eq!(batches_delivered.load(Ordering::SeqCst), 20);
    }

    #[test]
    fn test_completion_tracker_drains_contiguous() {
        let mut tracker = CompletionTracker::new();
        tracker.assign("src", "a.json");
        tracker.assign("src", "b.json");
        tracker.assign("src", "c.json");

        // Complete out of order: c, then a
        tracker.mark_completed("c.json");
        assert!(tracker.drain_contiguous().is_empty());

        tracker.mark_completed("a.json");
        let drained = tracker.drain_contiguous();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].1, "a.json");

        // Now complete b — both b and c should drain
        tracker.mark_completed("b.json");
        let drained = tracker.drain_contiguous();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].1, "b.json");
        assert_eq!(drained[1].1, "c.json");
    }

    #[test]
    fn test_completion_tracker_empty() {
        let mut tracker = CompletionTracker::new();
        assert!(tracker.drain_contiguous().is_empty());
    }

    #[test]
    fn test_completion_tracker_all_in_order() {
        let mut tracker = CompletionTracker::new();
        tracker.assign("src", "a.json");
        tracker.assign("src", "b.json");

        tracker.mark_completed("a.json");
        let drained = tracker.drain_contiguous();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].1, "a.json");

        tracker.mark_completed("b.json");
        let drained = tracker.drain_contiguous();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].1, "b.json");
    }
}

#[cfg(test)]
mod discovery_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use indexmap::IndexMap;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    use super::DiscoveryTask;
    use crate::pipeline::tracker::{DiscoverySnapshot, DiscoverySource};

    async fn create_test_storage(temp_dir: &TempDir) -> blizzard_core::StorageProviderRef {
        Arc::new(
            blizzard_core::storage::StorageProvider::for_url_with_options(
                temp_dir.path().to_str().unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap(),
        )
    }

    fn create_files(temp_dir: &TempDir, paths: &[&str]) {
        for path in paths {
            let full_path = temp_dir.path().join(path);
            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(full_path, b"").unwrap();
        }
    }

    /// Discovery with multiple sources finds files from all sources
    /// and returns the correct total count.
    #[tokio::test]
    async fn test_discovery_multiple_sources() {
        let dir_a = TempDir::new().unwrap();
        create_files(&dir_a, &["a1.ndjson.gz", "a2.ndjson.gz"]);
        let storage_a = create_test_storage(&dir_a).await;

        let dir_b = TempDir::new().unwrap();
        create_files(&dir_b, &["b1.ndjson.gz", "b2.ndjson.gz", "b3.ndjson.gz"]);
        let storage_b = create_test_storage(&dir_b).await;

        let mut sources = IndexMap::new();
        sources.insert(
            "source_a".to_string(),
            DiscoverySource {
                storage: storage_a,
                snapshot: DiscoverySnapshot::Processed {
                    processed_files: HashMap::new(),
                },
                prefixes: None,
            },
        );
        sources.insert(
            "source_b".to_string(),
            DiscoverySource {
                storage: storage_b,
                snapshot: DiscoverySnapshot::Processed {
                    processed_files: HashMap::new(),
                },
                prefixes: None,
            },
        );

        let shutdown = CancellationToken::new();
        let mut task = DiscoveryTask::spawn(sources, shutdown, "test".to_string());

        let mut received = Vec::new();
        while let Some(file) = task.rx.recv().await {
            received.push(file);
        }

        let total = task.handle.await.unwrap().unwrap();
        assert_eq!(total, 5);
        assert_eq!(received.len(), 5);

        let from_a: Vec<_> = received
            .iter()
            .filter(|f| f.source_name == "source_a")
            .collect();
        let from_b: Vec<_> = received
            .iter()
            .filter(|f| f.source_name == "source_b")
            .collect();
        assert_eq!(from_a.len(), 2);
        assert_eq!(from_b.len(), 3);
    }

    /// Discovery with zero sources completes immediately with count 0.
    #[tokio::test]
    async fn test_discovery_no_sources() {
        let sources = IndexMap::new();
        let shutdown = CancellationToken::new();
        let mut task = DiscoveryTask::spawn(sources, shutdown, "test".to_string());

        assert!(task.rx.recv().await.is_none());
        let total = task.handle.await.unwrap().unwrap();
        assert_eq!(total, 0);
    }

    /// Pre-cancelled shutdown token causes discovery to return 0 files.
    #[tokio::test]
    async fn test_discovery_shutdown_skips_listing() {
        let dir = TempDir::new().unwrap();
        create_files(&dir, &["file.ndjson.gz"]);
        let storage = create_test_storage(&dir).await;

        let mut sources = IndexMap::new();
        sources.insert(
            "src".to_string(),
            DiscoverySource {
                storage,
                snapshot: DiscoverySnapshot::Processed {
                    processed_files: HashMap::new(),
                },
                prefixes: None,
            },
        );

        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let mut task = DiscoveryTask::spawn(sources, shutdown, "test".to_string());

        let mut received = Vec::new();
        while let Some(file) = task.rx.recv().await {
            received.push(file);
        }

        let total = task.handle.await.unwrap().unwrap();
        assert_eq!(total, 0);
        assert!(received.is_empty());
    }

    /// Channel closes properly when discovery finishes.
    #[tokio::test]
    async fn test_discovery_channel_closes_on_completion() {
        let dir = TempDir::new().unwrap();
        create_files(&dir, &["f.ndjson.gz"]);
        let storage = create_test_storage(&dir).await;

        let mut sources = IndexMap::new();
        sources.insert(
            "src".to_string(),
            DiscoverySource {
                storage,
                snapshot: DiscoverySnapshot::Processed {
                    processed_files: HashMap::new(),
                },
                prefixes: None,
            },
        );

        let shutdown = CancellationToken::new();
        let mut task = DiscoveryTask::spawn(sources, shutdown, "test".to_string());

        while task.rx.recv().await.is_some() {}
        let total = task.handle.await.unwrap().unwrap();
        assert_eq!(total, 1);
    }
}
