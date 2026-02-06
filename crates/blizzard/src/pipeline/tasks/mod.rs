//! Background tasks for concurrent uploads and downloads.
//!
//! These tasks are spawned by the main pipeline to handle I/O-bound work
//! concurrently while the main loop processes files.

mod discovery;
mod download;
mod upload;

use std::ops::ControlFlow;
use std::sync::Arc;

use indexmap::IndexMap;
use tokio::sync::mpsc;
use tracing::warn;

use crate::error::ReaderError;
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
}
