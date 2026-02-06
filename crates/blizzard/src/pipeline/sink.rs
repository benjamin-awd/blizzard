//! High-level sink that combines batch writing with async uploads.

use std::path::Path;

use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;
use tokio::sync::mpsc;
use tracing::{debug, info};

use blizzard_core::metrics::events::{BatchesProcessed, BytesWritten, RecordsProcessed};
use blizzard_core::{PartitionExtractor, emit};

use super::tasks::UploadTask;
use crate::error::{PipelineError, ReaderError};
use crate::parquet::{ParquetWriter, ParquetWriterConfig};

/// High-level sink that handles the full output path: serialization and storage.
///
/// Combines batch writing (ParquetWriter), partition extraction, and async
/// upload (UploadTask) into a single interface.
///
/// Uses a streaming pattern for uploads: results are drained during operation
/// via `try_recv()` rather than accumulated until finalize.
pub(super) struct Sink {
    batch_writer: ParquetWriter,
    upload_task: UploadTask,
    partition_extractor: PartitionExtractor,
    pipeline_id: String,
    /// Bytes uploaded so far (tracked during streaming)
    uploaded_bytes: usize,
}

impl Sink {
    /// Create a new sink writer.
    pub fn new(
        schema: SchemaRef,
        writer_config: ParquetWriterConfig,
        upload_task: UploadTask,
        partition_extractor: PartitionExtractor,
        pipeline_id: String,
    ) -> Result<Self, PipelineError> {
        let batch_writer = ParquetWriter::new(schema, writer_config, pipeline_id.clone())?;

        Ok(Self {
            batch_writer,
            upload_task,
            partition_extractor,
            pipeline_id,
            uploaded_bytes: 0,
        })
    }

    /// Drain available upload results, tracking bytes and propagating errors.
    fn drain_upload_results(&mut self) -> Result<(), PipelineError> {
        while let Some(result) = self.upload_task.try_recv() {
            let uploaded = result?;
            self.uploaded_bytes += uploaded.size;
        }
        Ok(())
    }

    /// Consume batches from a channel, writing each to parquet as it arrives.
    ///
    /// Returns the number of records written.
    pub async fn write_file_stream(
        &mut self,
        path: &str,
        batch_rx: &mut mpsc::Receiver<Result<RecordBatch, ReaderError>>,
    ) -> Result<usize, PipelineError> {
        let partition_values = self.partition_extractor.extract(path);
        self.batch_writer.set_partition_context(partition_values)?;

        let mut batch_count: usize = 0;
        let mut total_records: usize = 0;

        while let Some(batch_result) = batch_rx.recv().await {
            let batch = batch_result.map_err(|e| PipelineError::Reader { source: e })?;

            total_records += batch.num_rows();
            batch_count += 1;
            self.batch_writer.write_batch(&batch)?;
        }

        emit!(RecordsProcessed {
            count: total_records as u64,
            target: self.pipeline_id.clone(),
        });
        emit!(BatchesProcessed {
            count: batch_count as u64,
            target: self.pipeline_id.clone(),
        });

        let short_name = Path::new(path)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(path);
        debug!(
            target = %self.pipeline_id,
            file = short_name,
            records = total_records,
            batches = batch_count,
            "Processed file"
        );

        // Drain completed uploads to prevent channel backpressure
        self.drain_upload_results()?;

        // Queue any rolled files for upload
        let finished = self.batch_writer.take_finished_files();
        for file in finished {
            self.upload_task.send(file).await?;
        }

        Ok(total_records)
    }

    /// Finalize the writer and wait for all uploads to complete.
    pub async fn finalize(mut self) -> Result<(), PipelineError> {
        // Drain any pending results before closing
        self.drain_upload_results()?;

        let finished_files = self.batch_writer.close()?;
        let final_file_count = finished_files.len();

        // Queue remaining files for upload
        for file in finished_files {
            self.upload_task.send(file).await?;
        }

        // Wait for all uploads to complete and collect remaining results
        let results = self.upload_task.finalize().await;

        // Check for any upload errors and accumulate bytes
        for result in results {
            let uploaded = result?;
            self.uploaded_bytes += uploaded.size;
        }

        if final_file_count > 0 {
            info!(
                target = %self.pipeline_id,
                files = final_file_count,
                "Finished writing parquet files to storage"
            );
        }

        if self.uploaded_bytes > 0 {
            emit!(BytesWritten {
                bytes: self.uploaded_bytes as u64,
                target: self.pipeline_id.clone(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blizzard_core::StorageProvider;
    use deltalake::arrow::array::{Int64Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

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

    /// Helper: send batches through a channel and return the receiver.
    fn send_batches(batches: Vec<RecordBatch>) -> mpsc::Receiver<Result<RecordBatch, ReaderError>> {
        let (tx, rx) = mpsc::channel(batches.len().max(1));
        for batch in batches {
            tx.try_send(Ok(batch)).unwrap();
        }
        // Drop tx so the receiver will see the channel close
        drop(tx);
        rx
    }

    #[tokio::test]
    async fn test_sink_writer_extracts_partitions_and_writes() {
        let temp_dir = TempDir::new().unwrap();
        let dest_uri = temp_dir.path().to_str().unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(dest_uri, HashMap::new())
                .await
                .unwrap(),
        );
        let upload_task = UploadTask::spawn(storage, 4, None, "test".to_string());
        let partition_extractor = PartitionExtractor::new(vec!["date".into()]);
        let writer_config = ParquetWriterConfig::default();

        let mut writer = Sink::new(
            test_schema(),
            writer_config,
            upload_task,
            partition_extractor,
            "test".to_string(),
        )
        .unwrap();

        let mut rx = send_batches(vec![test_batch(10)]);
        let records = writer
            .write_file_stream("date=2024-01-15/file.json", &mut rx)
            .await
            .unwrap();

        assert_eq!(records, 10);

        writer.finalize().await.unwrap();
    }

    #[tokio::test]
    async fn test_sink_writer_writes_multiple_files() {
        let temp_dir = TempDir::new().unwrap();
        let dest_uri = temp_dir.path().to_str().unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(dest_uri, HashMap::new())
                .await
                .unwrap(),
        );
        let upload_task = UploadTask::spawn(storage, 4, None, "test".to_string());
        let partition_extractor = PartitionExtractor::all();
        let writer_config = ParquetWriterConfig::default();

        let mut writer = Sink::new(
            test_schema(),
            writer_config,
            upload_task,
            partition_extractor,
            "test".to_string(),
        )
        .unwrap();

        // Write multiple files
        let mut rx1 = send_batches(vec![test_batch(5)]);
        writer
            .write_file_stream("file1.json", &mut rx1)
            .await
            .unwrap();
        let mut rx2 = send_batches(vec![test_batch(10)]);
        writer
            .write_file_stream("file2.json", &mut rx2)
            .await
            .unwrap();

        writer.finalize().await.unwrap();

        // Verify files were written
        let files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|x| x == "parquet")
                    .unwrap_or(false)
            })
            .collect();
        assert!(!files.is_empty());
    }
}
