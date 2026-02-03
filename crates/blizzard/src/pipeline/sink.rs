//! High-level sink that combines batch writing with async uploads.

use std::path::Path;

use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;
use tracing::{debug, info};

use blizzard_core::metrics::events::{BatchesProcessed, BytesWritten, RecordsProcessed};
use blizzard_core::{PartitionExtractor, emit};

use super::tasks::UploadTask;
use crate::error::PipelineError;
use crate::parquet::{ParquetWriter, ParquetWriterConfig};

/// High-level sink that handles the full output path: serialization and storage.
///
/// Combines batch writing (ParquetWriter), partition extraction, and async
/// upload (UploadTask) into a single interface.
pub(super) struct Sink {
    batch_writer: ParquetWriter,
    upload_task: UploadTask,
    partition_extractor: PartitionExtractor,
    pipeline_id: String,
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
        })
    }

    /// Process batches from a source file: extract partitions, write batches, persist rolled files.
    ///
    /// Returns the number of records written.
    pub async fn write_file_batches(
        &mut self,
        path: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<usize, PipelineError> {
        let partition_values = self.partition_extractor.extract(path);
        self.batch_writer.set_partition_context(partition_values)?;

        let batch_count = batches.len();
        let mut total_records = 0;

        for batch in batches {
            total_records += batch.num_rows();
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

        // Queue any rolled files for upload (non-blocking)
        let finished = self.batch_writer.take_finished_files();
        for file in finished {
            self.upload_task.send(file).await?;
        }

        Ok(total_records)
    }

    /// Finalize the writer and wait for all uploads to complete.
    pub async fn finalize(self) -> Result<(), PipelineError> {
        let finished_files = self.batch_writer.close()?;

        let final_file_count = finished_files.len();
        let final_bytes: usize = finished_files.iter().map(|f| f.size).sum();

        // Queue remaining files for upload
        for file in finished_files {
            self.upload_task.send(file).await?;
        }

        // Wait for all uploads to complete and collect results
        let results = self.upload_task.finalize().await;

        // Check for any upload errors
        let mut total_bytes = 0usize;
        for result in results {
            let uploaded = result?;
            total_bytes += uploaded.size;
        }

        // Add bytes from final files
        total_bytes += final_bytes;

        if final_file_count > 0 {
            info!(
                target = %self.pipeline_id,
                files = final_file_count,
                "Finished writing parquet files to storage"
            );
        }

        if total_bytes > 0 {
            emit!(BytesWritten {
                bytes: total_bytes as u64,
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

    #[tokio::test]
    async fn test_sink_writer_extracts_partitions_and_writes() {
        let temp_dir = TempDir::new().unwrap();
        let dest_uri = temp_dir.path().to_str().unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(dest_uri, HashMap::new())
                .await
                .unwrap(),
        );
        let upload_task = UploadTask::spawn(storage, 4, "test".to_string());
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

        let batches = vec![test_batch(10)];
        let records = writer
            .write_file_batches("date=2024-01-15/file.json", batches)
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
        let upload_task = UploadTask::spawn(storage, 4, "test".to_string());
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
        writer
            .write_file_batches("file1.json", vec![test_batch(5)])
            .await
            .unwrap();
        writer
            .write_file_batches("file2.json", vec![test_batch(10)])
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
