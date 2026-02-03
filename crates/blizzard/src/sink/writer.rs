//! High-level sink writer that combines batch writing with storage persistence.

use std::path::Path;

use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;
use tracing::{debug, info};

use blizzard_core::metrics::events::{BatchesProcessed, BytesWritten, RecordsProcessed};
use blizzard_core::{PartitionExtractor, emit};

use super::storage::StorageWriter;
use super::{ParquetWriter, ParquetWriterConfig};
use crate::error::PipelineError;

/// High-level writer that handles the full output path: serialization and storage.
///
/// Combines batch writing (ParquetWriter), partition extraction, and storage
/// persistence (StorageWriter) into a single interface.
pub struct SinkWriter {
    batch_writer: ParquetWriter,
    storage_writer: StorageWriter,
    partition_extractor: PartitionExtractor,
    pipeline_id: String,
}

impl SinkWriter {
    /// Create a new sink writer.
    pub fn new(
        schema: SchemaRef,
        writer_config: ParquetWriterConfig,
        storage_writer: StorageWriter,
        partition_extractor: PartitionExtractor,
        pipeline_id: String,
    ) -> Result<Self, PipelineError> {
        let batch_writer = ParquetWriter::new(schema, writer_config, pipeline_id.clone())?;

        Ok(Self {
            batch_writer,
            storage_writer,
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

        // Persist any rolled files
        let finished = self.batch_writer.take_finished_files();
        if !finished.is_empty() {
            self.storage_writer.write_files(&finished).await?;
        }

        Ok(total_records)
    }

    /// Finalize the writer and persist all remaining files.
    pub async fn finalize(self) -> Result<(), PipelineError> {
        let finished_files = self.batch_writer.close()?;

        if !finished_files.is_empty() {
            info!(
                target = %self.pipeline_id,
                files = finished_files.len(),
                "Writing parquet files to storage"
            );
            self.storage_writer.write_files(&finished_files).await?;

            let bytes: usize = finished_files.iter().map(|f| f.size).sum();
            emit!(BytesWritten {
                bytes: bytes as u64,
                target: self.pipeline_id.clone(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let ids: Vec<String> = (0..num_rows).map(|i| format!("id_{}", i)).collect();
        let values: Vec<i64> = (0..num_rows).map(|i| i as i64).collect();

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

        let storage_writer = StorageWriter::new(dest_uri, HashMap::new(), "test".to_string())
            .await
            .unwrap();
        let partition_extractor = PartitionExtractor::new(vec!["date".into()]);
        let writer_config = ParquetWriterConfig::default();

        let mut writer = SinkWriter::new(
            test_schema(),
            writer_config,
            storage_writer,
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

        let storage_writer = StorageWriter::new(dest_uri, HashMap::new(), "test".to_string())
            .await
            .unwrap();
        let partition_extractor = PartitionExtractor::all();
        let writer_config = ParquetWriterConfig::default();

        let mut writer = SinkWriter::new(
            test_schema(),
            writer_config,
            storage_writer,
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
