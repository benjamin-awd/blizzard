//! Storage writer for parquet file output.
//!
//! Writes Parquet files to storage (local filesystem, S3, GCS, Azure, etc.)
//! with support for partitioned directory structures.

use object_store::PutPayload;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

use blizzard_common::FinishedFile;
use blizzard_common::StorageProvider;
use blizzard_common::emit;
use blizzard_common::metrics::events::ParquetFileWritten;

use crate::error::{TableWriteError, WriteSnafu};

/// Writer for storing parquet files to a destination.
///
/// Writes Parquet files to the configured storage location with support
/// for partitioned directory structures (e.g., `date=2024-01-28/{uuid}.parquet`).
#[derive(Clone)]
pub struct StorageWriter {
    storage: Arc<StorageProvider>,
    /// Pipeline identifier for metrics labeling.
    pipeline: String,
}

impl StorageWriter {
    /// Create a new storage writer for the given destination URI.
    pub async fn new(
        destination_uri: &str,
        storage_options: HashMap<String, String>,
        pipeline: String,
    ) -> Result<Self, TableWriteError> {
        debug!(
            target = %pipeline,
            destination_uri = %destination_uri,
            storage_options = ?storage_options,
            "Creating StorageWriter"
        );
        let storage = StorageProvider::for_url_with_options(destination_uri, storage_options)
            .await
            .context(WriteSnafu)?;

        Ok(Self {
            storage: Arc::new(storage),
            pipeline,
        })
    }

    /// Write a finished file to storage.
    ///
    /// The file is written to its path (e.g., `date=2024-01-28/{uuid}.parquet`).
    pub async fn write_file(&self, file: &FinishedFile) -> Result<(), TableWriteError> {
        if let Some(bytes) = &file.bytes {
            self.storage
                .put_payload(
                    &object_store::path::Path::from(file.filename.as_str()),
                    PutPayload::from(bytes.clone()),
                )
                .await
                .context(WriteSnafu)?;
        }

        emit!(ParquetFileWritten {
            bytes: file.size,
            target: self.pipeline.clone(),
        });
        info!(
            target = %self.pipeline,
            path = %file.filename,
            size = file.size,
            records = file.record_count,
            "Wrote parquet file to storage"
        );

        Ok(())
    }

    /// Write multiple finished files to storage.
    pub async fn write_files(&self, files: &[FinishedFile]) -> Result<(), TableWriteError> {
        for file in files {
            self.write_file(file).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_table_writer_writes_to_partition() {
        let temp_dir = TempDir::new().unwrap();
        let table_uri = temp_dir.path().to_str().unwrap();

        let writer = StorageWriter::new(table_uri, HashMap::new(), "test".to_string())
            .await
            .unwrap();

        let file = FinishedFile {
            filename: "date=2026-01-28/test-uuid.parquet".to_string(),
            size: 100,
            record_count: 50,
            bytes: Some(bytes::Bytes::from(vec![1, 2, 3, 4])),
            partition_values: HashMap::from([("date".to_string(), "2026-01-28".to_string())]),
            source_file: Some("source.ndjson.gz".to_string()),
        };

        writer.write_file(&file).await.unwrap();

        // Verify parquet written directly to table partition directory
        let parquet_path = temp_dir.path().join("date=2026-01-28/test-uuid.parquet");
        assert!(
            parquet_path.exists(),
            "Parquet should be in table partition directory"
        );

        // Verify content
        let content = std::fs::read(&parquet_path).unwrap();
        assert_eq!(content, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_table_writer_writes_multiple_files() {
        let temp_dir = TempDir::new().unwrap();
        let table_uri = temp_dir.path().to_str().unwrap();

        let writer = StorageWriter::new(table_uri, HashMap::new(), "test".to_string())
            .await
            .unwrap();

        let files = vec![
            FinishedFile {
                filename: "date=2026-01-28/file1.parquet".to_string(),
                size: 10,
                record_count: 5,
                bytes: Some(bytes::Bytes::from(vec![1, 2])),
                partition_values: HashMap::from([("date".to_string(), "2026-01-28".to_string())]),
                source_file: None,
            },
            FinishedFile {
                filename: "date=2026-01-29/file2.parquet".to_string(),
                size: 20,
                record_count: 10,
                bytes: Some(bytes::Bytes::from(vec![3, 4])),
                partition_values: HashMap::from([("date".to_string(), "2026-01-29".to_string())]),
                source_file: None,
            },
        ];

        writer.write_files(&files).await.unwrap();

        assert!(
            temp_dir
                .path()
                .join("date=2026-01-28/file1.parquet")
                .exists()
        );
        assert!(
            temp_dir
                .path()
                .join("date=2026-01-29/file2.parquet")
                .exists()
        );
    }
}
