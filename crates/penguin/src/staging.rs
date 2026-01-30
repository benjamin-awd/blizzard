//! Staging metadata reader for penguin.
//!
//! This module watches the `{table_uri}/_staging/pending/` directory for
//! coordination files written by blizzard. Both parquet and metadata files
//! are written to staging by blizzard. Penguin then:
//! 1. Reads metadata from staging
//! 2. Commits to Delta Lake
//! 3. Server-side renames parquet from staging to table directory (free on GCS/S3)
//! 4. Server-side renames metadata from pending to archive

use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

use blizzard_common::storage::StorageProvider;
use blizzard_common::{FinishedFile, StorageError};

use crate::metrics::events::{InternalEvent, StagingFileCommitted};
use futures::StreamExt;
use object_store::path::Path;

use crate::error::StagingError;

/// Extract UUID from a filename like "date=2024-01-01/uuid.parquet" -> "uuid"
fn extract_uuid(filename: &str) -> &str {
    filename
        .split('/')
        .next_back()
        .unwrap_or(filename)
        .trim_end_matches(".parquet")
}

/// Staging metadata reader for penguin.
///
/// Reads coordination files from `{table_uri}/_staging/pending/` written by blizzard.
/// Both parquet and metadata files are in staging; penguin uses server-side rename
/// to move parquet files to the table directory (zero-copy on cloud storage).
pub struct StagingReader {
    storage: Arc<StorageProvider>,
    pending_prefix: String,
    archive_prefix: String,
    /// Table identifier for metrics labeling.
    table: String,
}

impl StagingReader {
    /// Create a new staging reader for the given table URI.
    ///
    /// Staging metadata is located at `{table_uri}/_staging/pending/`.
    pub async fn new(
        table_uri: &str,
        storage_options: HashMap<String, String>,
        table: String,
    ) -> Result<Self, StorageError> {
        let storage =
            Arc::new(StorageProvider::for_url_with_options(table_uri, storage_options).await?);

        Ok(Self {
            storage,
            pending_prefix: "_staging/pending/".to_string(),
            archive_prefix: "_staging/archive/".to_string(),
            table,
        })
    }

    /// List all pending metadata files in the staging directory.
    async fn list_pending_meta_files(&self) -> Result<Vec<String>, StagingError> {
        let mut stream = self
            .storage
            .list_with_prefix(&self.pending_prefix)
            .await
            .map_err(|source| StagingError::Read { source })?;

        let mut meta_files = Vec::new();
        while let Some(result) = stream.next().await {
            match result {
                Ok(path) => {
                    let path_str = path.to_string();
                    if path_str.ends_with(".meta.json") {
                        meta_files.push(path_str);
                    }
                }
                Err(e) => {
                    warn!(table = %self.table, "Error listing staging file: {}", e);
                }
            }
        }

        Ok(meta_files)
    }

    /// Read a metadata file and return the FinishedFile.
    async fn read_metadata(&self, meta_path: &str) -> Result<FinishedFile, StagingError> {
        let bytes = self
            .storage
            .get(meta_path)
            .await
            .map_err(|source| StagingError::Read { source })?;

        let metadata: FinishedFile = serde_json::from_slice(&bytes)
            .map_err(|source| StagingError::Deserialize { source })?;

        Ok(metadata)
    }

    /// Read all pending files and their metadata.
    ///
    /// Returns the FinishedFile metadata for each pending file. The parquet files
    /// are already in the table directory (written by blizzard), so no byte copying
    /// is needed.
    pub async fn read_pending_files(&self) -> Result<Vec<FinishedFile>, StagingError> {
        let meta_files = self.list_pending_meta_files().await?;
        let mut results = Vec::with_capacity(meta_files.len());

        for meta_path in meta_files {
            let metadata = self.read_metadata(&meta_path).await?;
            results.push(metadata);
        }

        Ok(results)
    }

    /// Move parquet file from staging to table directory.
    pub async fn move_to_table(&self, file: &FinishedFile) -> Result<(), StagingError> {
        let uuid = extract_uuid(&file.filename);

        let staging_parquet = Path::from(format!("{}{}.parquet", self.pending_prefix, uuid));
        let table_parquet = Path::from(file.filename.as_str());

        self.storage
            .rename(&staging_parquet, &table_parquet)
            .await
            .map_err(|source| StagingError::Move {
                from: staging_parquet.to_string(),
                to: table_parquet.to_string(),
                source,
            })?;

        debug!(
            table = %self.table,
            "Moved parquet from staging to table: {} -> {}",
            staging_parquet, table_parquet
        );

        Ok(())
    }

    /// Archive metadata file after successful Delta commit.
    pub async fn archive_meta(&self, file: &FinishedFile) -> Result<(), StagingError> {
        let uuid = extract_uuid(&file.filename);

        let meta_path = Path::from(format!("{}{}.meta.json", self.pending_prefix, uuid));
        let archive_path = Path::from(format!("{}{}.meta.json", self.archive_prefix, uuid));

        self.storage
            .rename(&meta_path, &archive_path)
            .await
            .map_err(|source| StagingError::Archive {
                path: meta_path.to_string(),
                source,
            })?;

        debug!(
            table = %self.table,
            "Archived staging metadata: {} -> {}",
            meta_path, archive_path
        );

        StagingFileCommitted {
            table: self.table.clone(),
        }
        .emit();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_staging_reader_move_and_archive() {
        let temp_dir = TempDir::new().unwrap();
        let table_uri = temp_dir.path().to_str().unwrap();

        // Create directories
        let staging_dir = temp_dir.path().join("_staging/pending");
        let archive_dir = temp_dir.path().join("_staging/archive");
        let partition_dir = temp_dir.path().join("date=2026-01-28");
        std::fs::create_dir_all(&staging_dir).unwrap();
        std::fs::create_dir_all(&partition_dir).unwrap();

        let file = FinishedFile {
            filename: "date=2026-01-28/test-uuid.parquet".to_string(),
            size: 100,
            record_count: 50,
            bytes: None,
            partition_values: std::collections::HashMap::from([(
                "date".to_string(),
                "2026-01-28".to_string(),
            )]),
            source_file: Some("source.ndjson.gz".to_string()),
        };

        // Write both parquet and meta to staging (as blizzard would)
        let staging_parquet_path = staging_dir.join("test-uuid.parquet");
        let meta_path = staging_dir.join("test-uuid.meta.json");
        let table_parquet_path = partition_dir.join("test-uuid.parquet");
        let archived_meta_path = archive_dir.join("test-uuid.meta.json");

        std::fs::write(&staging_parquet_path, b"parquet data").unwrap();
        std::fs::write(&meta_path, serde_json::to_vec_pretty(&file).unwrap()).unwrap();

        let reader = StagingReader::new(table_uri, HashMap::new(), "test".to_string())
            .await
            .unwrap();

        // Read pending files
        let pending = reader.read_pending_files().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].filename, "date=2026-01-28/test-uuid.parquet");

        // Step 1: Move parquet to table directory (before Delta commit)
        reader.move_to_table(&pending[0]).await.unwrap();

        assert!(
            !staging_parquet_path.exists(),
            "Parquet should be removed from staging"
        );
        assert!(
            table_parquet_path.exists(),
            "Parquet should be in table directory"
        );
        assert_eq!(std::fs::read(&table_parquet_path).unwrap(), b"parquet data");
        // Meta should still be in pending (not archived yet)
        assert!(meta_path.exists(), "Meta should still be in pending");

        // Step 2: Archive meta (after Delta commit)
        reader.archive_meta(&pending[0]).await.unwrap();

        assert!(!meta_path.exists(), "Meta should be removed from pending");
        assert!(archived_meta_path.exists(), "Meta should be in archive");

        // Verify pending is empty
        let pending_after = reader.read_pending_files().await.unwrap();
        assert!(pending_after.is_empty());
    }

    #[tokio::test]
    async fn test_move_and_archive_multiple_files() {
        let temp_dir = TempDir::new().unwrap();
        let table_uri = temp_dir.path().to_str().unwrap();

        let staging_dir = temp_dir.path().join("_staging/pending");
        let archive_dir = temp_dir.path().join("_staging/archive");
        let partition_dir = temp_dir.path().join("date=2026-01-28");
        std::fs::create_dir_all(&staging_dir).unwrap();
        std::fs::create_dir_all(&partition_dir).unwrap();

        // Create multiple files
        let files: Vec<FinishedFile> = (0..3)
            .map(|i| FinishedFile {
                filename: format!("date=2026-01-28/file-{}.parquet", i),
                size: 100 + i,
                record_count: 50 + i,
                bytes: None,
                partition_values: std::collections::HashMap::from([(
                    "date".to_string(),
                    "2026-01-28".to_string(),
                )]),
                source_file: Some(format!("source-{}.ndjson.gz", i)),
            })
            .collect();

        for (i, file) in files.iter().enumerate() {
            let staging_parquet = staging_dir.join(format!("file-{}.parquet", i));
            let meta_path = staging_dir.join(format!("file-{}.meta.json", i));
            std::fs::write(&staging_parquet, format!("parquet data {}", i)).unwrap();
            std::fs::write(&meta_path, serde_json::to_vec_pretty(file).unwrap()).unwrap();
        }

        let reader = StagingReader::new(table_uri, HashMap::new(), "test".to_string())
            .await
            .unwrap();

        let pending = reader.read_pending_files().await.unwrap();
        assert_eq!(pending.len(), 3);

        // Step 1: Move all parquet files to table directory
        for file in &pending {
            reader.move_to_table(file).await.unwrap();
        }

        // Verify all parquet files moved
        for i in 0..3 {
            let staging_parquet = staging_dir.join(format!("file-{}.parquet", i));
            let table_parquet = partition_dir.join(format!("file-{}.parquet", i));
            assert!(!staging_parquet.exists());
            assert!(table_parquet.exists());
        }

        // Step 2: Archive all meta files
        for file in &pending {
            reader.archive_meta(file).await.unwrap();
        }

        // Verify all meta files archived
        for i in 0..3 {
            let meta_path = staging_dir.join(format!("file-{}.meta.json", i));
            let archived_path = archive_dir.join(format!("file-{}.meta.json", i));
            assert!(!meta_path.exists());
            assert!(archived_path.exists());
        }

        // Verify pending is empty
        let pending_after = reader.read_pending_files().await.unwrap();
        assert!(pending_after.is_empty());
    }
}
