//! Staging metadata reader for penguin.
//!
//! This module watches the `{table_uri}/_staging/pending/` directory for
//! coordination metadata files written by blizzard. Parquet files are written
//! directly to the table directory by blizzard, so penguin only needs to read
//! the metadata and commit to Delta Lake.

use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, warn};

use blizzard_common::metrics::events::{InternalEvent, StagingFileCommitted};
use blizzard_common::storage::StorageProvider;
use blizzard_common::{FinishedFile, StorageError};
use bytes::Bytes;
use futures::StreamExt;
use object_store::PutPayload;
use object_store::path::Path;

use crate::error::StagingError;

/// Staging metadata reader for penguin.
///
/// Reads coordination metadata from `{table_uri}/_staging/pending/` written by blizzard.
/// Parquet files are already in the table directory, so no copying is needed.
pub struct StagingReader {
    storage: Arc<StorageProvider>,
    pending_prefix: String,
    archive_prefix: String,
}

impl StagingReader {
    /// Create a new staging reader for the given table URI.
    ///
    /// Staging metadata is located at `{table_uri}/_staging/pending/`.
    pub async fn new(
        table_uri: &str,
        storage_options: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let storage =
            Arc::new(StorageProvider::for_url_with_options(table_uri, storage_options).await?);

        Ok(Self {
            storage,
            pending_prefix: "_staging/pending/".to_string(),
            archive_prefix: "_staging/archive/".to_string(),
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
                    warn!("Error listing staging file: {}", e);
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
            match self.read_metadata(&meta_path).await {
                Ok(metadata) => {
                    results.push(metadata);
                }
                Err(e) => {
                    warn!("Failed to read metadata file {}: {}", meta_path, e);
                }
            }
        }

        Ok(results)
    }

    /// Mark a file as committed by archiving the `.meta.json` coordination file.
    ///
    /// The metadata file is moved from `_staging/pending/` to `_staging/archive/`.
    /// The parquet file remains in the table directory (it's now tracked by Delta).
    pub async fn mark_committed(&self, file: &FinishedFile) -> Result<(), StagingError> {
        // Extract UUID from filename to derive meta path
        let uuid = file
            .filename
            .split('/')
            .next_back()
            .unwrap_or(&file.filename)
            .trim_end_matches(".parquet");

        let meta_path = format!("{}{}.meta.json", self.pending_prefix, uuid);
        let archive_path = format!("{}{}.meta.json", self.archive_prefix, uuid);

        // Read the metadata file
        let bytes = self
            .storage
            .get(meta_path.as_str())
            .await
            .map_err(|source| StagingError::Read { source })?;

        // Write to archive location
        self.storage
            .put_payload(
                &Path::from(archive_path.as_str()),
                PutPayload::from(Bytes::from(bytes.to_vec())),
            )
            .await
            .map_err(|source| StagingError::Archive {
                path: archive_path.clone(),
                source,
            })?;

        // Delete the original metadata file
        self.storage
            .delete(&Path::from(meta_path.as_str()))
            .await
            .map_err(|source| StagingError::Delete {
                path: meta_path.clone(),
                source,
            })?;

        debug!(
            "Archived staging metadata: {} -> {}",
            meta_path, archive_path
        );

        StagingFileCommitted.emit();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_staging_reader() {
        let temp_dir = TempDir::new().unwrap();
        let table_uri = temp_dir.path().to_str().unwrap();

        // Create _staging/pending directory and write a test metadata file
        let staging_dir = temp_dir.path().join("_staging/pending");
        let archive_dir = temp_dir.path().join("_staging/archive");
        std::fs::create_dir_all(&staging_dir).unwrap();

        let file = FinishedFile {
            filename: "date=2026-01-28/test-uuid.parquet".to_string(),
            size: 100,
            record_count: 50,
            bytes: None, // Bytes not stored in metadata
            partition_values: std::collections::HashMap::from([(
                "date".to_string(),
                "2026-01-28".to_string(),
            )]),
            source_file: Some("source.ndjson.gz".to_string()),
        };

        let meta_path = staging_dir.join("test-uuid.meta.json");
        let archived_meta_path = archive_dir.join("test-uuid.meta.json");
        std::fs::write(&meta_path, serde_json::to_vec_pretty(&file).unwrap()).unwrap();

        // Create the reader
        let reader = StagingReader::new(table_uri, HashMap::new()).await.unwrap();

        // Read pending files
        let pending = reader.read_pending_files().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].filename, "date=2026-01-28/test-uuid.parquet");
        assert_eq!(pending[0].record_count, 50);

        // Mark as committed (archives meta file)
        reader.mark_committed(&pending[0]).await.unwrap();

        // Verify meta file is removed from pending
        assert!(!meta_path.exists());

        // Verify meta file is archived
        assert!(archived_meta_path.exists());
        let archived_content: FinishedFile =
            serde_json::from_slice(&std::fs::read(&archived_meta_path).unwrap()).unwrap();
        assert_eq!(archived_content.filename, file.filename);
        assert_eq!(archived_content.record_count, file.record_count);

        // Read again should be empty (pending is cleared)
        let pending_after = reader.read_pending_files().await.unwrap();
        assert!(pending_after.is_empty());
    }

    #[tokio::test]
    async fn test_mark_committed_archives_multiple_files() {
        let temp_dir = TempDir::new().unwrap();
        let table_uri = temp_dir.path().to_str().unwrap();

        let staging_dir = temp_dir.path().join("_staging/pending");
        let archive_dir = temp_dir.path().join("_staging/archive");
        std::fs::create_dir_all(&staging_dir).unwrap();

        // Create multiple metadata files
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
            let meta_path = staging_dir.join(format!("file-{}.meta.json", i));
            std::fs::write(&meta_path, serde_json::to_vec_pretty(file).unwrap()).unwrap();
        }

        let reader = StagingReader::new(table_uri, HashMap::new()).await.unwrap();

        // Verify all pending files are found
        let pending = reader.read_pending_files().await.unwrap();
        assert_eq!(pending.len(), 3);

        // Archive each file one by one
        for file in &pending {
            reader.mark_committed(file).await.unwrap();
        }

        // Verify all files are removed from pending
        for i in 0..3 {
            let meta_path = staging_dir.join(format!("file-{}.meta.json", i));
            assert!(
                !meta_path.exists(),
                "File {} should be removed from pending",
                i
            );
        }

        // Verify all files are in archive with correct content
        for i in 0..3 {
            let archived_path = archive_dir.join(format!("file-{}.meta.json", i));
            assert!(archived_path.exists(), "File {} should be archived", i);

            let archived_content: FinishedFile =
                serde_json::from_slice(&std::fs::read(&archived_path).unwrap()).unwrap();
            assert_eq!(archived_content.record_count, 50 + i);
            assert_eq!(archived_content.size, 100 + i);
        }

        // Verify pending is empty
        let pending_after = reader.read_pending_files().await.unwrap();
        assert!(pending_after.is_empty());
    }
}
