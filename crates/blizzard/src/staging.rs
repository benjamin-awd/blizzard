//! Direct table writer for blizzard/penguin communication.
//!
//! Blizzard writes Parquet files directly to the Delta table directory,
//! with coordination metadata stored in `{table_uri}/_staging/`.
//!
//! ## Directory Structure
//!
//! ```text
//! table_uri/
//! ├── _delta_log/              # Delta transaction log (managed by penguin)
//! ├── _staging/pending/        # Coordination metadata (.meta.json files)
//! ├── date=2024-01-01/         # Partitioned parquet files
//! │   └── uuid.parquet
//! └── ...
//! ```
//!
//! ## Protocol
//!
//! 1. Blizzard writes parquet file to `{table_uri}/{partition}/{uuid}.parquet`
//! 2. Blizzard writes metadata to `{table_uri}/_staging/pending/{uuid}.meta.json`
//! 3. Penguin reads `.meta.json`, commits to Delta log, deletes `.meta.json`

use object_store::PutPayload;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use blizzard_common::FinishedFile;
use blizzard_common::StorageProvider;
use blizzard_common::emit;
use blizzard_common::metrics::events::StagingFileWritten;

use crate::error::{SerializeSnafu, StagingError, StagingWriteSnafu};

/// Writer for direct table output.
///
/// Writes Parquet files directly to the table directory and coordination
/// metadata to `_staging/pending/` for penguin to pick up and commit.
pub struct StagingWriter {
    storage: Arc<StorageProvider>,
}

impl StagingWriter {
    /// Create a new staging writer for the given table URI.
    pub async fn new(
        table_uri: &str,
        storage_options: HashMap<String, String>,
    ) -> Result<Self, StagingError> {
        let storage = StorageProvider::for_url_with_options(table_uri, storage_options)
            .await
            .context(StagingWriteSnafu)?;

        Ok(Self {
            storage: Arc::new(storage),
        })
    }

    /// Write a finished file to the table directory.
    ///
    /// Writes parquet data directly to `{partition}/{uuid}.parquet` and
    /// metadata JSON to `_staging/pending/{uuid}.meta.json`.
    /// The metadata file is written last so penguin can use it as an
    /// atomic signal that the Parquet file is complete.
    pub async fn write_file(&self, file: &FinishedFile) -> Result<(), StagingError> {
        // Extract the UUID from the filename (handles partitioned paths)
        let uuid = file
            .filename
            .split('/')
            .next_back()
            .unwrap_or(&file.filename)
            .trim_end_matches(".parquet");

        // Write the Parquet file directly to table directory (at partition path)
        // file.filename is already in format "{partition}/{uuid}.parquet"
        if let Some(bytes) = &file.bytes {
            self.storage
                .put_payload(
                    &object_store::path::Path::from(file.filename.as_str()),
                    PutPayload::from(bytes.clone()),
                )
                .await
                .context(StagingWriteSnafu)?;
        }

        // Write the metadata file to _staging/pending/ (this serves as the commit signal)
        let meta_path = format!("_staging/pending/{}.meta.json", uuid);
        let metadata = serde_json::to_vec_pretty(file).context(SerializeSnafu)?;
        self.storage
            .put_payload(
                &object_store::path::Path::from(meta_path.as_str()),
                PutPayload::from(bytes::Bytes::from(metadata)),
            )
            .await
            .context(StagingWriteSnafu)?;

        emit!(StagingFileWritten { bytes: file.size });
        info!(
            "Wrote table file: {} ({} bytes, {} records)",
            file.filename, file.size, file.record_count
        );

        Ok(())
    }

    /// Write multiple finished files to the table directory.
    pub async fn write_files(&self, files: &[FinishedFile]) -> Result<(), StagingError> {
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
    async fn test_staging_writer() {
        let temp_dir = TempDir::new().unwrap();
        let table_uri = temp_dir.path().to_str().unwrap();

        // Create _staging/pending directory
        std::fs::create_dir_all(temp_dir.path().join("_staging/pending")).unwrap();

        let writer = StagingWriter::new(table_uri, HashMap::new()).await.unwrap();

        let file = FinishedFile {
            filename: "date=2026-01-28/test-uuid.parquet".to_string(),
            size: 100,
            record_count: 50,
            bytes: Some(bytes::Bytes::from(vec![1, 2, 3, 4])),
            partition_values: HashMap::from([("date".to_string(), "2026-01-28".to_string())]),
            source_file: Some("source.ndjson.gz".to_string()),
        };

        writer.write_file(&file).await.unwrap();

        // Verify parquet written to table directory (at partition path)
        let parquet_path = temp_dir.path().join("date=2026-01-28/test-uuid.parquet");
        // Verify metadata written to _staging/pending/
        let meta_path = temp_dir.path().join("_staging/pending/test-uuid.meta.json");

        assert!(parquet_path.exists());
        assert!(meta_path.exists());

        // Verify metadata content
        let meta_content = std::fs::read_to_string(&meta_path).unwrap();
        let parsed: FinishedFile = serde_json::from_str(&meta_content).unwrap();
        assert_eq!(parsed.filename, "date=2026-01-28/test-uuid.parquet");
        assert_eq!(parsed.record_count, 50);
    }
}
