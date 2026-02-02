//! Incoming file reader for discovering parquet files.
//!
//! This module handles scanning table directories for parquet files placed
//! directly by external writers in partition directories.
//!
//! # How It Works
//!
//! External writers place parquet files directly in partition directories:
//! ```text
//! {table_uri}/date=2024-01-28/{uuidv7}.parquet
//! ```
//!
//! Penguin tracks a high-watermark (lexicographically-sortable full path) to
//! identify which files have been committed to Delta Lake. On each poll:
//!
//! 1. Parse watermark to extract partition prefix and UUID
//! 2. List only partitions >= watermark's partition (efficient listing)
//! 3. Filter to files with UUID > watermark's UUID within watermark partition
//! 4. Cross-check against Delta log (already committed?)
//! 5. Commit new files to Delta
//! 6. Update watermark to highest committed path

use std::collections::HashSet;
use std::sync::Arc;

use snafu::ResultExt;
use tracing::{debug, info};

use blizzard_common::FinishedFile;
use blizzard_common::storage::StorageProvider;
use blizzard_common::watermark::{
    self, FileListingConfig, generate_prefixes, parse_partition_values,
};

use crate::config::PartitionFilterConfig;
use crate::error::{IncomingError, incoming_error};

/// File extension for parquet files.
const PARQUET_EXTENSION: &str = ".parquet";

/// Configuration for the incoming reader.
#[derive(Debug, Clone)]
pub struct IncomingConfig {
    /// Partition filter for cold start (no watermark yet).
    pub partition_filter: Option<PartitionFilterConfig>,
}

/// Information about an incoming file discovered in the table directory.
#[derive(Debug, Clone)]
pub struct IncomingFile {
    /// Path relative to table root (e.g., "date=2024-01-28/uuid.parquet").
    pub path: String,
    /// File size in bytes.
    pub size: usize,
}

/// Reader for incoming parquet files placed directly in table directories.
pub struct IncomingReader {
    storage: Arc<StorageProvider>,
    table: String,
    config: IncomingConfig,
}

impl IncomingReader {
    /// Create a new incoming reader for the given table.
    pub fn new(storage: Arc<StorageProvider>, table: String, config: IncomingConfig) -> Self {
        Self {
            storage,
            table,
            config,
        }
    }

    /// List uncommitted parquet files above the watermark.
    ///
    /// Uses watermark's partition + UUID to efficiently narrow listing:
    /// - Only scans partitions >= watermark's partition
    /// - Filters to UUIDs > watermark's UUID within the watermark partition
    /// - Cross-checks against `committed_paths` to avoid double-commits
    pub async fn list_uncommitted_files(
        &self,
        watermark: Option<&str>,
        committed_paths: &HashSet<String>,
    ) -> Result<Vec<IncomingFile>, IncomingError> {
        let files = match watermark {
            Some(wm) => self.list_files_above_watermark(wm).await?,
            None => self.list_files_cold_start().await?,
        };

        // Filter out already committed files
        let uncommitted: Vec<IncomingFile> = files
            .into_iter()
            .filter(|f| !committed_paths.contains(&f.path))
            .collect();

        if !uncommitted.is_empty() {
            info!(
                target = %self.table,
                count = uncommitted.len(),
                "Found uncommitted incoming files"
            );
        }

        Ok(uncommitted)
    }

    /// List files above the given watermark.
    async fn list_files_above_watermark(
        &self,
        watermark: &str,
    ) -> Result<Vec<IncomingFile>, IncomingError> {
        let config = FileListingConfig {
            extension: PARQUET_EXTENSION,
            target: &self.table,
        };

        let paths = watermark::list_files_above_watermark(&self.storage, watermark, &config)
            .await
            .context(incoming_error::ListSnafu)?;

        Ok(paths
            .into_iter()
            .map(|path| IncomingFile { path, size: 0 })
            .collect())
    }

    /// List files during cold start (no watermark).
    ///
    /// Uses partition filter if configured, otherwise scans all files.
    async fn list_files_cold_start(&self) -> Result<Vec<IncomingFile>, IncomingError> {
        let prefixes = self.generate_cold_start_prefixes();

        let config = FileListingConfig {
            extension: PARQUET_EXTENSION,
            target: &self.table,
        };

        if prefixes.as_ref().is_some_and(|p| !p.is_empty()) {
            info!(
                target = %self.table,
                prefix_count = prefixes.as_ref().unwrap().len(),
                "Cold start: scanning partitions with filter"
            );
        } else {
            info!(
                target = %self.table,
                "Cold start: scanning all files (no filter configured)"
            );
        }

        let paths = watermark::list_files_cold_start(&self.storage, prefixes.as_deref(), &config)
            .await
            .context(incoming_error::ListSnafu)?;

        Ok(paths
            .into_iter()
            .map(|path| IncomingFile { path, size: 0 })
            .collect())
    }

    /// Generate prefixes for cold start based on partition filter config.
    fn generate_cold_start_prefixes(&self) -> Option<Vec<String>> {
        self.config
            .partition_filter
            .as_ref()
            .map(|filter| generate_prefixes(&filter.prefix_template, filter.lookback))
    }

    /// Read metadata from a parquet file and create a FinishedFile.
    ///
    /// Extracts record count and file size from parquet metadata.
    /// Partition values are parsed from the file path.
    pub async fn read_parquet_metadata(
        &self,
        incoming: &IncomingFile,
    ) -> Result<FinishedFile, IncomingError> {
        use deltalake::parquet::file::reader::{FileReader, SerializedFileReader};

        // Read the parquet file
        let bytes = self
            .storage
            .get(incoming.path.as_str())
            .await
            .map_err(|source| IncomingError::Read {
                path: incoming.path.clone(),
                source,
            })?;

        let file_size = bytes.len();

        // Create a serialized file reader to parse parquet metadata
        let reader =
            SerializedFileReader::new(bytes).map_err(|source| IncomingError::ParquetMetadata {
                path: incoming.path.clone(),
                source,
            })?;

        // Get metadata and extract row counts
        let metadata = reader.metadata();
        let record_count: usize = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .sum();

        // Parse partition values from path
        let partition_values = parse_partition_values(&incoming.path);

        debug!(
            target = %self.table,
            path = %incoming.path,
            size = file_size,
            records = record_count,
            "Read parquet metadata"
        );

        Ok(FinishedFile::without_bytes(
            incoming.path.clone(),
            file_size,
            record_count,
            partition_values,
            None, // No source file for external writes
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_watermark() {
        let (partition, filename) = blizzard_common::watermark::parse_watermark(
            "date=2024-01-28/01926abc-def0-7123-4567-89abcdef0123.parquet",
        );
        assert_eq!(partition, "date=2024-01-28");
        assert_eq!(filename, "01926abc-def0-7123-4567-89abcdef0123.parquet");
    }

    #[test]
    fn test_parse_watermark_nested_partitions() {
        let (partition, filename) = blizzard_common::watermark::parse_watermark(
            "date=2024-01-28/hour=14/01926abc-def0-7123-4567-89abcdef0123.parquet",
        );
        assert_eq!(partition, "date=2024-01-28/hour=14");
        assert_eq!(filename, "01926abc-def0-7123-4567-89abcdef0123.parquet");
    }

    #[test]
    fn test_parse_watermark_no_partition() {
        let (partition, filename) = blizzard_common::watermark::parse_watermark("file.parquet");
        assert_eq!(partition, "");
        assert_eq!(filename, "file.parquet");
    }

    #[test]
    fn test_parse_partition_values() {
        let values = parse_partition_values("date=2024-01-28/hour=14/01926abc-def0-7123.parquet");
        assert_eq!(values.get("date"), Some(&"2024-01-28".to_string()));
        assert_eq!(values.get("hour"), Some(&"14".to_string()));
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_parse_partition_values_no_partitions() {
        let values = parse_partition_values("file.parquet");
        assert!(values.is_empty());
    }

    #[test]
    fn test_parse_partition_values_single_partition() {
        let values = parse_partition_values("date=2024-01-28/file.parquet");
        assert_eq!(values.get("date"), Some(&"2024-01-28".to_string()));
        assert_eq!(values.len(), 1);
    }

    #[tokio::test]
    async fn test_incoming_reader_cold_start_with_filter() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path();

        // Create partition directories with parquet files
        let partition1 = table_path.join("date=2026-01-28");
        let partition2 = table_path.join("date=2026-01-27");
        let old_partition = table_path.join("date=2026-01-20");
        std::fs::create_dir_all(&partition1).unwrap();
        std::fs::create_dir_all(&partition2).unwrap();
        std::fs::create_dir_all(&old_partition).unwrap();

        // Create test parquet files (just empty files for listing test)
        std::fs::write(partition1.join("file1.parquet"), b"").unwrap();
        std::fs::write(partition2.join("file2.parquet"), b"").unwrap();
        std::fs::write(old_partition.join("old-file.parquet"), b"").unwrap();

        // Create internal directories that should be excluded (those starting with _)
        let internal = table_path.join("_internal");
        let delta_log = table_path.join("_delta_log");
        std::fs::create_dir_all(&internal).unwrap();
        std::fs::create_dir_all(&delta_log).unwrap();
        std::fs::write(internal.join("test.meta.json"), b"{}").unwrap();
        std::fs::write(delta_log.join("00000.json"), b"{}").unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(table_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap(),
        );

        let reader = IncomingReader::new(
            storage.clone(),
            "test".to_string(),
            IncomingConfig {
                partition_filter: None, // No filter for this test
            },
        );

        // List partitions using the shared function
        let partitions = blizzard_common::watermark::list_partitions(&storage)
            .await
            .unwrap();

        // Should find our data partitions but not internal directories (those starting with _)
        assert!(partitions.contains(&"date=2026-01-27".to_string()));
        assert!(partitions.contains(&"date=2026-01-28".to_string()));
        assert!(partitions.contains(&"date=2026-01-20".to_string()));
        assert!(!partitions.iter().any(|p| p.starts_with('_')));

        // List all files (cold start without filter)
        let files = reader.list_files_cold_start().await.unwrap();
        assert_eq!(files.len(), 3);

        let paths: Vec<_> = files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"date=2026-01-20/old-file.parquet"));
        assert!(paths.contains(&"date=2026-01-27/file2.parquet"));
        assert!(paths.contains(&"date=2026-01-28/file1.parquet"));
    }

    #[tokio::test]
    async fn test_incoming_reader_with_watermark() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path();

        // Create partition directories with parquet files using UUIDv7-like names
        let partition1 = table_path.join("date=2026-01-28");
        std::fs::create_dir_all(&partition1).unwrap();

        // Create files with lexicographically ordered names
        std::fs::write(partition1.join("01926abc-1111.parquet"), b"").unwrap();
        std::fs::write(partition1.join("01926abc-2222.parquet"), b"").unwrap();
        std::fs::write(partition1.join("01926abc-3333.parquet"), b"").unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(table_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap(),
        );

        let reader = IncomingReader::new(
            storage,
            "test".to_string(),
            IncomingConfig {
                partition_filter: None,
            },
        );

        // List files above watermark "date=2026-01-28/01926abc-2222.parquet"
        let files = reader
            .list_files_above_watermark("date=2026-01-28/01926abc-2222.parquet")
            .await
            .unwrap();

        // Should only find the file after the watermark
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "date=2026-01-28/01926abc-3333.parquet");
    }
}
