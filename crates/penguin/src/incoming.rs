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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::StreamExt;
use snafu::ResultExt;
use tracing::{debug, info, warn};

use blizzard_common::FinishedFile;
use blizzard_common::storage::{DatePrefixGenerator, StorageProvider};

use crate::config::PartitionFilterConfig;
use crate::error::{IncomingError, incoming_error};

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
        let (watermark_partition, watermark_filename) = Self::parse_watermark(watermark);

        debug!(
            target = %self.table,
            watermark_partition = %watermark_partition,
            watermark_filename = %watermark_filename,
            "Listing files above watermark"
        );

        let mut files = Vec::new();

        // Handle root-level files (no partition)
        if watermark_partition.is_empty() {
            // List all files and filter to > watermark
            let all_files = self.list_all_parquet_files().await?;
            for file in all_files {
                if file.path.as_str() > watermark {
                    files.push(file);
                }
            }
        } else {
            // List partitions and filter to >= watermark partition
            let partitions = self.list_partitions().await?;
            let relevant_partitions: Vec<_> = partitions
                .into_iter()
                .filter(|p| p.as_str() >= watermark_partition.as_str())
                .collect();

            debug!(
                target = %self.table,
                partition_count = relevant_partitions.len(),
                "Scanning partitions >= watermark"
            );

            for partition in relevant_partitions {
                let partition_files = self.list_parquet_in_partition(&partition).await?;

                for file in partition_files {
                    // For watermark's partition, filter to files > watermark filename
                    // For partitions after watermark, include all files
                    let filename = file.path.split('/').next_back().unwrap_or(&file.path);

                    if partition == watermark_partition {
                        if filename > watermark_filename.as_str() {
                            files.push(file);
                        }
                    } else {
                        files.push(file);
                    }
                }
            }
        }

        // Sort by path for deterministic ordering
        files.sort_by(|a, b| a.path.cmp(&b.path));

        Ok(files)
    }

    /// List files during cold start (no watermark).
    ///
    /// Uses partition filter if configured, otherwise scans all files.
    async fn list_files_cold_start(&self) -> Result<Vec<IncomingFile>, IncomingError> {
        let prefixes = self.generate_cold_start_prefixes();

        let mut files = Vec::new();

        match prefixes {
            Some(prefixes) if !prefixes.is_empty() => {
                info!(
                    target = %self.table,
                    prefix_count = prefixes.len(),
                    "Cold start: scanning partitions with filter"
                );
                for prefix in prefixes {
                    let partition_files = self.list_parquet_in_partition(&prefix).await?;
                    files.extend(partition_files);
                }
            }
            _ => {
                info!(
                    target = %self.table,
                    "Cold start: scanning all files (no filter configured)"
                );
                // List all parquet files, including root-level (non-partitioned) files
                files = self.list_all_parquet_files().await?;
            }
        }

        // Sort by path for deterministic ordering
        files.sort_by(|a, b| a.path.cmp(&b.path));

        Ok(files)
    }

    /// List all parquet files in the table, including root-level files.
    async fn list_all_parquet_files(&self) -> Result<Vec<IncomingFile>, IncomingError> {
        let mut files = Vec::new();

        let mut stream = self
            .storage
            .list(true)
            .await
            .context(incoming_error::ListSnafu)?;

        while let Some(result) = stream.next().await {
            match result {
                Ok(path) => {
                    let path_str = path.to_string();
                    // Skip internal directories (those starting with _)
                    if path_str.starts_with('_') {
                        continue;
                    }
                    // Include all parquet files
                    if path_str.ends_with(".parquet") {
                        files.push(IncomingFile {
                            path: path_str,
                            size: 0,
                        });
                    }
                }
                Err(e) => {
                    warn!(target = %self.table, "Error listing file: {}", e);
                }
            }
        }

        debug!(
            target = %self.table,
            count = files.len(),
            "Found parquet files"
        );

        Ok(files)
    }

    /// Generate prefixes for cold start based on partition filter config.
    fn generate_cold_start_prefixes(&self) -> Option<Vec<String>> {
        self.config.partition_filter.as_ref().map(|filter| {
            let generator = DatePrefixGenerator::new(&filter.prefix_template, filter.lookback);
            generator.generate_prefixes()
        })
    }

    /// Parse watermark into partition prefix and filename.
    ///
    /// Example: "date=2024-01-28/01926abc-def0-7123-4567-89abcdef0123.parquet"
    ///       -> ("date=2024-01-28", "01926abc-def0-7123-4567-89abcdef0123.parquet")
    ///
    /// For root-level files (no partition):
    /// "01926abc-def0-7123-4567-89abcdef0123.parquet" -> ("", "01926abc-...")
    fn parse_watermark(watermark: &str) -> (String, String) {
        // Find the last '/' to split partition from filename
        if let Some(last_slash) = watermark.rfind('/') {
            let partition = watermark[..last_slash].to_string();
            let filename = watermark[last_slash + 1..].to_string();
            (partition, filename)
        } else {
            // No partition prefix, just a filename (root-level file)
            (String::new(), watermark.to_string())
        }
    }

    /// List all partition directories in the table.
    ///
    /// Excludes internal directories (those starting with `_`).
    async fn list_partitions(&self) -> Result<Vec<String>, IncomingError> {
        let mut partitions = HashSet::new();

        let mut stream = self
            .storage
            .list(true)
            .await
            .context(incoming_error::ListSnafu)?;

        while let Some(result) = stream.next().await {
            match result {
                Ok(path) => {
                    let path_str = path.to_string();
                    // Skip internal directories (those starting with _)
                    if path_str.starts_with('_') {
                        continue;
                    }
                    // Extract partition prefix (everything except the filename)
                    if let Some(last_slash) = path_str.rfind('/') {
                        let partition = &path_str[..last_slash];
                        if !partition.is_empty() {
                            partitions.insert(partition.to_string());
                        }
                    }
                }
                Err(e) => {
                    warn!(target = %self.table, "Error listing file: {}", e);
                }
            }
        }

        let mut partitions: Vec<_> = partitions.into_iter().collect();
        partitions.sort();

        debug!(
            target = %self.table,
            count = partitions.len(),
            "Found partitions"
        );

        Ok(partitions)
    }

    /// List parquet files in a specific partition.
    async fn list_parquet_in_partition(
        &self,
        partition: &str,
    ) -> Result<Vec<IncomingFile>, IncomingError> {
        let mut files = Vec::new();

        // Add trailing slash for prefix listing
        let prefix = if partition.ends_with('/') {
            partition.to_string()
        } else {
            format!("{}/", partition)
        };

        let mut stream = self
            .storage
            .list_with_prefix(&prefix)
            .await
            .context(incoming_error::ListSnafu)?;

        while let Some(result) = stream.next().await {
            match result {
                Ok(path) => {
                    let path_str = path.to_string();
                    if path_str.ends_with(".parquet") {
                        // Get file size from a HEAD request would be ideal,
                        // but for now we'll use 0 and get actual size from parquet metadata
                        files.push(IncomingFile {
                            path: path_str,
                            size: 0, // Will be populated from parquet metadata
                        });
                    }
                }
                Err(e) => {
                    warn!(target = %self.table, "Error listing file in partition {}: {}", partition, e);
                }
            }
        }

        Ok(files)
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
        let partition_values = Self::parse_partition_values(&incoming.path);

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

    /// Parse partition values from a file path.
    ///
    /// Example: "date=2024-01-28/hour=14/file.parquet"
    ///       -> {"date": "2024-01-28", "hour": "14"}
    fn parse_partition_values(path: &str) -> HashMap<String, String> {
        let mut values = HashMap::new();

        for segment in path.split('/') {
            if let Some(eq_pos) = segment.find('=') {
                let key = &segment[..eq_pos];
                let value = &segment[eq_pos + 1..];
                // Skip if this looks like a filename (ends with .parquet)
                if !value.ends_with(".parquet") {
                    values.insert(key.to_string(), value.to_string());
                }
            }
        }

        values
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_watermark() {
        let (partition, filename) = IncomingReader::parse_watermark(
            "date=2024-01-28/01926abc-def0-7123-4567-89abcdef0123.parquet",
        );
        assert_eq!(partition, "date=2024-01-28");
        assert_eq!(filename, "01926abc-def0-7123-4567-89abcdef0123.parquet");
    }

    #[test]
    fn test_parse_watermark_nested_partitions() {
        let (partition, filename) = IncomingReader::parse_watermark(
            "date=2024-01-28/hour=14/01926abc-def0-7123-4567-89abcdef0123.parquet",
        );
        assert_eq!(partition, "date=2024-01-28/hour=14");
        assert_eq!(filename, "01926abc-def0-7123-4567-89abcdef0123.parquet");
    }

    #[test]
    fn test_parse_watermark_no_partition() {
        let (partition, filename) = IncomingReader::parse_watermark("file.parquet");
        assert_eq!(partition, "");
        assert_eq!(filename, "file.parquet");
    }

    #[test]
    fn test_parse_partition_values() {
        let values = IncomingReader::parse_partition_values(
            "date=2024-01-28/hour=14/01926abc-def0-7123.parquet",
        );
        assert_eq!(values.get("date"), Some(&"2024-01-28".to_string()));
        assert_eq!(values.get("hour"), Some(&"14".to_string()));
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_parse_partition_values_no_partitions() {
        let values = IncomingReader::parse_partition_values("file.parquet");
        assert!(values.is_empty());
    }

    #[test]
    fn test_parse_partition_values_single_partition() {
        let values = IncomingReader::parse_partition_values("date=2024-01-28/file.parquet");
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
            storage,
            "test".to_string(),
            IncomingConfig {
                partition_filter: None, // No filter for this test
            },
        );

        // List partitions
        let partitions = reader.list_partitions().await.unwrap();

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
