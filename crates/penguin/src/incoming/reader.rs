//! Parquet file reader implementation.
//!
//! This module provides the `IncomingReader` implementation that discovers
//! parquet files placed directly by external writers in partition directories.
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

use async_trait::async_trait;
use snafu::ResultExt;
use tracing::{debug, info};

use blizzard_core::FinishedFile;
use blizzard_core::PartitionExtractor;
use blizzard_core::storage::{StorageProvider, expand_include_prefixes};
use blizzard_core::watermark::{
    self, FileListingConfig, generate_prefixes, list_files_above_watermark_with_prefixes,
};

use crate::config::PartitionFilterConfig;
use crate::error::{IncomingError, incoming_error};

use super::FileReader;

/// File extension for parquet files.
const PARQUET_EXTENSION: &str = ".parquet";

/// Configuration for the incoming reader.
#[derive(Debug, Clone)]
pub struct IncomingConfig {
    /// Partition filter for cold start (no watermark yet).
    pub partition_filter: Option<PartitionFilterConfig>,
    /// Extractor for partition values from file paths.
    pub partition_extractor: PartitionExtractor,
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
    /// - Applies client-side include filters for partition values that couldn't
    ///   be folded into the S3 prefix
    pub async fn list_uncommitted_files(
        &self,
        watermark: Option<&str>,
        committed_paths: &HashSet<String>,
        cold_start: bool,
    ) -> Result<Vec<IncomingFile>, IncomingError> {
        let files = match watermark {
            Some(wm) => self.list_files_above_watermark(wm).await?,
            None => self.list_files_cold_start(cold_start).await?,
        };

        // Filter out already committed files
        let mut uncommitted: Vec<IncomingFile> = files
            .into_iter()
            .filter(|f| !committed_paths.contains(&f.path))
            .collect();

        // Apply client-side include filters for entries that couldn't be
        // folded into the S3 prefix (due to gaps or missing placeholders).
        let remaining_filters = self.remaining_include_filters();
        if !remaining_filters.is_empty() {
            let before = uncommitted.len();
            uncommitted.retain(|f| {
                let values = self.config.partition_extractor.extract(&f.path);
                remaining_filters.iter().all(|(key, allowed)| {
                    match values.get(key) {
                        Some(val) => allowed.iter().any(|a| a == val),
                        // If the key isn't in the path, don't filter it out
                        None => true,
                    }
                })
            });
            let filtered = before - uncommitted.len();
            if filtered > 0 {
                debug!(
                    target = %self.table,
                    filtered,
                    remaining = uncommitted.len(),
                    "Applied client-side include filters"
                );
            }
        }

        if !uncommitted.is_empty() {
            debug!(
                target = %self.table,
                count = uncommitted.len(),
                "Found uncommitted incoming files"
            );
        }

        Ok(uncommitted)
    }

    /// List files above the given watermark.
    ///
    /// If a partition filter is configured, uses the filter prefixes to avoid
    /// expensive full-recursive partition discovery.
    async fn list_files_above_watermark(
        &self,
        watermark: &str,
    ) -> Result<Vec<IncomingFile>, IncomingError> {
        let config = FileListingConfig {
            extension: PARQUET_EXTENSION,
            target: &self.table,
        };

        // Use partition filter prefixes if available to avoid full partition scan
        let prefixes = self.generate_cold_start_prefixes();

        let paths = list_files_above_watermark_with_prefixes(
            &self.storage,
            watermark,
            prefixes.as_deref(),
            &config,
        )
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
    /// On the very first poll (`cold_start=true`), logs at `info!` level;
    /// subsequent polls log at `debug!` to avoid persistent log noise.
    async fn list_files_cold_start(
        &self,
        cold_start: bool,
    ) -> Result<Vec<IncomingFile>, IncomingError> {
        let prefixes = self.generate_cold_start_prefixes();

        let config = FileListingConfig {
            extension: PARQUET_EXTENSION,
            target: &self.table,
        };

        match &prefixes {
            Some(p) if !p.is_empty() => {
                if cold_start {
                    info!(
                        target = %self.table,
                        prefixes = ?p,
                        "Cold start: scanning partitions with filter"
                    );
                } else {
                    debug!(
                        target = %self.table,
                        prefixes = ?p,
                        "Cold start: scanning partitions with filter"
                    );
                }
            }
            _ => {
                if cold_start {
                    info!(
                        target = %self.table,
                        "Cold start: scanning all files (no filter configured)"
                    );
                } else {
                    debug!(
                        target = %self.table,
                        "Cold start: scanning all files (no filter configured)"
                    );
                }
            }
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
    ///
    /// When include filters are configured, expands `{key}` placeholders in the
    /// prefix template with include values (cartesian product), stopping at the
    /// first key without a match. Remaining filters are applied client-side.
    fn generate_cold_start_prefixes(&self) -> Option<Vec<String>> {
        self.config.partition_filter.as_ref().map(|filter| {
            let date_prefixes = generate_prefixes(&filter.prefix_template, filter.lookback);
            if filter.include.is_empty() {
                return date_prefixes;
            }
            let (expanded, _remaining) = expand_include_prefixes(&date_prefixes, &filter.include);
            expanded
        })
    }

    /// Compute the remaining include filters that must be applied client-side.
    ///
    /// These are include entries that couldn't be folded into the S3 prefix
    /// (due to gaps in `{key}` placeholders or keys not in the template).
    fn remaining_include_filters(&self) -> HashMap<String, Vec<String>> {
        let Some(filter) = &self.config.partition_filter else {
            return HashMap::new();
        };
        if filter.include.is_empty() {
            return HashMap::new();
        }
        // Use a dummy prefix to compute remaining filters — the structure is
        // the same regardless of the date values.
        let dummy = vec![filter.prefix_template.clone()];
        let (_expanded, remaining) = expand_include_prefixes(&dummy, &filter.include);
        remaining
    }

    /// Read metadata from a parquet file and create a FinishedFile.
    ///
    /// Fetches only the file footer (~64 KB) via a single suffix-range
    /// request instead of downloading the entire file (10-100 MB).
    /// Falls back to full download if footer parsing fails.
    pub async fn read_parquet_metadata(
        &self,
        incoming: &IncomingFile,
    ) -> Result<FinishedFile, IncomingError> {
        let (file_size, metadata) =
            crate::parquet::read_parquet_footer(&self.storage, incoming.path.as_str(), &self.table)
                .await
                .map_err(|e| match e {
                    crate::parquet::FooterReadError::Storage(source) => IncomingError::Read {
                        path: incoming.path.clone(),
                        source,
                    },
                    crate::parquet::FooterReadError::Parquet(source) => {
                        IncomingError::ParquetMetadata {
                            path: incoming.path.clone(),
                            source,
                        }
                    }
                })?;

        let record_count = usize::try_from(metadata.file_metadata().num_rows()).unwrap_or(0);

        // Parse partition values from path
        let partition_values = self.config.partition_extractor.extract(&incoming.path);

        debug!(
            target = %self.table,
            path = %incoming.path,
            size = file_size,
            records = record_count,
            "Read parquet metadata (footer-only)"
        );

        Ok(FinishedFile::without_bytes(
            incoming.path.clone(),
            // u64 → usize: lossless on 64-bit, saturates on 32-bit
            file_size.try_into().unwrap_or(usize::MAX),
            record_count,
            partition_values,
            None, // No source file for external writes
        ))
    }
}

#[async_trait]
impl FileReader for IncomingReader {
    async fn list_uncommitted_files(
        &self,
        watermark: Option<&str>,
        committed_paths: &HashSet<String>,
        cold_start: bool,
    ) -> Result<Vec<IncomingFile>, IncomingError> {
        IncomingReader::list_uncommitted_files(self, watermark, committed_paths, cold_start).await
    }

    async fn read_file_metadata(
        &self,
        incoming: &IncomingFile,
    ) -> Result<FinishedFile, IncomingError> {
        self.read_parquet_metadata(incoming).await
    }

    fn table_name(&self) -> &str {
        &self.table
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_watermark() {
        let (partition, filename) = blizzard_core::watermark::parse_watermark(
            "date=2024-01-28/01926abc-def0-7123-4567-89abcdef0123.parquet",
        );
        assert_eq!(partition, "date=2024-01-28");
        assert_eq!(filename, "01926abc-def0-7123-4567-89abcdef0123.parquet");
    }

    #[test]
    fn test_parse_watermark_nested_partitions() {
        let (partition, filename) = blizzard_core::watermark::parse_watermark(
            "date=2024-01-28/hour=14/01926abc-def0-7123-4567-89abcdef0123.parquet",
        );
        assert_eq!(partition, "date=2024-01-28/hour=14");
        assert_eq!(filename, "01926abc-def0-7123-4567-89abcdef0123.parquet");
    }

    #[test]
    fn test_parse_watermark_no_partition() {
        let (partition, filename) = blizzard_core::watermark::parse_watermark("file.parquet");
        assert_eq!(partition, "");
        assert_eq!(filename, "file.parquet");
    }

    #[test]
    fn test_partition_extractor_all() {
        let extractor = PartitionExtractor::all();
        let values = extractor.extract("date=2024-01-28/hour=14/01926abc-def0-7123.parquet");
        assert_eq!(values.get("date"), Some(&"2024-01-28".to_string()));
        assert_eq!(values.get("hour"), Some(&"14".to_string()));
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_partition_extractor_all_no_partitions() {
        let extractor = PartitionExtractor::all();
        let values = extractor.extract("file.parquet");
        assert!(values.is_empty());
    }

    #[test]
    fn test_partition_extractor_all_single_partition() {
        let extractor = PartitionExtractor::all();
        let values = extractor.extract("date=2024-01-28/file.parquet");
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
                partition_filter: None,
                partition_extractor: PartitionExtractor::all(),
            },
        );

        // List partitions using the shared function
        let partitions = blizzard_core::watermark::list_partitions(&storage)
            .await
            .unwrap();

        // Should find our data partitions but not internal directories (those starting with _)
        assert!(partitions.contains(&"date=2026-01-27".to_string()));
        assert!(partitions.contains(&"date=2026-01-28".to_string()));
        assert!(partitions.contains(&"date=2026-01-20".to_string()));
        assert!(!partitions.iter().any(|p| p.starts_with('_')));

        // List all files (cold start without filter)
        let files = reader.list_files_cold_start(true).await.unwrap();
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
                partition_extractor: PartitionExtractor::all(),
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

    #[tokio::test]
    async fn test_client_side_include_filter() {
        use blizzard_core::config::StringOrVec;
        use std::collections::HashMap;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path();

        // Use today's date so generate_prefixes(lookback=0) hits the right dir.
        let today = chrono::Utc::now();
        let year = today.format("%Y").to_string();
        let month = today.format("%m").to_string();
        let day = today.format("%d").to_string();

        // Create directories: {year}/{month}/{day}/{host}/{region}/{category}/
        for host in &["web-prod-01", "web-prod-02", "db-prod-01"] {
            for region in &["us-east-1", "eu-west-1"] {
                for category in &["events", "metrics", "logs"] {
                    let dir = table_path
                        .join(&year)
                        .join(&month)
                        .join(&day)
                        .join(host)
                        .join(region)
                        .join(category);
                    std::fs::create_dir_all(&dir).unwrap();
                    std::fs::write(dir.join("data.parquet"), b"").unwrap();
                }
            }
        }

        let storage = Arc::new(
            StorageProvider::for_url_with_options(table_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap(),
        );

        // Configure: prefix_template has {host} and {region} but include only
        // provides host. This creates a gap at {region}, so category (after
        // the gap) becomes a client-side filter.
        let mut include = HashMap::new();
        include.insert(
            "host".to_string(),
            StringOrVec::Multiple(vec!["web-prod-01".to_string(), "web-prod-02".to_string()]),
        );
        include.insert(
            "category".to_string(),
            StringOrVec::Multiple(vec!["events".to_string(), "logs".to_string()]),
        );

        let reader = IncomingReader::new(
            storage,
            "test".to_string(),
            IncomingConfig {
                partition_filter: Some(PartitionFilterConfig {
                    prefix_template: "%Y/%m/%d/{host}/{region}/{category}".to_string(),
                    lookback: 0,
                    include,
                }),
                partition_extractor: PartitionExtractor::from_template(
                    "year=%Y/month=%m/day=%d/host={host}/region={region}/category={category}",
                    None,
                ),
            },
        );

        // Verify remaining filters: {host} is folded, {region} has no include
        // (gap), so {category} becomes a remaining client-side filter.
        let remaining = reader.remaining_include_filters();
        assert!(
            remaining.contains_key("category"),
            "category should be remaining: {remaining:?}"
        );
        assert!(
            !remaining.contains_key("host"),
            "host should not be remaining: {remaining:?}"
        );

        // Cold start listing: prefixes will be expanded for web-prod-01 and web-prod-02.
        // All files under those hosts (both regions, all categories) will be listed.
        // Client-side filter should then narrow to only "events" and "logs".
        let committed = HashSet::new();
        let files = reader
            .list_uncommitted_files(None, &committed, true)
            .await
            .unwrap();

        // 2 hosts × 2 regions × 2 categories = 8 files (out of 3×2×3 = 18 total)
        assert_eq!(
            files.len(),
            8,
            "Expected 8 files after include filter, got: {}",
            files.len()
        );

        // Verify no db-prod-01 files (host not in include)
        assert!(
            !files.iter().any(|f| f.path.contains("db-prod-01")),
            "Should not contain db-prod-01 files"
        );
        // Verify no metrics files (category not in include)
        assert!(
            !files.iter().any(|f| f.path.contains("metrics")),
            "Should not contain metrics files"
        );
    }
}
