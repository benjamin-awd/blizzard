//! Shared watermark-based file listing logic.
//!
//! This module provides the core watermark listing functionality used by both
//! blizzard (NDJSON files) and penguin (Parquet files).

use std::collections::HashSet;

use futures::StreamExt;
use tracing::{debug, warn};

use crate::error::StorageError;
use crate::storage::StorageProvider;

/// Configuration for file listing operations.
#[derive(Debug, Clone)]
pub struct FileListingConfig<'a> {
    /// File extension to filter for (e.g., ".ndjson.gz" or ".parquet").
    pub extension: &'a str,
    /// Target identifier for logging (e.g., pipeline name or table name).
    pub target: &'a str,
}

/// Parse a watermark into (partition_prefix, filename).
///
/// # Examples
///
/// ```
/// use blizzard_core::watermark::parse_watermark;
///
/// let (partition, filename) = parse_watermark("date=2026-01-28/file.ndjson.gz");
/// assert_eq!(partition, "date=2026-01-28");
/// assert_eq!(filename, "file.ndjson.gz");
///
/// let (partition, filename) = parse_watermark("date=2026-01-28/hour=14/file.ndjson.gz");
/// assert_eq!(partition, "date=2026-01-28/hour=14");
/// assert_eq!(filename, "file.ndjson.gz");
///
/// let (partition, filename) = parse_watermark("file.ndjson.gz");
/// assert_eq!(partition, "");
/// assert_eq!(filename, "file.ndjson.gz");
/// ```
pub fn parse_watermark(watermark: &str) -> (String, String) {
    if let Some(last_slash) = watermark.rfind('/') {
        let partition = watermark[..last_slash].to_string();
        let filename = watermark[last_slash + 1..].to_string();
        (partition, filename)
    } else {
        // No partition prefix, just a filename (root-level file)
        (String::new(), watermark.to_string())
    }
}

/// List files above the given watermark.
///
/// Uses the watermark's partition and filename to efficiently narrow listing:
/// - Only scans partitions >= watermark's partition
/// - Filters to files > watermark (lexicographic comparison)
///
/// # Arguments
///
/// * `storage` - Storage provider for the source directory
/// * `watermark` - High-watermark (last processed file path)
/// * `config` - File listing configuration (extension and target for logging)
///
/// # Returns
///
/// List of file paths (relative to storage root) above the watermark,
/// sorted lexicographically for deterministic ordering.
pub async fn list_files_above_watermark(
    storage: &StorageProvider,
    watermark: &str,
    config: &FileListingConfig<'_>,
) -> Result<Vec<String>, StorageError> {
    let (watermark_partition, watermark_filename) = parse_watermark(watermark);

    debug!(
        target = %config.target,
        watermark_partition = %watermark_partition,
        watermark_filename = %watermark_filename,
        extension = %config.extension,
        "Listing files above watermark"
    );

    let mut files = Vec::new();

    if watermark_partition.is_empty() {
        // Root-level files (no partitions)
        let all_files = list_all_files(storage, config).await?;
        for file in all_files {
            if file.as_str() > watermark {
                files.push(file);
            }
        }
    } else {
        // List partitions and filter to >= watermark partition
        let partitions = list_partitions(storage).await?;
        let relevant_partitions: Vec<_> = partitions
            .into_iter()
            .filter(|p| p.as_str() >= watermark_partition.as_str())
            .collect();

        debug!(
            target = %config.target,
            partition_count = relevant_partitions.len(),
            "Scanning partitions >= watermark"
        );

        for partition in relevant_partitions {
            let partition_files = list_files_in_partition(storage, &partition, config).await?;

            for file in partition_files {
                // For watermark's partition, filter to files > watermark filename
                // For partitions after watermark, include all files
                let filename = file.split('/').next_back().unwrap_or(&file);

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
    files.sort();

    debug!(
        target = %config.target,
        count = files.len(),
        "Found files above watermark"
    );

    Ok(files)
}

/// List files during cold start (no watermark).
///
/// Uses partition filter prefixes if provided, otherwise scans all files.
///
/// # Arguments
///
/// * `storage` - Storage provider for the source directory
/// * `prefixes` - Optional partition prefixes to scan (for filtered cold start)
/// * `config` - File listing configuration (extension and target for logging)
///
/// # Returns
///
/// List of file paths (relative to storage root),
/// sorted lexicographically for deterministic ordering.
pub async fn list_files_cold_start(
    storage: &StorageProvider,
    prefixes: Option<&[String]>,
    config: &FileListingConfig<'_>,
) -> Result<Vec<String>, StorageError> {
    match prefixes {
        Some(prefixes) if !prefixes.is_empty() => {
            debug!(
                target = %config.target,
                prefix_count = prefixes.len(),
                "Cold start: scanning partitions with filter"
            );
            let mut files = Vec::new();

            for prefix in prefixes {
                match list_files_in_partition(storage, prefix, config).await {
                    Ok(partition_files) => files.extend(partition_files),
                    Err(e) if e.is_not_found() => {
                        debug!(target = %config.target, prefix = %prefix, "Prefix not found, skipping");
                    }
                    Err(e) => return Err(e),
                }
            }

            files.sort();
            files.dedup();
            Ok(files)
        }
        _ => {
            debug!(
                target = %config.target,
                "Cold start: scanning all files (no filter)"
            );
            list_all_files(storage, config).await
        }
    }
}

/// List all files with the configured extension recursively.
async fn list_all_files(
    storage: &StorageProvider,
    config: &FileListingConfig<'_>,
) -> Result<Vec<String>, StorageError> {
    let mut files = Vec::new();
    let mut stream = storage.list(true).await?;

    while let Some(result) = stream.next().await {
        match result {
            Ok(path) => {
                let path_str = path.to_string();
                // Skip internal directories
                if path_str.starts_with('_') {
                    continue;
                }
                if path_str.ends_with(config.extension) {
                    files.push(path_str);
                }
            }
            Err(e) => {
                warn!(target = %config.target, "Error listing file: {}", e);
            }
        }
    }

    files.sort();
    Ok(files)
}

/// List partition directories in the storage location.
pub async fn list_partitions(storage: &StorageProvider) -> Result<Vec<String>, StorageError> {
    let mut partitions = HashSet::new();
    let mut stream = storage.list(true).await?;

    while let Some(result) = stream.next().await {
        match result {
            Ok(path) => {
                let path_str = path.to_string();
                // Skip internal directories
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
                warn!("Error listing file: {}", e);
            }
        }
    }

    let mut partitions: Vec<_> = partitions.into_iter().collect();
    partitions.sort();
    Ok(partitions)
}

/// List files with the configured extension in a specific partition.
async fn list_files_in_partition(
    storage: &StorageProvider,
    partition: &str,
    config: &FileListingConfig<'_>,
) -> Result<Vec<String>, StorageError> {
    let mut files = Vec::new();

    // Add trailing slash for prefix listing
    let prefix = if partition.ends_with('/') {
        partition.to_string()
    } else {
        format!("{partition}/")
    };

    let mut stream = storage.list_with_prefix(&prefix).await?;

    while let Some(result) = stream.next().await {
        match result {
            Ok(path) => {
                let path_str = path.to_string();
                if path_str.ends_with(config.extension) {
                    files.push(path_str);
                }
            }
            Err(e) => {
                warn!(
                    target = %config.target,
                    "Error listing file in partition {}: {}", partition, e
                );
            }
        }
    }

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    async fn create_test_storage(temp_dir: &TempDir) -> StorageProvider {
        StorageProvider::for_url_with_options(temp_dir.path().to_str().unwrap(), HashMap::new())
            .await
            .unwrap()
    }

    fn ndjson_config(target: &str) -> FileListingConfig<'_> {
        FileListingConfig {
            extension: ".ndjson.gz",
            target,
        }
    }

    fn parquet_config(target: &str) -> FileListingConfig<'_> {
        FileListingConfig {
            extension: ".parquet",
            target,
        }
    }

    #[test]
    fn test_parse_watermark_with_partition() {
        let (partition, filename) = parse_watermark("date=2026-01-28/file.ndjson.gz");
        assert_eq!(partition, "date=2026-01-28");
        assert_eq!(filename, "file.ndjson.gz");
    }

    #[test]
    fn test_parse_watermark_nested_partitions() {
        let (partition, filename) = parse_watermark("date=2026-01-28/hour=14/file.ndjson.gz");
        assert_eq!(partition, "date=2026-01-28/hour=14");
        assert_eq!(filename, "file.ndjson.gz");
    }

    #[test]
    fn test_parse_watermark_no_partition() {
        let (partition, filename) = parse_watermark("file.ndjson.gz");
        assert_eq!(partition, "");
        assert_eq!(filename, "file.ndjson.gz");
    }

    #[tokio::test]
    async fn test_list_above_watermark_ndjson() {
        let temp_dir = TempDir::new().unwrap();

        // Create partition with files
        let partition = temp_dir.path().join("date=2026-01-28");
        std::fs::create_dir_all(&partition).unwrap();

        // Create files with lexicographically ordered names
        std::fs::write(partition.join("1738100100-uuid1.ndjson.gz"), b"").unwrap();
        std::fs::write(partition.join("1738100200-uuid2.ndjson.gz"), b"").unwrap();
        std::fs::write(partition.join("1738100300-uuid3.ndjson.gz"), b"").unwrap();

        let storage = create_test_storage(&temp_dir).await;
        let config = ndjson_config("test");

        // List files above watermark
        let files = list_files_above_watermark(
            &storage,
            "date=2026-01-28/1738100200-uuid2.ndjson.gz",
            &config,
        )
        .await
        .unwrap();

        // Should only find the file after the watermark
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], "date=2026-01-28/1738100300-uuid3.ndjson.gz");
    }

    #[tokio::test]
    async fn test_list_above_watermark_parquet() {
        let temp_dir = TempDir::new().unwrap();

        // Create partition with files
        let partition = temp_dir.path().join("date=2026-01-28");
        std::fs::create_dir_all(&partition).unwrap();

        // Create files with lexicographically ordered names
        std::fs::write(partition.join("01926abc-1111.parquet"), b"").unwrap();
        std::fs::write(partition.join("01926abc-2222.parquet"), b"").unwrap();
        std::fs::write(partition.join("01926abc-3333.parquet"), b"").unwrap();

        let storage = create_test_storage(&temp_dir).await;
        let config = parquet_config("test");

        // List files above watermark
        let files =
            list_files_above_watermark(&storage, "date=2026-01-28/01926abc-2222.parquet", &config)
                .await
                .unwrap();

        // Should only find the file after the watermark
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], "date=2026-01-28/01926abc-3333.parquet");
    }

    #[tokio::test]
    async fn test_list_above_watermark_multiple_partitions() {
        let temp_dir = TempDir::new().unwrap();

        // Create multiple partitions
        let partition1 = temp_dir.path().join("date=2026-01-27");
        let partition2 = temp_dir.path().join("date=2026-01-28");
        let partition3 = temp_dir.path().join("date=2026-01-29");
        std::fs::create_dir_all(&partition1).unwrap();
        std::fs::create_dir_all(&partition2).unwrap();
        std::fs::create_dir_all(&partition3).unwrap();

        std::fs::write(partition1.join("file1.ndjson.gz"), b"").unwrap();
        std::fs::write(partition2.join("file2.ndjson.gz"), b"").unwrap();
        std::fs::write(partition2.join("file3.ndjson.gz"), b"").unwrap();
        std::fs::write(partition3.join("file4.ndjson.gz"), b"").unwrap();

        let storage = create_test_storage(&temp_dir).await;
        let config = ndjson_config("test");

        // List files above watermark in partition 2
        let files =
            list_files_above_watermark(&storage, "date=2026-01-28/file2.ndjson.gz", &config)
                .await
                .unwrap();

        // Should find file3 in partition2 and file4 in partition3
        assert_eq!(files.len(), 2);
        assert_eq!(files[0], "date=2026-01-28/file3.ndjson.gz");
        assert_eq!(files[1], "date=2026-01-29/file4.ndjson.gz");
    }

    #[tokio::test]
    async fn test_list_cold_start_no_prefixes() {
        let temp_dir = TempDir::new().unwrap();

        let partition = temp_dir.path().join("date=2026-01-28");
        std::fs::create_dir_all(&partition).unwrap();
        std::fs::write(partition.join("file1.ndjson.gz"), b"").unwrap();
        std::fs::write(partition.join("file2.ndjson.gz"), b"").unwrap();

        let storage = create_test_storage(&temp_dir).await;
        let config = ndjson_config("test");

        // Cold start - no prefixes
        let files = list_files_cold_start(&storage, None, &config)
            .await
            .unwrap();

        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_list_cold_start_with_prefixes() {
        let temp_dir = TempDir::new().unwrap();

        // Create partitions
        let partition1 = temp_dir.path().join("date=2026-01-27");
        let partition2 = temp_dir.path().join("date=2026-01-28");
        std::fs::create_dir_all(&partition1).unwrap();
        std::fs::create_dir_all(&partition2).unwrap();

        std::fs::write(partition1.join("old-file.ndjson.gz"), b"").unwrap();
        std::fs::write(partition2.join("new-file.ndjson.gz"), b"").unwrap();

        let storage = create_test_storage(&temp_dir).await;
        let config = ndjson_config("test");

        // Cold start with prefix filter
        let prefixes = vec!["date=2026-01-28".to_string()];
        let files = list_files_cold_start(&storage, Some(&prefixes), &config)
            .await
            .unwrap();

        // Should only find file in the filtered partition
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], "date=2026-01-28/new-file.ndjson.gz");
    }

    #[tokio::test]
    async fn test_skips_internal_directories() {
        let temp_dir = TempDir::new().unwrap();

        // Create internal directory that should be skipped
        let internal = temp_dir.path().join("_blizzard");
        std::fs::create_dir_all(&internal).unwrap();
        std::fs::write(internal.join("checkpoint.json"), b"{}").unwrap();

        // Create data partition
        let partition = temp_dir.path().join("date=2026-01-28");
        std::fs::create_dir_all(&partition).unwrap();
        std::fs::write(partition.join("data.ndjson.gz"), b"").unwrap();

        let storage = create_test_storage(&temp_dir).await;
        let config = ndjson_config("test");

        let files = list_files_cold_start(&storage, None, &config)
            .await
            .unwrap();

        // Should only find data file, not internal files
        assert_eq!(files.len(), 1);
        assert!(files[0].contains("data.ndjson.gz"));
    }

    #[tokio::test]
    async fn test_list_partitions() {
        let temp_dir = TempDir::new().unwrap();

        // Create partition directories
        let partition1 = temp_dir.path().join("date=2026-01-27");
        let partition2 = temp_dir.path().join("date=2026-01-28");
        let internal = temp_dir.path().join("_internal");
        std::fs::create_dir_all(&partition1).unwrap();
        std::fs::create_dir_all(&partition2).unwrap();
        std::fs::create_dir_all(&internal).unwrap();

        std::fs::write(partition1.join("file1.parquet"), b"").unwrap();
        std::fs::write(partition2.join("file2.parquet"), b"").unwrap();
        std::fs::write(internal.join("meta.json"), b"{}").unwrap();

        let storage = create_test_storage(&temp_dir).await;

        let partitions = list_partitions(&storage).await.unwrap();

        // Should find data partitions but not internal directories
        assert!(partitions.contains(&"date=2026-01-27".to_string()));
        assert!(partitions.contains(&"date=2026-01-28".to_string()));
        assert!(!partitions.iter().any(|p| p.starts_with('_')));
    }
}
