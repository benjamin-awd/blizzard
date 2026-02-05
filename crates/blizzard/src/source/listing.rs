//! Watermark-based file listing for efficient incremental discovery.
//!
//! Provides functions to list NDJSON files above a high-watermark,
//! enabling efficient incremental processing without maintaining
//! an unbounded in-memory set of processed files.
//!
//! # Watermark Format
//!
//! Watermarks are lexicographically-sortable file paths:
//! - `date=2026-01-28/1738100400-uuid.ndjson.gz`
//! - `date=2026-01-28/hour=14/1738100400-uuid.ndjson.gz`
//!
//! Files must be named such that lexicographic order matches chronological order
//! (e.g., using timestamp prefixes or UUIDv7).

use std::collections::HashMap;

use blizzard_core::error::StorageError;
use blizzard_core::storage::StorageProvider;
use blizzard_core::watermark::{self, FileListingConfig};

/// File extension for NDJSON gzip files.
const NDJSON_EXTENSION: &str = ".ndjson.gz";

/// List NDJSON files above the given watermark.
///
/// Uses the watermark's partition and filename to efficiently narrow listing:
/// - Only scans partitions >= watermark's partition
/// - Filters to files > watermark (lexicographic comparison)
///
/// # Arguments
///
/// * `storage` - Storage provider for the source directory
/// * `watermark` - Optional high-watermark (last processed file path)
/// * `prefixes` - Optional partition prefixes to scan (for cold start)
/// * `pipeline` - Pipeline identifier for logging
///
/// # Returns
///
/// List of file paths (relative to storage root) above the watermark,
/// sorted lexicographically for deterministic ordering.
pub async fn list_ndjson_files_above_watermark(
    storage: &StorageProvider,
    watermark: Option<&str>,
    prefixes: Option<&[String]>,
    pipeline: &str,
) -> Result<Vec<String>, StorageError> {
    let config = FileListingConfig {
        extension: NDJSON_EXTENSION,
        target: pipeline,
    };

    match watermark {
        Some(wm) => watermark::list_files_above_watermark(storage, wm, &config).await,
        None => watermark::list_files_cold_start(storage, prefixes, &config).await,
    }
}

/// List NDJSON files using per-partition watermarks for efficient filtering.
///
/// When partition watermarks are available and non-empty, uses them for
/// more efficient filtering of concurrent partitions. Falls back to the
/// global watermark when partition watermarks are empty.
///
/// # Arguments
///
/// * `storage` - Storage provider for the source directory
/// * `watermark` - Optional global high-watermark (for fallback)
/// * `partition_watermarks` - Per-partition watermarks (partition → filename)
/// * `prefixes` - Optional partition prefixes to scan (for cold start)
/// * `pipeline` - Pipeline identifier for logging
pub async fn list_ndjson_files_with_partition_watermarks(
    storage: &StorageProvider,
    watermark: Option<&str>,
    partition_watermarks: Option<&HashMap<String, String>>,
    prefixes: Option<&[String]>,
    pipeline: &str,
) -> Result<Vec<String>, StorageError> {
    let config = FileListingConfig {
        extension: NDJSON_EXTENSION,
        target: pipeline,
    };

    // Use partition watermarks if available and non-empty
    if let Some(pw) = partition_watermarks
        && !pw.is_empty() {
            return watermark::list_files_above_partition_watermarks(storage, pw, prefixes, &config)
                .await;
        }

    // Fall back to global watermark logic
    match watermark {
        Some(wm) => watermark::list_files_above_watermark(storage, wm, &config).await,
        None => watermark::list_files_cold_start(storage, prefixes, &config).await,
    }
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

    // parse_watermark tests are in blizzard-core/src/watermark/listing.rs

    #[tokio::test]
    async fn test_list_above_watermark() {
        let temp_dir = TempDir::new().unwrap();

        // Create partition with files
        let partition = temp_dir.path().join("date=2026-01-28");
        std::fs::create_dir_all(&partition).unwrap();

        // Create files with lexicographically ordered names
        std::fs::write(partition.join("1738100100-uuid1.ndjson.gz"), b"").unwrap();
        std::fs::write(partition.join("1738100200-uuid2.ndjson.gz"), b"").unwrap();
        std::fs::write(partition.join("1738100300-uuid3.ndjson.gz"), b"").unwrap();

        let storage = create_test_storage(&temp_dir).await;

        // List files above watermark
        let files = list_ndjson_files_above_watermark(
            &storage,
            Some("date=2026-01-28/1738100200-uuid2.ndjson.gz"),
            None,
            "test",
        )
        .await
        .unwrap();

        // Should only find the file after the watermark
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], "date=2026-01-28/1738100300-uuid3.ndjson.gz");
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

        // List files above watermark in partition 2
        let files = list_ndjson_files_above_watermark(
            &storage,
            Some("date=2026-01-28/file2.ndjson.gz"),
            None,
            "test",
        )
        .await
        .unwrap();

        // Should find file3 in partition2 and file4 in partition3
        assert_eq!(files.len(), 2);
        assert_eq!(files[0], "date=2026-01-28/file3.ndjson.gz");
        assert_eq!(files[1], "date=2026-01-29/file4.ndjson.gz");
    }

    #[tokio::test]
    async fn test_list_cold_start_no_watermark() {
        let temp_dir = TempDir::new().unwrap();

        let partition = temp_dir.path().join("date=2026-01-28");
        std::fs::create_dir_all(&partition).unwrap();
        std::fs::write(partition.join("file1.ndjson.gz"), b"").unwrap();
        std::fs::write(partition.join("file2.ndjson.gz"), b"").unwrap();

        let storage = create_test_storage(&temp_dir).await;

        // Cold start - no watermark, no prefixes
        let files = list_ndjson_files_above_watermark(&storage, None, None, "test")
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

        // Cold start with prefix filter
        let prefixes = vec!["date=2026-01-28".to_string()];
        let files = list_ndjson_files_above_watermark(&storage, None, Some(&prefixes), "test")
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

        let files = list_ndjson_files_above_watermark(&storage, None, None, "test")
            .await
            .unwrap();

        // Should only find data file, not internal files
        assert_eq!(files.len(), 1);
        assert!(files[0].contains("data.ndjson.gz"));
    }
}
