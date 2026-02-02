//! Watermark-based file listing for efficient incremental discovery.
//!
//! Provides functions to list files above a high-watermark,
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

use crate::storage::DatePrefixGenerator;

pub mod listing;

pub use listing::{
    FileListingConfig, list_files_above_watermark, list_files_cold_start, list_partitions,
    parse_watermark,
};

/// Generate date prefixes from a partition filter config.
///
/// Utility function to create prefixes for cold start scanning based on
/// a date format template and lookback period.
///
/// # Arguments
///
/// * `prefix_template` - A strftime-style template (e.g., "date=%Y-%m-%d")
/// * `lookback` - Number of days to look back from today
///
/// # Returns
///
/// A vector of formatted prefix strings for each day in the lookback period.
///
/// # Examples
///
/// ```
/// use blizzard_common::watermark::generate_prefixes;
///
/// // Generate prefixes for the last 3 days
/// let prefixes = generate_prefixes("date=%Y-%m-%d", 3);
/// assert_eq!(prefixes.len(), 3);
/// ```
pub fn generate_prefixes(prefix_template: &str, lookback: u32) -> Vec<String> {
    DatePrefixGenerator::new(prefix_template, lookback).generate_prefixes()
}

/// Parse partition values from a Hive-style partitioned file path.
///
/// Extracts key=value pairs from path segments, excluding the filename.
///
/// # Arguments
///
/// * `path` - A file path with Hive-style partition segments
///
/// # Returns
///
/// A HashMap of partition keys to their values.
///
/// # Examples
///
/// ```
/// use blizzard_common::watermark::parse_partition_values;
///
/// let values = parse_partition_values("date=2024-01-28/hour=14/file.parquet");
/// assert_eq!(values.get("date"), Some(&"2024-01-28".to_string()));
/// assert_eq!(values.get("hour"), Some(&"14".to_string()));
/// assert_eq!(values.len(), 2);
///
/// let values = parse_partition_values("file.parquet");
/// assert!(values.is_empty());
/// ```
pub fn parse_partition_values(path: &str) -> HashMap<String, String> {
    let mut values = HashMap::new();

    for segment in path.split('/') {
        if let Some(eq_pos) = segment.find('=') {
            let key = &segment[..eq_pos];
            let value = &segment[eq_pos + 1..];
            // Skip if this looks like a filename (contains a dot after the '=')
            // This handles cases like "key=value.parquet" which is a filename, not a partition
            if !value.contains('.') {
                values.insert(key.to_string(), value.to_string());
            }
        }
    }

    values
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_partition_values_multiple() {
        let values = parse_partition_values("date=2024-01-28/hour=14/file.parquet");
        assert_eq!(values.get("date"), Some(&"2024-01-28".to_string()));
        assert_eq!(values.get("hour"), Some(&"14".to_string()));
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_parse_partition_values_single() {
        let values = parse_partition_values("date=2024-01-28/file.parquet");
        assert_eq!(values.get("date"), Some(&"2024-01-28".to_string()));
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn test_parse_partition_values_no_partitions() {
        let values = parse_partition_values("file.parquet");
        assert!(values.is_empty());
    }

    #[test]
    fn test_parse_partition_values_nested() {
        let values = parse_partition_values("year=2024/month=01/day=28/file.parquet");
        assert_eq!(values.get("year"), Some(&"2024".to_string()));
        assert_eq!(values.get("month"), Some(&"01".to_string()));
        assert_eq!(values.get("day"), Some(&"28".to_string()));
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_generate_prefixes() {
        // lookback=2 means look back 2 days, so we get 3 prefixes: today, yesterday, 2 days ago
        let prefixes = generate_prefixes("date=%Y-%m-%d", 2);
        assert_eq!(prefixes.len(), 3);
        // Each prefix should start with "date="
        for prefix in &prefixes {
            assert!(prefix.starts_with("date="));
        }
    }
}
