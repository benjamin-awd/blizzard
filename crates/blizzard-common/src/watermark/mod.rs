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

#[cfg(test)]
mod tests {
    use super::*;

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
