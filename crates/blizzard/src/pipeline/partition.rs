//! Partition value extraction from file paths.
//!
//! Extracts `key=value` patterns from source paths for Hive-style partitioning.

use std::collections::HashMap;

/// Extracts partition values from file paths based on configured partition columns.
///
/// Looks for `key=value` patterns in paths and extracts values for each configured
/// partition column.
#[derive(Debug, Clone)]
pub struct PartitionExtractor {
    /// The partition column names to extract (e.g., ["date", "hour"]).
    columns: Vec<String>,
}

impl PartitionExtractor {
    /// Create a new extractor for the given partition columns.
    pub fn new(columns: Vec<String>) -> Self {
        Self { columns }
    }

    /// Extract partition values from a source path.
    ///
    /// For each configured partition column, looks for `column=value` pattern
    /// in the path and extracts the value.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let extractor = PartitionExtractor::new(vec!["date".into(), "hour".into()]);
    /// let values = extractor.extract("s3://bucket/date=2024-01-15/hour=12/file.json");
    /// assert_eq!(values.get("date"), Some(&"2024-01-15".to_string()));
    /// assert_eq!(values.get("hour"), Some(&"12".to_string()));
    /// ```
    pub fn extract(&self, path: &str) -> HashMap<String, String> {
        self.columns
            .iter()
            .filter_map(|key| extract_value(path, key).map(|value| (key.clone(), value)))
            .collect()
    }
}

/// Extract a single partition value from a path for a given key.
///
/// Looks for `key=value` pattern and extracts the value (up to the next `/` or end of string).
fn extract_value(path: &str, key: &str) -> Option<String> {
    let pattern = format!("{}=", key);
    let start = path.find(&pattern)? + pattern.len();
    let rest = &path[start..];
    let end = rest.find('/').unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_single_partition() {
        let extractor = PartitionExtractor::new(vec!["date".into()]);
        let values = extractor.extract("s3://bucket/date=2024-01-15/file.json");

        assert_eq!(values.len(), 1);
        assert_eq!(values.get("date"), Some(&"2024-01-15".to_string()));
    }

    #[test]
    fn test_extract_multiple_partitions() {
        let extractor = PartitionExtractor::new(vec!["date".into(), "hour".into()]);
        let values = extractor.extract("s3://bucket/date=2024-01-15/hour=12/file.json");

        assert_eq!(values.len(), 2);
        assert_eq!(values.get("date"), Some(&"2024-01-15".to_string()));
        assert_eq!(values.get("hour"), Some(&"12".to_string()));
    }

    #[test]
    fn test_extract_missing_partition() {
        let extractor = PartitionExtractor::new(vec!["date".into(), "region".into()]);
        let values = extractor.extract("s3://bucket/date=2024-01-15/file.json");

        assert_eq!(values.len(), 1);
        assert_eq!(values.get("date"), Some(&"2024-01-15".to_string()));
        assert_eq!(values.get("region"), None);
    }

    #[test]
    fn test_extract_empty_columns() {
        let extractor = PartitionExtractor::new(vec![]);
        let values = extractor.extract("s3://bucket/date=2024-01-15/file.json");

        assert!(values.is_empty());
    }

    #[test]
    fn test_extract_value_at_end_of_path() {
        let extractor = PartitionExtractor::new(vec!["id".into()]);
        let values = extractor.extract("s3://bucket/id=12345");

        assert_eq!(values.get("id"), Some(&"12345".to_string()));
    }

    #[test]
    fn test_extract_value_helper() {
        assert_eq!(
            extract_value("path/date=2024-01-15/file", "date"),
            Some("2024-01-15".to_string())
        );
        assert_eq!(extract_value("path/file", "date"), None);
        assert_eq!(
            extract_value("date=value", "date"),
            Some("value".to_string())
        );
    }
}
