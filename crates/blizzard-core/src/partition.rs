//! Partition value extraction from file paths.
//!
//! Extracts `key=value` patterns from Hive-style partitioned file paths.

use std::collections::HashMap;

/// Extracts partition values from file paths.
///
/// Supports two modes:
/// - **Specific columns**: Extract only values for configured partition columns
/// - **All columns**: Extract all `key=value` patterns found in the path
///
/// # Examples
///
/// ```
/// use blizzard_core::PartitionExtractor;
///
/// // Extract all partition values
/// let extractor = PartitionExtractor::all();
/// let values = extractor.extract("date=2024-01-15/hour=12/file.parquet");
/// assert_eq!(values.get("date"), Some(&"2024-01-15".to_string()));
/// assert_eq!(values.get("hour"), Some(&"12".to_string()));
///
/// // Extract only specific columns
/// let extractor = PartitionExtractor::new(vec!["date".into()]);
/// let values = extractor.extract("date=2024-01-15/hour=12/file.parquet");
/// assert_eq!(values.get("date"), Some(&"2024-01-15".to_string()));
/// assert_eq!(values.get("hour"), None); // Not in configured columns
/// ```
#[derive(Debug, Clone)]
pub struct PartitionExtractor {
    /// The partition column names to extract. None means extract all.
    columns: Option<Vec<String>>,
}

impl PartitionExtractor {
    /// Create an extractor for specific partition columns.
    ///
    /// Only values for the specified columns will be extracted.
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns: Some(columns),
        }
    }

    /// Create an extractor that extracts all partition columns.
    ///
    /// All `key=value` patterns in the path will be extracted.
    pub fn all() -> Self {
        Self { columns: None }
    }

    /// Extract partition values from a source path.
    ///
    /// For Hive-style paths like `date=2024-01-15/hour=12/file.parquet`,
    /// extracts the key-value pairs based on the extractor's configuration.
    pub fn extract(&self, path: &str) -> HashMap<String, String> {
        match &self.columns {
            Some(cols) => self.extract_specific(path, cols),
            None => self.extract_all(path),
        }
    }

    /// Extract all partition values from a path.
    fn extract_all(&self, path: &str) -> HashMap<String, String> {
        let mut values = HashMap::new();

        for segment in path.split('/') {
            if let Some(eq_pos) = segment.find('=') {
                let key = &segment[..eq_pos];
                let value = &segment[eq_pos + 1..];
                // Skip if this looks like a filename (contains a dot after the '=')
                if !value.contains('.') {
                    values.insert(key.to_string(), value.to_string());
                }
            }
        }

        values
    }

    /// Extract only specific partition columns from a path.
    fn extract_specific(&self, path: &str, columns: &[String]) -> HashMap<String, String> {
        columns
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

    // Tests for PartitionExtractor::all()

    #[test]
    fn test_extract_all_multiple_partitions() {
        let extractor = PartitionExtractor::all();
        let values = extractor.extract("date=2024-01-28/hour=14/file.parquet");

        assert_eq!(values.get("date"), Some(&"2024-01-28".to_string()));
        assert_eq!(values.get("hour"), Some(&"14".to_string()));
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_extract_all_single_partition() {
        let extractor = PartitionExtractor::all();
        let values = extractor.extract("date=2024-01-28/file.parquet");

        assert_eq!(values.get("date"), Some(&"2024-01-28".to_string()));
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn test_extract_all_no_partitions() {
        let extractor = PartitionExtractor::all();
        let values = extractor.extract("file.parquet");

        assert!(values.is_empty());
    }

    #[test]
    fn test_extract_all_nested_partitions() {
        let extractor = PartitionExtractor::all();
        let values = extractor.extract("year=2024/month=01/day=28/file.parquet");

        assert_eq!(values.get("year"), Some(&"2024".to_string()));
        assert_eq!(values.get("month"), Some(&"01".to_string()));
        assert_eq!(values.get("day"), Some(&"28".to_string()));
        assert_eq!(values.len(), 3);
    }

    // Tests for PartitionExtractor::new() with specific columns

    #[test]
    fn test_extract_specific_single_partition() {
        let extractor = PartitionExtractor::new(vec!["date".into()]);
        let values = extractor.extract("s3://bucket/date=2024-01-15/file.json");

        assert_eq!(values.len(), 1);
        assert_eq!(values.get("date"), Some(&"2024-01-15".to_string()));
    }

    #[test]
    fn test_extract_specific_multiple_partitions() {
        let extractor = PartitionExtractor::new(vec!["date".into(), "hour".into()]);
        let values = extractor.extract("s3://bucket/date=2024-01-15/hour=12/file.json");

        assert_eq!(values.len(), 2);
        assert_eq!(values.get("date"), Some(&"2024-01-15".to_string()));
        assert_eq!(values.get("hour"), Some(&"12".to_string()));
    }

    #[test]
    fn test_extract_specific_missing_partition() {
        let extractor = PartitionExtractor::new(vec!["date".into(), "region".into()]);
        let values = extractor.extract("s3://bucket/date=2024-01-15/file.json");

        assert_eq!(values.len(), 1);
        assert_eq!(values.get("date"), Some(&"2024-01-15".to_string()));
        assert_eq!(values.get("region"), None);
    }

    #[test]
    fn test_extract_specific_empty_columns() {
        let extractor = PartitionExtractor::new(vec![]);
        let values = extractor.extract("s3://bucket/date=2024-01-15/file.json");

        assert!(values.is_empty());
    }

    #[test]
    fn test_extract_specific_value_at_end_of_path() {
        let extractor = PartitionExtractor::new(vec!["id".into()]);
        let values = extractor.extract("s3://bucket/id=12345");

        assert_eq!(values.get("id"), Some(&"12345".to_string()));
    }

    #[test]
    fn test_extract_specific_filters_columns() {
        // Path has date and hour, but we only ask for date
        let extractor = PartitionExtractor::new(vec!["date".into()]);
        let values = extractor.extract("date=2024-01-15/hour=12/file.parquet");

        assert_eq!(values.len(), 1);
        assert_eq!(values.get("date"), Some(&"2024-01-15".to_string()));
        assert_eq!(values.get("hour"), None);
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
