//! Partition value extraction from file paths.
//!
//! Extracts `key=value` patterns from Hive-style partitioned file paths.

use std::collections::HashMap;

/// Extracts partition values from file paths.
///
/// Supports three modes:
/// - **All columns**: Extract all `key=value` patterns found in the path
/// - **Specific columns**: Extract only values for configured partition columns
/// - **Template**: Extract values by positional matching against a path template
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
///
/// // Template-based extraction from non-Hive paths
/// let extractor = PartitionExtractor::from_template(
///     "year=%Y/month=%m/day=%d/region={region}/category={category}/source={source}",
///     Some(vec!["year".into(), "category".into(), "source".into()]),
/// );
/// let values = extractor.extract("2026/02/03/us-east-1/events/app-server/file.parquet");
/// assert_eq!(values.get("year"), Some(&"2026".to_string()));
/// assert_eq!(values.get("category"), Some(&"events".to_string()));
/// assert_eq!(values.get("source"), Some(&"app-server".to_string()));
/// assert_eq!(values.get("region"), None); // Not in filter
/// ```
#[derive(Debug, Clone)]
pub struct PartitionExtractor {
    mode: ExtractionMode,
}

#[derive(Debug, Clone)]
enum ExtractionMode {
    /// Extract all `key=value` patterns found in the path.
    All,
    /// Extract only values for specific `key=value` columns.
    Specific(Vec<String>),
    /// Extract values by positional matching against a path template.
    Template {
        /// Column names parsed from template segments (one per `/`-delimited segment).
        segments: Vec<String>,
        /// If set, only return values for these columns.
        filter: Option<Vec<String>>,
    },
}

impl PartitionExtractor {
    /// Create an extractor for specific partition columns.
    ///
    /// Only values for the specified columns will be extracted from `key=value` segments.
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            mode: ExtractionMode::Specific(columns),
        }
    }

    /// Create an extractor that extracts all partition columns.
    ///
    /// All `key=value` patterns in the path will be extracted.
    pub fn all() -> Self {
        Self {
            mode: ExtractionMode::All,
        }
    }

    /// Create a template-based extractor for non-Hive paths.
    ///
    /// The template is a `/`-delimited string where each segment's column name
    /// is the part before `=` (e.g., `"year=%Y/host={host}"`). Extraction is
    /// purely positional — the right side of `=` is informational only.
    ///
    /// If `filter` is `Some`, only columns in the filter list are included in
    /// the output. Other columns are still used for positional alignment.
    pub fn from_template(template: &str, filter: Option<Vec<String>>) -> Self {
        let segments: Vec<String> = template
            .split('/')
            .map(|seg| {
                seg.find('=')
                    .map(|idx| seg[..idx].to_string())
                    .unwrap_or_else(|| seg.to_string())
            })
            .collect();
        Self {
            mode: ExtractionMode::Template { segments, filter },
        }
    }

    /// Return all column names this extractor can produce.
    ///
    /// For `All` mode, returns an empty vec (columns are dynamic).
    /// For `Specific`, returns the configured column names.
    /// For `Template`, returns the filtered column names (or all segment names if no filter).
    pub fn column_names(&self) -> Vec<String> {
        match &self.mode {
            ExtractionMode::All => vec![],
            ExtractionMode::Specific(cols) => cols.clone(),
            ExtractionMode::Template { segments, filter } => match filter {
                Some(f) => f.clone(),
                None => segments.clone(),
            },
        }
    }

    /// Extract partition values from a source path.
    pub fn extract(&self, path: &str) -> HashMap<String, String> {
        match &self.mode {
            ExtractionMode::All => self.extract_all(path),
            ExtractionMode::Specific(cols) => self.extract_specific(path, cols),
            ExtractionMode::Template { segments, filter } => {
                self.extract_template(path, segments, filter.as_deref())
            }
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

    /// Extract values by positional matching against template segments.
    ///
    /// Path segments beyond the template length (e.g., the trailing filename)
    /// are ignored. Template segments beyond the path length are also ignored.
    fn extract_template(
        &self,
        path: &str,
        segments: &[String],
        filter: Option<&[String]>,
    ) -> HashMap<String, String> {
        let path_parts: Vec<&str> = path.split('/').collect();
        let mut values = HashMap::new();

        for (i, col_name) in segments.iter().enumerate() {
            if i >= path_parts.len() {
                break;
            }
            if let Some(filter) = filter
                && !filter.iter().any(|f| f == col_name)
            {
                continue;
            }
            values.insert(col_name.clone(), path_parts[i].to_string());
        }

        values
    }
}

/// Extract a single partition value from a path for a given key.
///
/// Looks for `key=value` pattern and extracts the value (up to the next `/` or end of string).
fn extract_value(path: &str, key: &str) -> Option<String> {
    let pattern = format!("{key}=");
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

    // Tests for PartitionExtractor::from_template()

    #[test]
    fn test_template_parsing() {
        let extractor = PartitionExtractor::from_template(
            "year=%Y/month=%m/day=%d/region={region}/category={category}/source={source}",
            None,
        );
        assert_eq!(
            extractor.column_names(),
            vec!["year", "month", "day", "region", "category", "source"]
        );
    }

    #[test]
    fn test_template_extraction() {
        let extractor = PartitionExtractor::from_template(
            "year=%Y/month=%m/day=%d/region={region}/category={category}/source={source}",
            None,
        );
        let values = extractor.extract("2026/02/03/us-east-1/events/app-server/file.parquet");

        assert_eq!(values.get("year"), Some(&"2026".to_string()));
        assert_eq!(values.get("month"), Some(&"02".to_string()));
        assert_eq!(values.get("day"), Some(&"03".to_string()));
        assert_eq!(values.get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(values.get("category"), Some(&"events".to_string()));
        assert_eq!(values.get("source"), Some(&"app-server".to_string()));
        assert_eq!(values.len(), 6);
    }

    #[test]
    fn test_template_extraction_with_filter() {
        let extractor = PartitionExtractor::from_template(
            "year=%Y/month=%m/day=%d/region={region}/category={category}/source={source}",
            Some(vec![
                "year".into(),
                "month".into(),
                "day".into(),
                "category".into(),
                "source".into(),
            ]),
        );
        let values = extractor.extract("2026/02/03/us-east-1/events/app-server/file.parquet");

        assert_eq!(values.get("year"), Some(&"2026".to_string()));
        assert_eq!(values.get("category"), Some(&"events".to_string()));
        assert_eq!(values.get("source"), Some(&"app-server".to_string()));
        assert_eq!(values.get("region"), None); // Filtered out
        assert_eq!(values.len(), 5);
    }

    #[test]
    fn test_template_column_names_with_filter() {
        let extractor = PartitionExtractor::from_template(
            "year=%Y/month=%m/day=%d/region={region}",
            Some(vec!["year".into(), "day".into()]),
        );
        assert_eq!(extractor.column_names(), vec!["year", "day"]);
    }

    #[test]
    fn test_template_path_shorter_than_template() {
        let extractor =
            PartitionExtractor::from_template("year=%Y/month=%m/day=%d/region={region}", None);
        let values = extractor.extract("2026/02");

        assert_eq!(values.get("year"), Some(&"2026".to_string()));
        assert_eq!(values.get("month"), Some(&"02".to_string()));
        assert_eq!(values.get("day"), None);
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_template_path_longer_than_template() {
        let extractor = PartitionExtractor::from_template("year=%Y/month=%m", None);
        let values = extractor.extract("2026/02/03/us-east-1/events/app-server/file.parquet");

        assert_eq!(values.get("year"), Some(&"2026".to_string()));
        assert_eq!(values.get("month"), Some(&"02".to_string()));
        assert_eq!(values.len(), 2);
    }
}
