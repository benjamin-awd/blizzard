//! Configuration for the penguin delta checkpointer.

blizzard_core::define_component_key!(TableKey);

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::info;

use blizzard_core::AppConfig;
pub use blizzard_core::config::{
    ConfigPath, ErrorHandlingConfig, InterpolationResult, Mergeable, MetricsConfig,
    ParquetCompression, PartitionByConfig, PartitionFilterConfig, Resource, StringOrVec,
    interpolate, load_from_paths,
};
use blizzard_core::config::{
    default_max_concurrent_parts, default_max_concurrent_uploads, default_min_multipart_size_mb,
    default_part_size_mb,
};
use blizzard_core::topology::PipelineContext;
pub use blizzard_core::{GlobalConfig, KB, MB};

use crate::pipeline::Pipeline;
use crate::schema::SchemaEvolutionMode;
use blizzard_core::error::ConfigError;

fn default_poll_interval() -> u64 {
    10
}

/// Partition-by configuration: either a list of column names or a
/// template-based config with a `prefix_template` field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PenguinPartitionBy {
    /// List of partition column names (e.g., `[year, month, day, exchange, symbol]`).
    List(Vec<String>),
    /// Template-based config with `prefix_template` (backward compatible).
    Template(PartitionByConfig),
}

impl PenguinPartitionBy {
    /// Return the partition column names for this configuration.
    pub fn partition_columns(&self) -> Vec<String> {
        match self {
            PenguinPartitionBy::List(cols) => cols.clone(),
            PenguinPartitionBy::Template(config) => config.partition_columns(),
        }
    }
}

/// Configuration for a Delta table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    /// URI of the Delta Lake table.
    pub table_uri: String,
    /// Poll interval in seconds for checking new files.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Partition configuration: either a list of column names or a template config.
    pub partition_by: Option<PenguinPartitionBy>,
    /// Path template for extracting column values from positional path segments.
    /// E.g., `"year=%Y/month=%m/day=%d/host={host}/exchange={exchange}/symbol={symbol}"`.
    pub path_columns: Option<String>,
    /// Delta checkpoint interval (number of commits between checkpoints).
    #[serde(default = "default_delta_checkpoint_interval")]
    pub delta_checkpoint_interval: usize,
    /// Maximum concurrent uploads.
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
    /// Maximum concurrent parts per upload.
    #[serde(default = "default_max_concurrent_parts")]
    pub max_concurrent_parts: usize,
    /// Part size for multipart uploads in MB.
    #[serde(default = "default_part_size_mb")]
    pub part_size_mb: usize,
    /// Minimum file size for multipart uploads in MB.
    #[serde(default = "default_min_multipart_size_mb")]
    pub min_multipart_size_mb: usize,
    /// Storage options for Delta Lake storage.
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
    /// Schema evolution mode: "strict", "merge" (default), or "overwrite".
    #[serde(default)]
    pub schema_evolution: SchemaEvolutionMode,
    /// Maximum concurrent parquet metadata reads per poll cycle.
    #[serde(default = "default_max_concurrent_metadata_reads")]
    pub max_concurrent_metadata_reads: usize,
    /// Partition filter for cold start when no watermark exists yet.
    /// Uses strftime-style templates for date-based filtering.
    /// E.g., `prefix_template: "date=%Y-%m-%d"` with `lookback: 7` scans last 7 days.
    pub partition_filter: Option<PartitionFilterConfig>,
}

impl TableConfig {
    /// Returns exclusive resources used by this table configuration.
    ///
    /// Each table claims exclusive access to its table URI to prevent
    /// multiple processors from writing to the same Delta table.
    pub fn resources(&self) -> Vec<Resource> {
        vec![Resource::directory(&self.table_uri)]
    }
}

fn default_delta_checkpoint_interval() -> usize {
    10
}

fn default_max_concurrent_metadata_reads() -> usize {
    32
}

/// Main configuration for penguin.
///
/// # Example
///
/// ```yaml
/// tables:
///   events:
///     table_uri: gs://bucket/events
///     poll_interval_secs: 30
///     partition_filter:
///       prefix_template: "date=%Y-%m-%d"
///       lookback: 7
///   users:
///     table_uri: gs://bucket/users
///
/// global:
///   total_concurrency: 8
///
/// metrics:
///   enabled: true
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Named table configurations.
    #[serde(default)]
    pub tables: IndexMap<TableKey, TableConfig>,
    /// Global configuration options.
    #[serde(default)]
    pub global: GlobalConfig,
    /// Metrics configuration.
    #[serde(default)]
    pub metrics: MetricsConfig,
}

blizzard_core::impl_mergeable!(Config, TableKey, TableConfig, tables);

impl Config {
    fn validate_config(&self) -> Result<(), ConfigError> {
        let mut errors = Vec::new();

        for (key, table) in &self.tables {
            if table.table_uri.is_empty() {
                errors.push(format!("Table '{}': table_uri is empty", key.id()));
            }
        }

        let conflicts = Resource::conflicts(
            self.tables
                .iter()
                .map(|(key, config)| (key.id().to_string(), config.resources())),
        );
        Resource::extend_errors(&mut errors, conflicts);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::MultipleErrors { errors })
        }
    }
}

impl Config {
    /// Iterate over all tables with their keys.
    pub fn tables(&self) -> impl Iterator<Item = (&TableKey, &TableConfig)> {
        self.tables.iter()
    }

    /// Get the number of tables in the configuration.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }
}

impl AppConfig for Config {
    type Pipeline = Pipeline;

    const COMPONENT_NAME: &'static str = "table";

    fn from_paths(paths: &[ConfigPath]) -> Result<Self, ConfigError> {
        let config: Self = load_from_paths(paths)?;
        config.validate()?;
        Ok(config)
    }

    fn create_pipelines(&self, context: PipelineContext) -> Vec<Self::Pipeline> {
        Pipeline::from_config(self, context)
    }

    fn log_startup_info(&self) {
        let table_count = self.table_count();
        info!("Starting checkpointer with {table_count} table(s)");
        for (key, cfg) in self.tables() {
            let uri = &cfg.table_uri;
            info!("  Table: {key} ({uri})");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_table_parse() {
        let yaml = r#"
tables:
  events:
    table_uri: gs://bucket/events
    poll_interval_secs: 30
"#;
        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.table_count(), 1);

        let (key, table) = config.tables().next().unwrap();
        assert_eq!(key.id(), "events");
        assert_eq!(table.table_uri, "gs://bucket/events");
        assert_eq!(table.poll_interval_secs, 30);
    }

    #[test]
    fn test_multi_table_parse() {
        let yaml = r#"
tables:
  events:
    table_uri: gs://bucket/events
    poll_interval_secs: 30
  users:
    table_uri: gs://bucket/users
    poll_interval_secs: 60

global:
  total_concurrency: 8
"#;
        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.table_count(), 2);
        assert_eq!(config.global.total_concurrency, Some(8));

        let tables: Vec<_> = config.tables().collect();
        assert_eq!(tables.len(), 2);

        // IndexMap preserves insertion order
        assert_eq!(tables[0].0.id(), "events");
        assert_eq!(tables[0].1.table_uri, "gs://bucket/events");
        assert_eq!(tables[1].0.id(), "users");
        assert_eq!(tables[1].1.table_uri, "gs://bucket/users");
    }

    #[test]
    fn test_empty_uri_error() {
        let yaml = r#"
tables:
  events:
    table_uri: gs://bucket/events
  users:
    table_uri: ""
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("users"));
        assert!(err.to_string().contains("table_uri is empty"));
    }

    #[test]
    fn test_metrics_default() {
        let yaml = r#"
tables:
  events:
    table_uri: gs://bucket/events
"#;
        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.metrics.address, "0.0.0.0:9090");
    }

    #[test]
    fn test_global_default() {
        let yaml = r#"
tables:
  events:
    table_uri: gs://bucket/events
"#;
        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.global.total_concurrency, None);
    }

    #[test]
    fn test_table_config_defaults() {
        let yaml = r#"
tables:
  events:
    table_uri: gs://bucket/events
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, table) = config.tables().next().unwrap();

        assert_eq!(table.poll_interval_secs, 10);
        assert_eq!(table.delta_checkpoint_interval, 10);
        assert_eq!(table.max_concurrent_uploads, 4);
        assert_eq!(table.max_concurrent_parts, 8);
        assert_eq!(table.part_size_mb, 10);
        assert_eq!(table.min_multipart_size_mb, 100);
        assert!(table.partition_by.is_none());
        assert!(table.storage_options.is_empty());
        assert!(table.partition_filter.is_none());
    }

    #[test]
    fn test_table_config_resources() {
        let table = TableConfig {
            table_uri: "gs://bucket/my_table".to_string(),
            poll_interval_secs: 10,
            partition_by: None,
            path_columns: None,
            delta_checkpoint_interval: 10,
            max_concurrent_uploads: 4,
            max_concurrent_parts: 8,
            part_size_mb: 10,
            min_multipart_size_mb: 100,
            storage_options: HashMap::new(),
            schema_evolution: Default::default(),
            max_concurrent_metadata_reads: 32,
            partition_filter: None,
        };

        let resources = table.resources();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0], Resource::directory("gs://bucket/my_table"));
    }

    #[test]
    fn test_partition_filter_config() {
        let yaml = r#"
tables:
  events:
    table_uri: gs://bucket/events
    partition_filter:
      prefix_template: "date=%Y-%m-%d"
      lookback: 7
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, table) = config.tables().next().unwrap();

        let filter = table.partition_filter.as_ref().unwrap();
        assert_eq!(filter.prefix_template, "date=%Y-%m-%d");
        assert_eq!(filter.lookback, 7);
        assert!(filter.include.is_empty());
    }

    #[test]
    fn test_partition_filter_include_single_string() {
        let yaml = r#"
tables:
  telemetry:
    table_uri: s3://bucket/data/telemetry
    partition_filter:
      prefix_template: "%Y/%m/%d/{host}/{region}/{category}"
      lookback: 0
      include:
        region: "us-east-1"
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, table) = config.tables().next().unwrap();

        let filter = table.partition_filter.as_ref().unwrap();
        let region = filter.include.get("region").unwrap();
        assert_eq!(region.values(), &["us-east-1"]);
    }

    #[test]
    fn test_partition_filter_include_list() {
        let yaml = r#"
tables:
  telemetry:
    table_uri: s3://bucket/data/telemetry
    partition_filter:
      prefix_template: "%Y/%m/%d/{host}/{region}/{category}"
      lookback: 0
      include:
        host:
          - "web-prod-01"
          - "web-prod-02"
        category: ["events", "metrics"]
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, table) = config.tables().next().unwrap();

        let filter = table.partition_filter.as_ref().unwrap();
        let host = filter.include.get("host").unwrap();
        assert_eq!(host.values(), &["web-prod-01", "web-prod-02"]);
        let category = filter.include.get("category").unwrap();
        assert_eq!(category.values(), &["events", "metrics"]);
    }

    #[test]
    fn test_partition_filter_include_mixed() {
        let yaml = r#"
tables:
  telemetry:
    table_uri: s3://bucket/data/telemetry
    partition_filter:
      prefix_template: "%Y/%m/%d/{host}/{region}/{category}"
      lookback: 0
      include:
        host:
          - "web-prod-01"
          - "web-prod-02"
        region: "us-east-1"
        category: ["events", "metrics"]
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, table) = config.tables().next().unwrap();

        let filter = table.partition_filter.as_ref().unwrap();
        assert_eq!(filter.include.len(), 3);
        assert_eq!(
            filter.include.get("host").unwrap().values(),
            &["web-prod-01", "web-prod-02"]
        );
        assert_eq!(
            filter.include.get("region").unwrap().values(),
            &["us-east-1"]
        );
        assert_eq!(
            filter.include.get("category").unwrap().values(),
            &["events", "metrics"]
        );
    }

    #[test]
    fn test_resource_conflict_same_uri() {
        let yaml = r#"
tables:
  a:
    table_uri: gs://bucket/delta/same
  b:
    table_uri: gs://bucket/delta/same
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Resource conflict"),
            "Expected resource conflict error, got: {msg}"
        );
        assert!(
            msg.contains("gs://bucket/delta/same"),
            "Expected table URI in error, got: {msg}"
        );
    }

    #[test]
    fn test_no_resource_conflict_different_uris() {
        let yaml = r#"
tables:
  a:
    table_uri: gs://bucket/delta/table_a
  b:
    table_uri: gs://bucket/delta/table_b
"#;
        let result = Config::parse(yaml);
        assert!(result.is_ok());
    }

    #[test]
    fn test_partition_by_config_partition_columns() {
        let config = PartitionByConfig {
            prefix_template: "date=%Y-%m-%d/hour=%H".to_string(),
        };
        let columns = config.partition_columns();
        assert_eq!(columns, vec!["date", "hour"]);
    }

    #[test]
    fn test_partition_by_template_yaml_parsing() {
        let yaml = r#"
tables:
  events:
    table_uri: gs://bucket/events
    partition_by:
      prefix_template: "date=%Y-%m-%d"
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, table) = config.tables().next().unwrap();

        let partition_by = table.partition_by.as_ref().unwrap();
        assert!(matches!(partition_by, PenguinPartitionBy::Template(_)));
        assert_eq!(partition_by.partition_columns(), vec!["date"]);
    }

    #[test]
    fn test_partition_by_list_yaml_parsing() {
        let yaml = r#"
tables:
  logs:
    table_uri: s3://bucket/logs
    partition_by: [year, month, day, category, source]
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, table) = config.tables().next().unwrap();

        let partition_by = table.partition_by.as_ref().unwrap();
        assert!(matches!(partition_by, PenguinPartitionBy::List(_)));
        assert_eq!(
            partition_by.partition_columns(),
            vec!["year", "month", "day", "category", "source"]
        );
    }

    #[test]
    fn test_path_columns_yaml_parsing() {
        let yaml = r#"
tables:
  logs:
    table_uri: s3://bucket/logs
    path_columns: "year=%Y/month=%m/day=%d/region={region}/category={category}/source={source}"
    partition_by: [year, month, day, category, source]
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, table) = config.tables().next().unwrap();

        assert_eq!(
            table.path_columns.as_deref(),
            Some("year=%Y/month=%m/day=%d/region={region}/category={category}/source={source}")
        );
        assert!(matches!(
            table.partition_by.as_ref().unwrap(),
            PenguinPartitionBy::List(_)
        ));
    }

    #[test]
    fn test_unknown_field_rejected_in_table() {
        let yaml = r#"
tables:
  events:
    table_uri: gs://bucket/events
    pollinterval: 30
"#;
        let result = Config::parse(yaml);
        assert!(
            result.is_err(),
            "Should reject unknown field 'pollinterval'"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field"),
            "Error should mention unknown field: {err}"
        );
    }

    #[test]
    fn test_unknown_field_rejected_at_top_level() {
        let yaml = r#"
tables:
  events:
    table_uri: gs://bucket/events
unknown_key: value
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err(), "Should reject unknown field 'unknown_key'");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field"),
            "Error should mention unknown field: {err}"
        );
    }

    #[test]
    fn test_multiple_errors_collected() {
        let yaml = r#"
tables:
  a:
    table_uri: ""
  b:
    table_uri: ""
  c:
    table_uri: gs://bucket/valid
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err(), "Should have validation errors");
        let err = result.unwrap_err().to_string();
        // Should contain errors for both 'a' and 'b' tables
        assert!(err.contains("Table 'a'"), "Should mention table 'a': {err}");
        assert!(err.contains("Table 'b'"), "Should mention table 'b': {err}");
        assert!(
            err.contains("table_uri is empty"),
            "Should mention empty table_uri: {err}"
        );
    }
}
