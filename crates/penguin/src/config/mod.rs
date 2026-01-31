//! Configuration for the penguin delta checkpointer.

mod table_key;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub use blizzard_common::config::{
    ConfigPath, ErrorHandlingConfig, InterpolationResult, Mergeable, MetricsConfig,
    ParquetCompression, Resource, interpolate, load_from_paths,
};
pub use blizzard_common::{GlobalConfig, KB, MB};
pub use table_key::TableKey;

use crate::schema::SchemaEvolutionMode;
use blizzard_common::error::ConfigError;

fn default_poll_interval() -> u64 {
    10
}

/// Configuration for a partition filter used during cold start.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionFilterConfig {
    /// strftime-style prefix template (e.g., "date=%Y-%m-%d").
    pub prefix_template: String,
    /// Number of units to look back (days or hours depending on template).
    #[serde(default)]
    pub lookback: u32,
}

/// Configuration for a Delta table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    /// URI of the Delta Lake table.
    pub table_uri: String,
    /// Poll interval in seconds for checking new files.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Columns to partition output by.
    #[serde(default)]
    pub partition_by: Vec<String>,
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

fn default_max_concurrent_uploads() -> usize {
    4
}

fn default_max_concurrent_parts() -> usize {
    8
}

fn default_part_size_mb() -> usize {
    10
}

fn default_min_multipart_size_mb() -> usize {
    100
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

impl Default for Config {
    fn default() -> Self {
        Self {
            tables: IndexMap::new(),
            global: GlobalConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

impl Mergeable for Config {
    type Key = TableKey;
    type Component = TableConfig;

    fn components_mut(&mut self) -> &mut IndexMap<Self::Key, Self::Component> {
        &mut self.tables
    }

    fn global_mut(&mut self) -> &mut GlobalConfig {
        &mut self.global
    }

    fn metrics_mut(&mut self) -> &mut MetricsConfig {
        &mut self.metrics
    }

    fn parse_yaml(contents: &str) -> Result<Self, ConfigError> {
        serde_yaml::from_str(contents).map_err(|source| ConfigError::YamlParse { source })
    }
}

impl Config {
    /// Load configuration from multiple paths (files or directories).
    pub fn from_paths(paths: &[ConfigPath]) -> Result<Self, ConfigError> {
        let config: Self = load_from_paths(paths)?;
        config.validate()?;
        Ok(config)
    }

    /// Load configuration from a file.
    pub fn from_file(path: &str) -> Result<Self, ConfigError> {
        let contents =
            std::fs::read_to_string(path).map_err(|source| ConfigError::ReadFile { source })?;
        Self::parse(&contents)
    }

    /// Parse configuration from a YAML string.
    pub fn parse(contents: &str) -> Result<Self, ConfigError> {
        // Interpolate environment variables
        let result = interpolate(contents);
        if !result.is_ok() {
            return Err(ConfigError::EnvInterpolation {
                message: result.errors.join("\n"),
            });
        }

        // Parse YAML
        let config: Config = serde_yaml::from_str(&result.text)
            .map_err(|source| ConfigError::YamlParse { source })?;

        // Validate
        config.validate()?;

        Ok(config)
    }

    /// Validate the configuration.
    ///
    /// Checks:
    /// - All tables have non-empty table_uri
    /// - No resource conflicts (e.g., two tables using the same Delta table)
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Check for empty table_uri
        for (key, table) in &self.tables {
            if table.table_uri.is_empty() {
                return Err(ConfigError::EmptyTableUriForTable {
                    table: key.id().to_string(),
                });
            }
        }

        // Check for resource conflicts
        let conflicts = Resource::conflicts(
            self.tables
                .iter()
                .map(|(key, config)| (key.id().to_string(), config.resources())),
        );

        if !conflicts.is_empty() {
            let message = conflicts
                .iter()
                .map(|(resource, keys)| {
                    let keys_list: Vec<_> = keys.iter().collect();
                    format!("{} claimed by: {:?}", resource, keys_list)
                })
                .collect::<Vec<_>>()
                .join("; ");
            return Err(ConfigError::ResourceConflict { message });
        }

        Ok(())
    }

    /// Iterate over all tables with their keys.
    pub fn tables(&self) -> impl Iterator<Item = (&TableKey, &TableConfig)> {
        self.tables.iter()
    }

    /// Get the number of tables in the configuration.
    pub fn table_count(&self) -> usize {
        self.tables.len()
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
        assert!(err.to_string().contains("empty table_uri"));
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
        assert!(table.partition_by.is_empty());
        assert!(table.storage_options.is_empty());
        assert!(table.partition_filter.is_none());
    }

    #[test]
    fn test_table_config_resources() {
        let table = TableConfig {
            table_uri: "gs://bucket/my_table".to_string(),
            poll_interval_secs: 10,
            partition_by: vec![],
            delta_checkpoint_interval: 10,
            max_concurrent_uploads: 4,
            max_concurrent_parts: 8,
            part_size_mb: 10,
            min_multipart_size_mb: 100,
            storage_options: HashMap::new(),
            schema_evolution: Default::default(),
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
            "Expected resource conflict error, got: {}",
            msg
        );
        assert!(
            msg.contains("gs://bucket/delta/same"),
            "Expected table URI in error, got: {}",
            msg
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
}
