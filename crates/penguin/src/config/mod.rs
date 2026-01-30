//! Configuration for the penguin delta checkpointer.

mod table_key;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub use blizzard_common::config::{
    ErrorHandlingConfig, InterpolationResult, MetricsConfig, ParquetCompression, Resource,
    interpolate,
};
pub use blizzard_common::{GlobalConfig, KB, MB};
pub use table_key::TableKey;

use crate::error::ConfigError;
use crate::schema::SchemaEvolutionMode;

fn default_poll_interval() -> u64 {
    10
}

/// Configuration for a Delta table.
///
/// The staging directory is derived from the table URI: `{table_uri}/_staging/`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    /// URI of the Delta Lake table.
    /// Staging metadata is read from `{table_uri}/_staging/`
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
}

impl TableConfig {
    /// Returns exclusive resources used by this table configuration.
    ///
    /// Resources are things that cannot be shared between tables running in
    /// the same process. This is used during config validation to detect
    /// conflicts like two tables pointing to the same staging directory.
    pub fn resources(&self) -> Vec<Resource> {
        // The staging directory is derived from table_uri: {table_uri}/_staging/
        let staging_dir = format!("{}/_staging", self.table_uri.trim_end_matches('/'));
        vec![Resource::directory(&staging_dir)]
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
    pub tables: IndexMap<TableKey, TableConfig>,
    /// Global configuration options.
    #[serde(default)]
    pub global: GlobalConfig,
    /// Metrics configuration.
    #[serde(default)]
    pub metrics: MetricsConfig,
}

impl Config {
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
    /// - No resource conflicts (e.g., two tables using the same staging directory)
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
        assert!(config.metrics.enabled);
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
        };

        let resources = table.resources();
        assert_eq!(resources.len(), 1);
        assert_eq!(
            resources[0],
            Resource::directory("gs://bucket/my_table/_staging")
        );
    }

    #[test]
    fn test_table_config_resources_trailing_slash() {
        let table = TableConfig {
            table_uri: "gs://bucket/my_table/".to_string(),
            poll_interval_secs: 10,
            partition_by: vec![],
            delta_checkpoint_interval: 10,
            max_concurrent_uploads: 4,
            max_concurrent_parts: 8,
            part_size_mb: 10,
            min_multipart_size_mb: 100,
            storage_options: HashMap::new(),
            schema_evolution: Default::default(),
        };

        let resources = table.resources();
        assert_eq!(
            resources[0],
            Resource::directory("gs://bucket/my_table/_staging")
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
            "Expected resource conflict error, got: {}",
            msg
        );
        assert!(
            msg.contains("gs://bucket/delta/same/_staging"),
            "Expected staging dir in error, got: {}",
            msg
        );
    }

    #[test]
    fn test_resource_conflict_trailing_slash_normalization() {
        // Same URI with/without trailing slash should conflict
        let yaml = r#"
tables:
  a:
    table_uri: gs://bucket/delta/same
  b:
    table_uri: gs://bucket/delta/same/
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Resource conflict"));
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
