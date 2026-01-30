//! Configuration for the blizzard file loader.

mod pipeline_key;

use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub use blizzard_common::config::{
    ConfigPath, ErrorHandlingConfig, InterpolationResult, Mergeable, MetricsConfig,
    ParquetCompression, Resource, interpolate, load_from_paths,
};
pub use blizzard_common::{GlobalConfig, KB, MB};
pub use pipeline_key::PipelineKey;

use blizzard_common::error::ConfigError;

/// Configuration for a partition filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionFilterConfig {
    /// strftime-style prefix template (e.g., "date=%Y-%m-%d/hour=%H").
    pub prefix_template: String,
    /// Number of units to look back (days or hours depending on template).
    #[serde(default)]
    pub lookback: u32,
}

/// Configuration for the input source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Path to the input directory (supports S3, GCS, Azure, local).
    pub path: String,
    /// Compression format of input files.
    #[serde(default)]
    pub compression: CompressionFormat,
    /// Number of records per batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum concurrent file downloads.
    #[serde(default = "default_max_concurrent_files")]
    pub max_concurrent_files: usize,
    /// Poll interval in seconds.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Optional partition filter for efficient listing.
    pub partition_filter: Option<PartitionFilterConfig>,
    /// Storage options for source storage (credentials, region, etc.)
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

fn default_batch_size() -> usize {
    8192
}

fn default_max_concurrent_files() -> usize {
    4
}

fn default_poll_interval() -> u64 {
    60
}

/// Compression format of input files.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionFormat {
    #[default]
    Gzip,
    Zstd,
    None,
}

/// Configuration for the sink Delta table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    /// URI of the Delta table (supports S3, GCS, Azure, local).
    /// Parquet files are written directly to {table_uri}/{partition}/
    /// Coordination metadata is written to {table_uri}/_staging/pending/
    pub table_uri: String,
    /// Target Parquet file size in MB.
    #[serde(default = "default_file_size_mb")]
    pub file_size_mb: usize,
    /// Row group size in bytes for memory management.
    #[serde(default = "default_row_group_size")]
    pub row_group_size_bytes: usize,
    /// Parquet compression codec.
    #[serde(default)]
    pub compression: ParquetCompression,
    /// Columns to partition output by.
    #[serde(default)]
    pub partition_by: Vec<String>,
    /// Storage options for table storage (credentials, region, etc.).
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

fn default_file_size_mb() -> usize {
    128
}

fn default_row_group_size() -> usize {
    128 * MB
}

/// Schema field configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConfig {
    /// Field name.
    pub name: String,
    /// Field type.
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Whether the field is nullable.
    #[serde(default = "default_nullable")]
    pub nullable: bool,
}

fn default_nullable() -> bool {
    true
}

/// Supported field types.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    Timestamp,
    Date,
    Json,
    Binary,
}

impl FieldType {
    /// Convert to Arrow DataType.
    pub fn to_arrow_type(self) -> DataType {
        match self {
            FieldType::String => DataType::Utf8,
            FieldType::Int32 => DataType::Int32,
            FieldType::Int64 => DataType::Int64,
            FieldType::Float32 => DataType::Float32,
            FieldType::Float64 => DataType::Float64,
            FieldType::Boolean => DataType::Boolean,
            FieldType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            FieldType::Date => DataType::Date32,
            FieldType::Json => DataType::Utf8, // JSON stored as string
            FieldType::Binary => DataType::Binary,
        }
    }
}

/// Schema configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaConfig {
    /// List of fields in the schema.
    pub fields: Vec<FieldConfig>,
}

impl SchemaConfig {
    /// Convert to Arrow Schema.
    pub fn to_arrow_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .fields
            .iter()
            .map(|f| Field::new(&f.name, f.field_type.to_arrow_type(), f.nullable))
            .collect();
        Arc::new(Schema::new(fields))
    }
}

/// Configuration for a single pipeline (source → sink with schema).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Source configuration.
    pub source: SourceConfig,
    /// Sink configuration.
    pub sink: SinkConfig,
    /// Schema configuration.
    pub schema: SchemaConfig,
    /// Error handling configuration.
    #[serde(default)]
    pub error_handling: ErrorHandlingConfig,
}

impl PipelineConfig {
    /// Returns exclusive resources used by this pipeline configuration.
    ///
    /// Resources are things that cannot be shared between pipelines running in
    /// the same process. This is used during config validation to detect
    /// conflicts like two pipelines reading from the same source directory.
    pub fn resources(&self) -> Vec<Resource> {
        let source_dir = self.source.path.trim_end_matches('/');
        let staging_dir = format!("{}/_staging", self.sink.table_uri.trim_end_matches('/'));
        vec![
            Resource::directory(source_dir),
            Resource::directory(&staging_dir),
        ]
    }
}

/// Main configuration for blizzard.
///
/// # Example
///
/// ```yaml
/// pipelines:
///   orderbooks:
///     source:
///       path: "gs://bucket/orderbooks-raw"
///       compression: gzip
///     sink:
///       table_uri: "gs://bucket/delta/orderbooks"
///     schema:
///       fields:
///         - name: id
///           type: string
///
///   trades:
///     source:
///       path: "gs://bucket/trades-raw"
///     sink:
///       table_uri: "gs://bucket/delta/trades"
///     schema:
///       fields:
///         - name: trade_id
///           type: string
///
/// global:
///   total_concurrency: 8
///
/// metrics:
///   enabled: true
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Named pipeline configurations.
    #[serde(default)]
    pub pipelines: IndexMap<PipelineKey, PipelineConfig>,
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
            pipelines: IndexMap::new(),
            global: GlobalConfig::default(),
            metrics: MetricsConfig::default(),
        }
    }
}

impl Mergeable for Config {
    type Key = PipelineKey;
    type Component = PipelineConfig;

    fn components_mut(&mut self) -> &mut IndexMap<Self::Key, Self::Component> {
        &mut self.pipelines
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
    /// - All pipelines have non-empty source path, table_uri, and schema
    /// - No resource conflicts (e.g., two pipelines using the same source directory)
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Check for empty paths and schema
        for (key, pipeline) in &self.pipelines {
            if pipeline.source.path.is_empty() {
                return Err(ConfigError::EmptySourcePathForPipeline {
                    pipeline: key.id().to_string(),
                });
            }
            if pipeline.sink.table_uri.is_empty() {
                return Err(ConfigError::EmptyTableUriForPipeline {
                    pipeline: key.id().to_string(),
                });
            }
            if pipeline.schema.fields.is_empty() {
                return Err(ConfigError::EmptySchemaForPipeline {
                    pipeline: key.id().to_string(),
                });
            }
        }

        // Check for resource conflicts
        let conflicts = Resource::conflicts(
            self.pipelines
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

    /// Iterate over all pipelines with their keys.
    pub fn pipelines(&self) -> impl Iterator<Item = (&PipelineKey, &PipelineConfig)> {
        self.pipelines.iter()
    }

    /// Get the number of pipelines in the configuration.
    pub fn pipeline_count(&self) -> usize {
        self.pipelines.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_pipeline_parse() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw-data
      compression: gzip
    sink:
      table_uri: gs://bucket/delta/events
      file_size_mb: 128
    schema:
      fields:
        - name: id
          type: string
"#;
        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.pipeline_count(), 1);

        let (key, pipeline) = config.pipelines().next().unwrap();
        assert_eq!(key.id(), "events");
        assert_eq!(pipeline.source.path, "gs://bucket/raw-data");
        assert_eq!(pipeline.sink.table_uri, "gs://bucket/delta/events");
    }

    #[test]
    fn test_multi_pipeline_parse() {
        let yaml = r#"
pipelines:
  orderbooks:
    source:
      path: gs://bucket/orderbooks-raw
      compression: gzip
    sink:
      table_uri: gs://bucket/delta/orderbooks
    schema:
      fields:
        - name: id
          type: string

  trades:
    source:
      path: gs://bucket/trades-raw
    sink:
      table_uri: gs://bucket/delta/trades
    schema:
      fields:
        - name: trade_id
          type: string

global:
  total_concurrency: 8
"#;
        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.pipeline_count(), 2);
        assert_eq!(config.global.total_concurrency, Some(8));

        let pipelines: Vec<_> = config.pipelines().collect();
        assert_eq!(pipelines.len(), 2);

        // IndexMap preserves insertion order
        assert_eq!(pipelines[0].0.id(), "orderbooks");
        assert_eq!(pipelines[0].1.source.path, "gs://bucket/orderbooks-raw");
        assert_eq!(pipelines[1].0.id(), "trades");
        assert_eq!(pipelines[1].1.source.path, "gs://bucket/trades-raw");
    }

    #[test]
    fn test_empty_source_error() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: ""
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields:
        - name: id
          type: string
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("events"));
        assert!(err.to_string().contains("empty source path"));
    }

    #[test]
    fn test_empty_sink_error() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: ""
    schema:
      fields:
        - name: id
          type: string
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("events"));
        assert!(err.to_string().contains("empty table_uri"));
    }

    #[test]
    fn test_empty_schema_error() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields: []
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("events"));
        assert!(err.to_string().contains("empty schema"));
    }

    #[test]
    fn test_metrics_default() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields:
        - name: id
          type: string
"#;
        let config = Config::parse(yaml).unwrap();
        assert!(config.metrics.enabled);
        assert_eq!(config.metrics.address, "0.0.0.0:9090");
    }

    #[test]
    fn test_global_default() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields:
        - name: id
          type: string
"#;
        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.global.total_concurrency, None);
    }

    #[test]
    fn test_source_config_defaults() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields:
        - name: id
          type: string
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();

        assert_eq!(pipeline.source.batch_size, 8192);
        assert_eq!(pipeline.source.max_concurrent_files, 4);
        assert_eq!(pipeline.source.poll_interval_secs, 60);
        assert!(pipeline.source.partition_filter.is_none());
    }

    #[test]
    fn test_sink_config_defaults() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields:
        - name: id
          type: string
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();

        assert_eq!(pipeline.sink.file_size_mb, 128);
        assert_eq!(pipeline.sink.row_group_size_bytes, 128 * MB);
        assert!(pipeline.sink.partition_by.is_empty());
    }

    #[test]
    fn test_pipeline_config_resources() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw-events
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields:
        - name: id
          type: string
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();

        let resources = pipeline.resources();
        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0], Resource::directory("gs://bucket/raw-events"));
        assert_eq!(
            resources[1],
            Resource::directory("gs://bucket/delta/events/_staging")
        );
    }

    #[test]
    fn test_pipeline_config_resources_trailing_slash() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw-events/
    sink:
      table_uri: gs://bucket/delta/events/
    schema:
      fields:
        - name: id
          type: string
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();

        let resources = pipeline.resources();
        assert_eq!(resources[0], Resource::directory("gs://bucket/raw-events"));
        assert_eq!(
            resources[1],
            Resource::directory("gs://bucket/delta/events/_staging")
        );
    }

    #[test]
    fn test_resource_conflict_same_source() {
        let yaml = r#"
pipelines:
  a:
    source:
      path: gs://bucket/raw/same
    sink:
      table_uri: gs://bucket/delta/a
    schema:
      fields:
        - name: id
          type: string
  b:
    source:
      path: gs://bucket/raw/same
    sink:
      table_uri: gs://bucket/delta/b
    schema:
      fields:
        - name: id
          type: string
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
            msg.contains("gs://bucket/raw/same"),
            "Expected source dir in error, got: {}",
            msg
        );
    }

    #[test]
    fn test_resource_conflict_same_sink() {
        let yaml = r#"
pipelines:
  a:
    source:
      path: gs://bucket/raw/a
    sink:
      table_uri: gs://bucket/delta/same
    schema:
      fields:
        - name: id
          type: string
  b:
    source:
      path: gs://bucket/raw/b
    sink:
      table_uri: gs://bucket/delta/same
    schema:
      fields:
        - name: id
          type: string
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
        // Same source with/without trailing slash should conflict
        let yaml = r#"
pipelines:
  a:
    source:
      path: gs://bucket/raw/same
    sink:
      table_uri: gs://bucket/delta/a
    schema:
      fields:
        - name: id
          type: string
  b:
    source:
      path: gs://bucket/raw/same/
    sink:
      table_uri: gs://bucket/delta/b
    schema:
      fields:
        - name: id
          type: string
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Resource conflict"));
    }

    #[test]
    fn test_no_resource_conflict_different_paths() {
        let yaml = r#"
pipelines:
  a:
    source:
      path: gs://bucket/raw/a
    sink:
      table_uri: gs://bucket/delta/a
    schema:
      fields:
        - name: id
          type: string
  b:
    source:
      path: gs://bucket/raw/b
    sink:
      table_uri: gs://bucket/delta/b
    schema:
      fields:
        - name: id
          type: string
"#;
        let result = Config::parse(yaml);
        assert!(result.is_ok());
    }
}
