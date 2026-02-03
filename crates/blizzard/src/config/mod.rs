//! Configuration for the blizzard file loader.

mod pipeline_key;

use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub use blizzard_core::config::{
    ConfigPath, ErrorHandlingConfig, InterpolationResult, Mergeable, MetricsConfig,
    ParquetCompression, Resource, interpolate, load_from_paths,
};
use blizzard_core::storage::DatePrefixGenerator;
pub use blizzard_core::{GlobalConfig, KB, MB};
pub use pipeline_key::PipelineKey;

use blizzard_core::error::ConfigError;

/// Trait for config types that provide storage connection details.
pub trait StorageSource {
    /// The URL/path for this storage location.
    fn url(&self) -> &str;
    /// Storage options (credentials, region, etc.).
    fn storage_options(&self) -> &HashMap<String, String>;
}

/// Configuration for a partition filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionFilterConfig {
    /// strftime-style prefix template (e.g., "date=%Y-%m-%d/hour=%H").
    pub prefix_template: String,
    /// Number of units to look back (days or hours depending on template).
    #[serde(default)]
    pub lookback: u32,
}

/// Configuration for partitioning output files.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionByConfig {
    /// strftime-style prefix template (e.g., "date=%Y-%m-%d/hour=%H").
    pub prefix_template: String,
}

impl PartitionByConfig {
    /// Extract partition column names from the template.
    /// e.g., "date=%Y-%m-%d/hour=%H" -> ["date", "hour"]
    pub fn partition_columns(&self) -> Vec<String> {
        self.prefix_template
            .split('/')
            .filter_map(|segment| segment.find('=').map(|idx| segment[..idx].to_string()))
            .collect()
    }
}

/// Configuration for the input source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Enable watermark-based source tracking.
    ///
    /// When true, uses a persistent high-watermark checkpoint to track processed files.
    /// This replaces the unbounded in-memory HashMap with efficient lexicographic filtering.
    ///
    /// Requirements:
    /// - Source files must be lexicographically sortable (e.g., timestamp prefixes, UUIDv7)
    /// - Checkpoint is stored at `{table_uri}/_blizzard/{pipeline}_checkpoint.json`
    ///
    /// First run performs a full scan; subsequent runs only list files above the watermark.
    #[serde(default)]
    pub use_watermark: bool,
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

impl StorageSource for SourceConfig {
    fn url(&self) -> &str {
        &self.path
    }
    fn storage_options(&self) -> &HashMap<String, String> {
        &self.storage_options
    }
}

impl SourceConfig {
    /// Generate date prefixes for partition filtering.
    ///
    /// Returns `None` if no partition filter is configured.
    pub fn date_prefixes(&self) -> Option<Vec<String>> {
        self.partition_filter.as_ref().map(|pf| {
            DatePrefixGenerator::new(&pf.prefix_template, pf.lookback).generate_prefixes()
        })
    }
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
#[serde(deny_unknown_fields)]
pub struct SinkConfig {
    /// URI of the Delta table (supports S3, GCS, Azure, local).
    /// Parquet files are written directly to {table_uri}/{partition}/
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
    /// Partition configuration with strftime-style prefix template.
    pub partition_by: Option<PartitionByConfig>,
    /// Storage options for table storage (credentials, region, etc.).
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
}

impl StorageSource for SinkConfig {
    fn url(&self) -> &str {
        &self.table_uri
    }
    fn storage_options(&self) -> &HashMap<String, String> {
        &self.storage_options
    }
}

fn default_file_size_mb() -> usize {
    128
}

fn default_row_group_size() -> usize {
    128 * MB
}

/// Schema field configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Schema configuration - either explicit fields or inference mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaConfig {
    /// When true, schema is inferred from the first NDJSON file.
    #[serde(default)]
    pub infer: bool,
    /// List of fields in the schema. Required unless `infer: true`.
    #[serde(default)]
    pub fields: Vec<FieldConfig>,
}

impl SchemaConfig {
    /// Returns true if schema should be inferred from source data.
    pub fn should_infer(&self) -> bool {
        self.infer
    }

    /// Returns the explicit fields if defined.
    pub fn fields(&self) -> &[FieldConfig] {
        &self.fields
    }

    /// Convert to Arrow Schema. Panics if schema is set to infer mode.
    pub fn to_arrow_schema(&self) -> SchemaRef {
        if self.infer {
            panic!("Cannot convert infer schema config to Arrow schema")
        }
        let arrow_fields: Vec<Field> = self
            .fields
            .iter()
            .map(|f| Field::new(&f.name, f.field_type.to_arrow_type(), f.nullable))
            .collect();
        Arc::new(Schema::new(arrow_fields))
    }
}

/// Configuration for a single pipeline (source → sink with schema).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    /// Source configuration.
    pub source: SourceConfig,
    /// Sink configuration.
    pub sink: SinkConfig,
    /// Schema configuration - either explicit fields or `infer: true`.
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
    /// conflicts like two pipelines reading from the same source directory
    /// or writing to the same table.
    pub fn resources(&self) -> Vec<Resource> {
        let source_dir = self.source.path.trim_end_matches('/');
        let table_dir = self.sink.table_uri.trim_end_matches('/');
        vec![
            Resource::directory(source_dir),
            Resource::directory(table_dir),
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
#[serde(deny_unknown_fields)]
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

    fn components(&self) -> &IndexMap<Self::Key, Self::Component> {
        &self.pipelines
    }

    fn components_mut(&mut self) -> &mut IndexMap<Self::Key, Self::Component> {
        &mut self.pipelines
    }

    fn global(&self) -> &GlobalConfig {
        &self.global
    }

    fn global_mut(&mut self) -> &mut GlobalConfig {
        &mut self.global
    }

    fn metrics(&self) -> &MetricsConfig {
        &self.metrics
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
    /// - All pipelines have non-empty source path and table_uri
    /// - All pipelines have either explicit schema fields or `schema: { infer: true }`
    /// - No resource conflicts (e.g., two pipelines using the same source directory)
    ///
    /// Collects all validation errors and returns them together, rather than
    /// stopping at the first error.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut errors = Vec::new();

        // Check for empty paths and schema
        for (key, pipeline) in &self.pipelines {
            if pipeline.source.path.is_empty() {
                errors.push(format!("Pipeline '{}': source.path is empty", key.id()));
            }
            if pipeline.sink.table_uri.is_empty() {
                errors.push(format!("Pipeline '{}': sink.table_uri is empty", key.id()));
            }

            // Validate schema configuration
            let has_fields = !pipeline.schema.fields.is_empty();
            let wants_infer = pipeline.schema.infer;

            if wants_infer && has_fields {
                errors.push(format!(
                    "Pipeline '{}': cannot specify both 'infer: true' and 'fields'",
                    key.id()
                ));
            }
            if !wants_infer && !has_fields {
                errors.push(format!(
                    "Pipeline '{}': empty schema (specify either 'infer: true' or 'fields')",
                    key.id()
                ));
            }
        }

        // Check for resource conflicts
        let conflicts = Resource::conflicts(
            self.pipelines
                .iter()
                .map(|(key, config)| (key.id().to_string(), config.resources())),
        );

        for (resource, keys) in conflicts {
            let keys_list: Vec<_> = keys.iter().collect();
            errors.push(format!(
                "Resource conflict: {} claimed by {:?}",
                resource, keys_list
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else if errors.len() == 1 {
            // For a single error, return a more specific error type if possible
            Err(ConfigError::MultipleErrors { errors })
        } else {
            Err(ConfigError::MultipleErrors { errors })
        }
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
        assert!(err.to_string().contains("source.path is empty"));
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
        assert!(err.to_string().contains("sink.table_uri is empty"));
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
        assert!(pipeline.sink.partition_by.is_none());
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
            Resource::directory("gs://bucket/delta/events")
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
            Resource::directory("gs://bucket/delta/events")
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
            msg.contains("gs://bucket/delta/same"),
            "Expected table dir in error, got: {}",
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

    #[test]
    fn test_infer_schema_valid() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      infer: true
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();
        assert!(pipeline.schema.should_infer());
    }

    #[test]
    fn test_infer_schema_false_error() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      infer: false
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("events"));
        assert!(err.to_string().contains("empty schema"));
    }

    #[test]
    fn test_schema_both_infer_and_fields_error() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      infer: true
      fields:
        - name: id
          type: string
"#;
        let result = Config::parse(yaml);
        assert!(
            result.is_err(),
            "Should error when both infer and fields are specified"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("cannot specify both"),
            "Error should mention the conflict: {}",
            err
        );
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
    fn test_partition_by_config_single_column() {
        let config = PartitionByConfig {
            prefix_template: "date=%Y-%m-%d".to_string(),
        };
        let columns = config.partition_columns();
        assert_eq!(columns, vec!["date"]);
    }

    #[test]
    fn test_partition_by_config_empty_template() {
        let config = PartitionByConfig {
            prefix_template: "".to_string(),
        };
        let columns = config.partition_columns();
        assert!(columns.is_empty());
    }

    #[test]
    fn test_partition_by_config_yaml_parsing() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
      partition_by:
        prefix_template: "date=%Y-%m-%d"
    schema:
      fields:
        - name: id
          type: string
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();

        let partition_by = pipeline.sink.partition_by.as_ref().unwrap();
        assert_eq!(partition_by.prefix_template, "date=%Y-%m-%d");
        assert_eq!(partition_by.partition_columns(), vec!["date"]);
    }

    #[test]
    fn test_unknown_field_rejected_in_source() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
      batchsize: 100
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      infer: true
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err(), "Should reject unknown field 'batchsize'");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field"),
            "Error should mention unknown field: {}",
            err
        );
    }

    #[test]
    fn test_unknown_field_rejected_in_sink() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
      filesize: 128
    schema:
      infer: true
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err(), "Should reject unknown field 'filesize'");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field"),
            "Error should mention unknown field: {}",
            err
        );
    }

    #[test]
    fn test_unknown_field_rejected_in_pipeline() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      infer: true
    unknown_key: value
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err(), "Should reject unknown field 'unknown_key'");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field"),
            "Error should mention unknown field: {}",
            err
        );
    }

    #[test]
    fn test_unknown_field_rejected_at_top_level() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      infer: true
unknown_top_level: value
"#;
        let result = Config::parse(yaml);
        assert!(
            result.is_err(),
            "Should reject unknown field 'unknown_top_level'"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field"),
            "Error should mention unknown field: {}",
            err
        );
    }

    #[test]
    fn test_multiple_errors_collected() {
        let yaml = r#"
pipelines:
  a:
    source:
      path: ""
    sink:
      table_uri: ""
    schema:
      infer: true
  b:
    source:
      path: ""
    sink:
      table_uri: gs://bucket/b
    schema:
      infer: true
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err(), "Should have validation errors");
        let err = result.unwrap_err().to_string();
        // Should contain errors for pipeline 'a' (empty path and empty table_uri)
        // and pipeline 'b' (empty path)
        assert!(
            err.contains("Pipeline 'a'"),
            "Should mention pipeline 'a': {}",
            err
        );
        assert!(
            err.contains("Pipeline 'b'"),
            "Should mention pipeline 'b': {}",
            err
        );
        assert!(
            err.contains("source.path is empty"),
            "Should mention empty source path: {}",
            err
        );
    }

    #[test]
    fn test_multiple_schema_errors_collected() {
        let yaml = r#"
pipelines:
  a:
    source:
      path: gs://bucket/a
    sink:
      table_uri: gs://bucket/delta/a
    schema:
      infer: true
      fields:
        - name: id
          type: string
  b:
    source:
      path: gs://bucket/b
    sink:
      table_uri: gs://bucket/delta/b
    schema:
      infer: false
"#;
        let result = Config::parse(yaml);
        assert!(result.is_err(), "Should have validation errors");
        let err = result.unwrap_err().to_string();
        // Pipeline 'a' has both infer and fields
        // Pipeline 'b' has empty schema
        assert!(
            err.contains("Pipeline 'a'") && err.contains("cannot specify both"),
            "Should mention pipeline 'a' schema conflict: {}",
            err
        );
        assert!(
            err.contains("Pipeline 'b'") && err.contains("empty schema"),
            "Should mention pipeline 'b' empty schema: {}",
            err
        );
    }

    #[test]
    fn test_use_watermark_default_false() {
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

        assert!(
            !pipeline.source.use_watermark,
            "use_watermark should default to false"
        );
    }

    #[test]
    fn test_use_watermark_enabled() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: gs://bucket/raw
      use_watermark: true
      partition_filter:
        prefix_template: "date=%Y-%m-%d"
        lookback: 2
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields:
        - name: id
          type: string
"#;
        let config = Config::parse(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();

        assert!(
            pipeline.source.use_watermark,
            "use_watermark should be true"
        );
        assert!(pipeline.source.partition_filter.is_some());
    }
}
