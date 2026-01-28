//! Configuration parsing and schema management.
//!
//! Handles loading configuration from YAML files and command-line arguments,
//! and converts user-defined schemas to Arrow schemas.

mod vars;
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::sink::parquet::RollingPolicy;

use crate::error::{
    ConfigError, EmptySchemaSnafu, EmptySinkPathSnafu, EmptySourcePathSnafu, EnvInterpolationSnafu,
    ReadFileSnafu, YamlParseSnafu,
};

/// Byte size constants (binary/IEC units).
pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;

/// Main configuration structure for the pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub schema: SchemaConfig,
    /// Metrics configuration (optional, enabled by default).
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Error handling configuration (optional).
    #[serde(default)]
    pub error_handling: ErrorHandlingConfig,
}

/// Error handling configuration for resilient pipeline execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ErrorHandlingConfig {
    /// Maximum failures before stopping pipeline (0 = unlimited, default: 0).
    #[serde(default)]
    pub max_failures: usize,
    /// Path to write failed file records (required for DLQ).
    #[serde(default)]
    pub dlq_path: Option<String>,
    /// Storage options for DLQ (credentials, region, etc.)
    #[serde(default)]
    pub dlq_storage_options: HashMap<String, String>,
}

/// Metrics configuration for Prometheus endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Whether metrics collection is enabled (default: true).
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,
    /// Address to bind the metrics HTTP server (default: "0.0.0.0:9090").
    #[serde(default = "default_metrics_address")]
    pub address: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            address: default_metrics_address(),
        }
    }
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_address() -> String {
    "0.0.0.0:9090".to_string()
}

/// Source configuration for reading NDJSON files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Path pattern for input files (supports glob patterns).
    /// Examples: "s3://bucket/input/*.ndjson.gz", "/local/path/**/*.ndjson.gz"
    pub path: String,

    /// Compression format of input files.
    #[serde(default)]
    pub compression: CompressionFormat,

    /// Storage options (credentials, region, etc.)
    #[serde(default)]
    pub storage_options: HashMap<String, String>,

    /// Batch size for reading records (default: 8192)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Maximum number of files to process concurrently (default: 4)
    #[serde(default = "default_max_concurrent_files")]
    pub max_concurrent_files: usize,

    /// Interval in seconds between polls for new files (default: 60).
    /// The pipeline continuously checks for new files at this interval.
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,

    /// Optional partition filter to limit file listing to recent date prefixes.
    /// When configured, only lists files under matching date partitions instead
    /// of scanning the entire bucket.
    #[serde(default)]
    pub partition_filter: Option<PartitionFilterConfig>,
}

/// Configuration for date-based partition filtering.
///
/// Limits file listing to recent date prefixes, dramatically reducing
/// the number of files that need to be listed in large buckets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionFilterConfig {
    /// Template using strftime codes: %Y (year), %m (month), %d (day), %H (hour).
    /// Example: "date=%Y-%m-%d" or "date=%Y-%m-%d/hour=%H"
    pub prefix_template: String,

    /// Number of time units to look back (default: 1).
    ///
    /// The unit depends on the template:
    /// - If template contains `%H`: lookback is in hours
    /// - Otherwise: lookback is in days
    ///
    /// With lookback=1 and a day template, lists today + 1 day back (2 total).
    /// With lookback=1 and an hour template, lists current hour + 1 hour back (2 total).
    #[serde(default = "default_lookback")]
    pub lookback: u32,
}

fn default_lookback() -> u32 {
    1
}

fn default_batch_size() -> usize {
    8192
}

fn default_max_concurrent_files() -> usize {
    4
}

fn default_poll_interval_secs() -> u64 {
    60
}

/// Sink configuration for writing to Delta Lake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    /// Path to the Delta Lake table.
    /// Examples: "s3://bucket/output/my-table", "/local/path/my-table"
    pub path: String,

    /// Target file size in MB (default: 128)
    #[serde(default = "default_file_size_mb")]
    pub file_size_mb: usize,

    /// Target row group size in bytes (default: 128MB)
    /// Row groups are flushed when in_progress_size exceeds this threshold
    #[serde(default = "default_row_group_size_bytes")]
    pub row_group_size_bytes: usize,

    /// Roll file after this many seconds of inactivity (optional)
    #[serde(default)]
    pub inactivity_timeout_secs: Option<u64>,

    /// Roll file after it's been open this long (optional)
    #[serde(default)]
    pub rollover_timeout_secs: Option<u64>,

    /// Target size per multipart part in MB (default: 32)
    #[serde(default = "default_part_size_mb")]
    pub part_size_mb: usize,

    /// Minimum file size in MB before using multipart upload (default: 5)
    /// Files smaller than this use single PUT
    #[serde(default = "default_min_multipart_size_mb")]
    pub min_multipart_size_mb: usize,

    /// Maximum concurrent file uploads (default: 4)
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,

    /// Maximum concurrent parts per multipart upload (default: 8)
    #[serde(default = "default_max_concurrent_parts")]
    pub max_concurrent_parts: usize,

    /// Storage options (credentials, region, etc.)
    #[serde(default)]
    pub storage_options: HashMap<String, String>,

    /// Parquet compression codec.
    #[serde(default)]
    pub compression: ParquetCompression,
}

fn default_file_size_mb() -> usize {
    128
}

fn default_row_group_size_bytes() -> usize {
    128 * MB
}

fn default_part_size_mb() -> usize {
    32
}

fn default_min_multipart_size_mb() -> usize {
    5
}

fn default_max_concurrent_uploads() -> usize {
    4
}

fn default_max_concurrent_parts() -> usize {
    8
}

impl SinkConfig {
    /// Build rolling policies from this sink configuration.
    pub fn rolling_policies(&self) -> Vec<RollingPolicy> {
        let mut policies = Vec::new();

        // Always include size-based rolling
        let size_bytes = self.file_size_mb * MB;
        policies.push(RollingPolicy::SizeLimit(size_bytes));

        // Add inactivity timeout if configured
        if let Some(secs) = self.inactivity_timeout_secs {
            policies.push(RollingPolicy::InactivityDuration(Duration::from_secs(secs)));
        }

        // Add rollover timeout if configured
        if let Some(secs) = self.rollover_timeout_secs {
            policies.push(RollingPolicy::RolloverDuration(Duration::from_secs(secs)));
        }

        policies
    }
}

/// Schema configuration defining the structure of input data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaConfig {
    pub fields: Vec<FieldConfig>,
}

/// Configuration for a single schema field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldConfig {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: FieldType,
    #[serde(default)]
    pub nullable: bool,
}

impl Default for FieldConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            field_type: FieldType::String,
            nullable: true,
        }
    }
}

/// Supported field types for the schema.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    #[default]
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    Timestamp,
    Date,
    /// Raw JSON stored as a string
    Json,
    /// Binary data (base64 encoded in JSON)
    Binary,
}

/// Compression format for source files.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionFormat {
    None,
    #[default]
    Gzip,
    Zstd,
}

/// Parquet compression codec.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ParquetCompression {
    Uncompressed,
    #[default]
    Snappy,
    Gzip,
    Zstd,
    Lz4,
}

impl Config {
    /// Load configuration from a YAML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        Self::from_file_with_options(path, true)
    }

    /// Load configuration from a YAML file with optional environment variable interpolation.
    pub fn from_file_with_options(
        path: impl AsRef<Path>,
        interpolate_env: bool,
    ) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path.as_ref()).context(ReadFileSnafu)?;

        let content = if interpolate_env {
            let result = vars::interpolate(&content);
            if !result.is_ok() {
                let error_msg = result.errors.join("\n");
                return EnvInterpolationSnafu { message: error_msg }.fail();
            }
            result.text
        } else {
            content
        };

        let config: Config = serde_yaml::from_str(&content).context(YamlParseSnafu)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the configuration.
    fn validate(&self) -> Result<(), ConfigError> {
        ensure!(!self.source.path.is_empty(), EmptySourcePathSnafu);
        ensure!(!self.sink.path.is_empty(), EmptySinkPathSnafu);
        ensure!(!self.schema.fields.is_empty(), EmptySchemaSnafu);
        Ok(())
    }

    /// Convert the schema configuration to an Arrow schema.
    pub fn to_arrow_schema(&self) -> Arc<Schema> {
        let fields: Vec<Field> = self
            .schema
            .fields
            .iter()
            .map(|f| {
                let data_type = match f.field_type {
                    FieldType::String => DataType::Utf8,
                    FieldType::Int32 => DataType::Int32,
                    FieldType::Int64 => DataType::Int64,
                    FieldType::Float32 => DataType::Float32,
                    FieldType::Float64 => DataType::Float64,
                    FieldType::Boolean => DataType::Boolean,
                    FieldType::Timestamp => {
                        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
                    }
                    FieldType::Date => DataType::Date32,
                    FieldType::Json => DataType::Utf8, // Store JSON as string
                    FieldType::Binary => DataType::Binary,
                };
                Field::new(&f.name, data_type, f.nullable)
            })
            .collect();

        Arc::new(Schema::new(fields))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_type_to_arrow() {
        let config = Config {
            source: SourceConfig {
                path: "s3://bucket/input/*.ndjson.gz".to_string(),
                compression: CompressionFormat::Gzip,
                storage_options: HashMap::new(),
                batch_size: 8192,
                max_concurrent_files: 4,
                poll_interval_secs: 60,
                partition_filter: None,
            },
            sink: SinkConfig {
                path: "s3://bucket/output/table".to_string(),
                file_size_mb: 128,
                row_group_size_bytes: 128 * MB,
                inactivity_timeout_secs: None,
                rollover_timeout_secs: None,
                part_size_mb: 32,
                min_multipart_size_mb: 5,
                max_concurrent_uploads: 4,
                max_concurrent_parts: 8,
                storage_options: HashMap::new(),
                compression: ParquetCompression::Snappy,
            },
            schema: SchemaConfig {
                fields: vec![
                    FieldConfig {
                        name: "id".to_string(),
                        field_type: FieldType::String,
                        nullable: false,
                    },
                    FieldConfig {
                        name: "timestamp".to_string(),
                        field_type: FieldType::Timestamp,
                        nullable: false,
                    },
                    FieldConfig {
                        name: "value".to_string(),
                        field_type: FieldType::Float64,
                        nullable: true,
                    },
                ],
            },
            metrics: MetricsConfig::default(),
            error_handling: ErrorHandlingConfig::default(),
        };

        let schema = config.to_arrow_schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(!schema.field(0).is_nullable());
    }

    #[test]
    fn test_config_yaml_parsing() {
        let yaml = r#"
source:
  path: "s3://bucket/input/*.ndjson.gz"
  compression: gzip

sink:
  path: "s3://bucket/output/table"
  file_size_mb: 128

schema:
  fields:
    - name: id
      type: string
    - name: value
      type: float64
      nullable: true
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.schema.fields.len(), 2);
    }
}
