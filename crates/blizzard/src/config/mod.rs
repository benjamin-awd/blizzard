//! Configuration for the blizzard file loader.

use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::ConfigError;
pub use blizzard_common::config::{
    ErrorHandlingConfig, InterpolationResult, MetricsConfig, ParquetCompression, interpolate,
};
pub use blizzard_common::{KB, MB};

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

/// Main configuration for blizzard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Source configuration.
    pub source: SourceConfig,
    /// Sink configuration.
    pub sink: SinkConfig,
    /// Schema configuration.
    pub schema: SchemaConfig,
    /// Metrics configuration.
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Error handling configuration.
    #[serde(default)]
    pub error_handling: ErrorHandlingConfig,
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
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.source.path.is_empty() {
            return Err(ConfigError::EmptySourcePath);
        }
        if self.sink.table_uri.is_empty() {
            return Err(ConfigError::EmptyTableUri);
        }
        if self.schema.fields.is_empty() {
            return Err(ConfigError::EmptySchema);
        }
        Ok(())
    }
}
