//! Common configuration types shared between blizzard and penguin.

mod component_key;
mod global;
mod loader;
mod path;
mod resource;
mod vars;

pub use component_key::ComponentKey;
pub use global::GlobalConfig;
pub use loader::{Mergeable, load_from_paths};
pub use path::{CliArgs, ConfigPath, is_yaml_file};
pub use resource::Resource;
pub use vars::{InterpolationResult, interpolate};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Byte size constants (binary/IEC units).
pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;

/// A string value or list of strings, for YAML ergonomics.
///
/// Allows writing either `key: "value"` or `key: ["a", "b"]` in config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringOrVec {
    Single(String),
    Multiple(Vec<String>),
}

impl StringOrVec {
    /// Return the values as a slice.
    pub fn values(&self) -> &[String] {
        match self {
            StringOrVec::Single(s) => std::slice::from_ref(s),
            StringOrVec::Multiple(v) => v,
        }
    }
}

/// Configuration for a partition filter.
///
/// Used for efficient date-based listing during cold starts or polling.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionFilterConfig {
    /// strftime-style prefix template (e.g., "date=%Y-%m-%d/hour=%H").
    pub prefix_template: String,
    /// Number of units to look back (days or hours depending on template).
    #[serde(default)]
    pub lookback: u32,
    /// Include filters: key-value pairs that extend the S3 prefix or act as
    /// client-side filters. Keys matching `{key}` placeholders in the template
    /// extend the prefix (cartesian product); others become client-side filters.
    #[serde(default)]
    pub include: HashMap<String, StringOrVec>,
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

/// Metrics configuration for Prometheus endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    /// Address to bind the metrics HTTP server (default: "0.0.0.0:9090").
    #[serde(default = "default_metrics_address")]
    pub address: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            address: default_metrics_address(),
        }
    }
}

impl MetricsConfig {
    /// Merge values from another MetricsConfig (last-write-wins).
    pub fn merge_from(&mut self, other: &Self) {
        if other.address != default_metrics_address() {
            self.address = other.address.clone();
        }
    }
}

fn default_metrics_address() -> String {
    "0.0.0.0:9090".to_string()
}

/// Error handling configuration for resilient pipeline execution.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Default maximum concurrent upload operations.
pub const DEFAULT_MAX_CONCURRENT_UPLOADS: usize = 4;
/// Default maximum concurrent parts per multipart upload.
pub const DEFAULT_MAX_CONCURRENT_PARTS: usize = 8;
/// Default part size for multipart uploads in MB.
pub const DEFAULT_PART_SIZE_MB: usize = 10;
/// Default minimum file size to use multipart upload in MB.
pub const DEFAULT_MIN_MULTIPART_SIZE_MB: usize = 100;

/// Serde default for `max_concurrent_uploads`.
pub fn default_max_concurrent_uploads() -> usize {
    DEFAULT_MAX_CONCURRENT_UPLOADS
}
/// Serde default for `max_concurrent_parts`.
pub fn default_max_concurrent_parts() -> usize {
    DEFAULT_MAX_CONCURRENT_PARTS
}
/// Serde default for `part_size_mb`.
pub fn default_part_size_mb() -> usize {
    DEFAULT_PART_SIZE_MB
}
/// Serde default for `min_multipart_size_mb`.
pub fn default_min_multipart_size_mb() -> usize {
    DEFAULT_MIN_MULTIPART_SIZE_MB
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
