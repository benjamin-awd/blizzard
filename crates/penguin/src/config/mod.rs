//! Configuration for the penguin delta checkpointer.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub use blizzard_common::config::{
    ErrorHandlingConfig, InterpolationResult, MetricsConfig, ParquetCompression, interpolate,
};
pub use blizzard_common::{KB, MB};

use crate::error::ConfigError;

fn default_poll_interval() -> u64 {
    10
}

/// Configuration for the source Delta table.
///
/// The staging directory is derived from the table URI: `{table_uri}/_staging/`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Source configuration (Delta table URI from which staging metadata is read).
    pub source: SourceConfig,
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
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.source.table_uri.is_empty() {
            return Err(ConfigError::EmptyTableUri);
        }
        Ok(())
    }
}
