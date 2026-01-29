//! Common configuration types shared between blizzard and penguin.

mod vars;

pub use vars::{InterpolationResult, interpolate};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Byte size constants (binary/IEC units).
pub const KB: usize = 1024;
pub const MB: usize = 1024 * KB;

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
