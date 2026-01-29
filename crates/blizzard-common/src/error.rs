//! Common error types shared between blizzard and penguin.
//!
//! This module defines error types for storage, configuration, metrics, and DLQ
//! operations that are used by both crates.

use snafu::prelude::*;

// ============ Storage Errors ============

/// Errors that can occur during storage operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum StorageError {
    /// Invalid storage URL format.
    #[snafu(display("Invalid storage URL: {url}"))]
    InvalidUrl { url: String },

    /// Object store operation failed.
    #[snafu(display("Storage operation failed: {source}"))]
    ObjectStore { source: object_store::Error },

    /// IO error during storage operations.
    #[snafu(display("IO error: {source}"))]
    Io { source: std::io::Error },

    /// S3 configuration error.
    #[snafu(display("S3 configuration error: {source}"))]
    S3Config { source: object_store::Error },

    /// GCS configuration error.
    #[snafu(display("GCS configuration error: {source}"))]
    GcsConfig { source: object_store::Error },

    /// Azure configuration error.
    #[snafu(display("Azure configuration error: {source}"))]
    AzureConfig { source: object_store::Error },
}

impl StorageError {
    /// Check if this error represents a "not found" condition (404, NoSuchKey, etc.)
    pub fn is_not_found(&self) -> bool {
        match self {
            StorageError::ObjectStore { source } => {
                matches!(source, object_store::Error::NotFound { .. })
            }
            _ => false,
        }
    }
}

// ============ Config Errors ============

/// Errors that can occur during configuration parsing and validation.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ConfigError {
    /// Source path is empty.
    #[snafu(display("Source path cannot be empty"))]
    EmptySourcePath,

    /// Sink path is empty.
    #[snafu(display("Sink path cannot be empty"))]
    EmptySinkPath,

    /// Schema has no fields.
    #[snafu(display("Schema must have at least one field"))]
    EmptySchema,

    /// Table URI is empty.
    #[snafu(display("Table URI cannot be empty"))]
    EmptyTableUri,

    /// Environment variable interpolation failed.
    #[snafu(display("Environment variable interpolation failed:\n{message}"))]
    EnvInterpolation { message: String },

    /// Failed to parse YAML configuration.
    #[snafu(display("Failed to parse YAML configuration"))]
    YamlParse { source: serde_yaml::Error },

    /// Failed to read configuration file.
    #[snafu(display("Failed to read configuration file"))]
    ReadFile { source: std::io::Error },
}

// ============ Metrics Errors ============

/// Errors that can occur during metrics initialization.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MetricsError {
    /// Failed to initialize Prometheus recorder.
    #[snafu(display("Failed to initialize Prometheus recorder"))]
    PrometheusInit {
        source: metrics_exporter_prometheus::BuildError,
    },
}

// ============ DLQ Errors ============

/// Errors that can occur during Dead Letter Queue operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
// Prefix is intentional to avoid snafu selector conflicts (e.g., WriteSnafu)
#[allow(clippy::enum_variant_names)]
pub enum DlqError {
    /// Failed to write to DLQ.
    #[snafu(display("Failed to write to DLQ"))]
    DlqWrite { source: StorageError },

    /// Failed to serialize failed file record.
    #[snafu(display("Failed to serialize DLQ record"))]
    DlqSerialize { source: serde_json::Error },

    /// Failed to create DLQ storage provider.
    #[snafu(display("Failed to create DLQ storage"))]
    DlqStorage { source: StorageError },
}
