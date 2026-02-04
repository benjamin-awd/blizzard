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

    /// Required regex capture group missing (internal error).
    #[snafu(display("Missing required URL component: {group}"))]
    RegexGroupMissing { group: String },

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

    /// Table URI is empty for a specific table.
    #[snafu(display("Table '{table}' has empty table_uri"))]
    EmptyTableUriForTable { table: String },

    /// Source path is empty for a specific pipeline.
    #[snafu(display("Pipeline '{pipeline}' has empty source path"))]
    EmptySourcePathForPipeline { pipeline: String },

    /// Table URI is empty for a specific pipeline.
    #[snafu(display("Pipeline '{pipeline}' has empty table_uri"))]
    EmptyTableUriForPipeline { pipeline: String },

    /// Schema is empty for a specific pipeline.
    #[snafu(display(
        "Pipeline '{pipeline}' has empty schema (specify either 'infer: true' or 'fields')"
    ))]
    EmptySchemaForPipeline { pipeline: String },

    /// Schema has conflicting options.
    #[snafu(display(
        "Pipeline '{pipeline}' has invalid schema: cannot specify both 'infer: true' and 'fields'"
    ))]
    SchemaConflict { pipeline: String },

    /// Environment variable interpolation failed.
    #[snafu(display("Environment variable interpolation failed:\n{message}"))]
    EnvInterpolation { message: String },

    /// Failed to parse YAML configuration.
    #[snafu(display("Failed to parse YAML: {source}"))]
    YamlParse { source: serde_yaml::Error },

    /// Failed to read configuration file.
    #[snafu(display("Failed to read configuration file: {source}"))]
    ReadFile { source: std::io::Error },

    /// Resource conflict detected (e.g., two tables using the same staging directory).
    #[snafu(display("Resource conflict: {message}"))]
    ResourceConflict { message: String },

    /// Duplicate component keys found across config files.
    #[snafu(display("Duplicate component keys: {}", keys.join(", ")))]
    DuplicateComponents { keys: Vec<String> },

    /// Unsupported config file format.
    #[snafu(display("Unsupported config format for {}: only .yaml/.yml supported", path.display()))]
    UnsupportedFormat { path: std::path::PathBuf },

    /// Failed to read configuration directory.
    #[snafu(display("Failed to read directory {}", path.display()))]
    ReadDir {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    /// Multiple configuration errors occurred.
    #[snafu(display("Multiple config errors:\n{}", errors.join("\n")))]
    MultipleErrors { errors: Vec<String> },

    /// Generic internal configuration error.
    #[snafu(display("{message}"))]
    Internal { message: String },
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

    /// Metrics server already initialized (double-init attempted).
    #[snafu(display("Metrics server already initialized"))]
    AlreadyInitialized,

    /// Metrics server not initialized (controller accessed before init).
    #[snafu(display("Metrics server not initialized"))]
    NotInitialized,
}

// ============ Pipeline Setup Errors ============

/// Errors that can occur during pipeline setup (before running).
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum PipelineSetupError {
    /// Failed to parse metrics address.
    #[snafu(display("Failed to parse metrics address: {source}"))]
    AddressParse { source: std::net::AddrParseError },

    /// Failed to initialize metrics.
    #[snafu(display("Failed to initialize metrics: {source}"))]
    Metrics { source: MetricsError },
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
