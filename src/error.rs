//! Error types for Blizzard using snafu.
//!
//! This module defines structured error types with context selectors for
//! all error conditions in the codebase.

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
    #[snafu(display("Storage operation failed"))]
    ObjectStore { source: object_store::Error },

    /// IO error during storage operations.
    #[snafu(display("IO error"))]
    Io { source: std::io::Error },

    /// S3 configuration error.
    #[snafu(display("S3 configuration error"))]
    S3Config { source: object_store::Error },

    /// GCS configuration error.
    #[snafu(display("GCS configuration error"))]
    GcsConfig { source: object_store::Error },

    /// Azure configuration error.
    #[snafu(display("Azure configuration error"))]
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

// ============ Reader Errors ============

/// Errors that can occur during NDJSON file reading.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ReaderError {
    /// Zstd decompression failed (during decoder creation).
    #[snafu(display("Zstd decompression failed for {path}"))]
    ZstdDecompression {
        source: std::io::Error,
        path: String,
    },

    /// Failed to build JSON decoder.
    #[snafu(display("Failed to build JSON decoder: {message}"))]
    DecoderBuild { message: String },

    /// Failed to decode JSON (includes streaming decompression errors).
    #[snafu(display("Failed to decode JSON for {path}: {message}"))]
    JsonDecode { path: String, message: String },
}

// ============ Delta Errors ============

/// Errors that can occur during Delta Lake operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum DeltaError {
    /// Failed to create struct type.
    #[snafu(display("Failed to create struct type: {message}"))]
    StructType { message: String },

    /// Schema conversion error.
    #[snafu(display("Schema conversion failed"))]
    SchemaConversion {
        source: deltalake::arrow::error::ArrowError,
    },

    /// Delta Lake operation failed.
    #[snafu(display("Delta Lake operation failed"))]
    DeltaLake { source: deltalake::DeltaTableError },

    /// Failed to parse URL.
    #[snafu(display("Failed to parse URL"))]
    UrlParse { source: url::ParseError },

    /// JSON serialization/deserialization error for checkpoint data.
    #[snafu(display("JSON error in checkpoint"))]
    CheckpointJson { source: serde_json::Error },

    /// Base64 decode error.
    #[snafu(display("Base64 decode error"))]
    Base64Decode { source: base64::DecodeError },

    /// Failed to parse object store path.
    #[snafu(display("Failed to parse path: {path}"))]
    PathParse { path: String },

    /// Checkpoint state corrupted (missing expected prefix after validation).
    #[snafu(display("Checkpoint state corrupted: invalid app_id format"))]
    CheckpointCorrupted,
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

// ============ Parquet Errors ============

/// Errors that can occur during Parquet file writing.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ParquetError {
    /// Parquet write error.
    #[snafu(display("Parquet write error"))]
    Write {
        source: deltalake::parquet::errors::ParquetError,
    },

    /// Failed to create Parquet writer.
    #[snafu(display("Failed to create Parquet writer"))]
    WriterCreate {
        source: deltalake::parquet::errors::ParquetError,
    },

    /// Writer is not available (internal state error).
    #[snafu(display("Parquet writer is not available"))]
    WriterUnavailable,

    /// Buffer lock error (mutex poisoned).
    #[snafu(display("Buffer lock failed: mutex poisoned"))]
    BufferLock,

    /// Buffer has outstanding references and cannot be consumed.
    #[snafu(display("Buffer has outstanding references"))]
    BufferInUse,
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

// ============ Pipeline Error (top-level) ============

/// Top-level pipeline errors that aggregate all error types.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum PipelineError {
    /// Storage error.
    #[snafu(display("Storage error"))]
    PipelineStorage { source: StorageError },

    /// Configuration error.
    #[snafu(display("Configuration error"))]
    Config { source: ConfigError },

    /// Reader error.
    #[snafu(display("Reader error"))]
    Reader { source: ReaderError },

    /// Delta Lake error.
    #[snafu(display("Delta error"))]
    Delta { source: DeltaError },

    /// Parquet error.
    #[snafu(display("Parquet error"))]
    Parquet { source: ParquetError },

    /// Task join error.
    #[snafu(display("Task join error"))]
    TaskJoin { source: tokio::task::JoinError },

    /// Channel send error.
    #[snafu(display("Channel closed unexpectedly"))]
    ChannelClosed,

    /// Address parsing error.
    #[snafu(display("Failed to parse address"))]
    AddressParse { source: std::net::AddrParseError },

    /// Metrics error.
    #[snafu(display("Metrics error"))]
    Metrics { source: MetricsError },

    /// DLQ error.
    #[snafu(display("DLQ error"))]
    Dlq { source: DlqError },

    /// Max failures exceeded.
    #[snafu(display("Max failures exceeded: {count} failures"))]
    MaxFailuresExceeded { count: usize },
}

impl PipelineError {
    /// Check if this error represents a "not found" condition (404, NoSuchKey, etc.)
    pub fn is_not_found(&self) -> bool {
        match self {
            PipelineError::PipelineStorage { source } => source.is_not_found(),
            _ => false,
        }
    }
}
