//! Error types for the penguin delta checkpointer.

use snafu::prelude::*;

// Re-export common errors
pub use blizzard_common::error::{ConfigError, StorageError};

/// Errors that can occur during Delta Lake operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum DeltaError {
    /// Failed to parse Delta table URL.
    #[snafu(display("Failed to parse Delta table URL: {url}"))]
    UrlParse { url: String },

    /// Delta Lake operation failed.
    #[snafu(display("Delta Lake operation failed: {source}"))]
    DeltaOperation { source: deltalake::DeltaTableError },

    /// Failed to encode checkpoint JSON.
    #[snafu(display("Failed to encode checkpoint JSON: {source}"))]
    CheckpointJsonEncode { source: serde_json::Error },

    /// Failed to decode checkpoint JSON.
    #[snafu(display("Failed to decode checkpoint JSON: {source}"))]
    CheckpointJsonDecode { source: serde_json::Error },

    /// Failed to encode/decode base64.
    #[snafu(display("Failed to encode/decode base64: {source}"))]
    Base64 { source: base64::DecodeError },

    /// Invalid checkpoint format.
    #[snafu(display("Invalid checkpoint format: {message}"))]
    InvalidCheckpoint { message: String },

    /// Failed to extract struct type from schema.
    #[snafu(display("Failed to extract struct type from schema: {message}"))]
    StructType { message: String },

    /// Failed to convert schema.
    #[snafu(display("Failed to convert schema: {source}"))]
    SchemaConversion {
        source: deltalake::arrow::error::ArrowError,
    },

    /// Failed to parse path.
    #[snafu(display("Failed to parse path: {path}"))]
    PathParse { path: String },
}

/// Errors that can occur during staging operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum StagingError {
    /// Failed to read staging file.
    #[snafu(display("Failed to read staging file: {source}"))]
    Read { source: StorageError },

    /// Failed to deserialize metadata.
    #[snafu(display("Failed to deserialize staging metadata: {source}"))]
    Deserialize { source: serde_json::Error },

    /// Failed to delete file.
    #[snafu(display("Failed to delete file {path}: {source}"))]
    Delete { path: String, source: StorageError },
}

/// Top-level pipeline errors.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum PipelineError {
    /// Configuration error.
    #[snafu(display("Configuration error: {source}"))]
    Config { source: ConfigError },

    /// Storage error.
    #[snafu(display("Storage error: {source}"))]
    Storage { source: StorageError },

    /// Delta error.
    #[snafu(display("Delta error: {source}"))]
    Delta { source: DeltaError },

    /// Staging error.
    #[snafu(display("Staging error: {source}"))]
    Staging { source: StagingError },

    /// Task join error.
    #[snafu(display("Task join error: {source}"))]
    TaskJoin { source: tokio::task::JoinError },

    /// Failed to parse metrics address.
    #[snafu(display("Failed to parse metrics address: {source}"))]
    AddressParse { source: std::net::AddrParseError },

    /// Metrics error.
    #[snafu(display("Metrics error: {source}"))]
    Metrics {
        source: blizzard_common::MetricsError,
    },
}

impl From<StorageError> for PipelineError {
    fn from(source: StorageError) -> Self {
        PipelineError::Storage { source }
    }
}

impl From<ConfigError> for PipelineError {
    fn from(source: ConfigError) -> Self {
        PipelineError::Config { source }
    }
}

impl From<DeltaError> for PipelineError {
    fn from(source: DeltaError) -> Self {
        PipelineError::Delta { source }
    }
}

impl From<StagingError> for PipelineError {
    fn from(source: StagingError) -> Self {
        PipelineError::Staging { source }
    }
}
