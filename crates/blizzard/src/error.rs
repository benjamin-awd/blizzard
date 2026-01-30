//! Error types for the blizzard file loader.

use snafu::prelude::*;

// Re-export common errors
pub use blizzard_common::error::{ConfigError, DlqError, StorageError};

/// Errors that can occur during schema inference from NDJSON files.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum InferenceError {
    /// No files found for schema inference.
    #[snafu(display("No files found for schema inference"))]
    NoFilesFound,

    /// Failed to read file for inference.
    #[snafu(display("Failed to read file for inference: {source}"))]
    ReadFile { source: StorageError },

    /// Failed to decompress file for inference.
    #[snafu(display("Failed to decompress file for inference: {message}"))]
    Decompression { message: String },

    /// Failed to parse JSON during inference.
    #[snafu(display("Failed to parse JSON during inference: {message}"))]
    JsonParse { message: String },

    /// No valid JSON records found for inference.
    #[snafu(display("No valid JSON records found for schema inference"))]
    NoValidRecords,
}

/// Errors that can occur during NDJSON reading and parsing.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ReaderError {
    /// Failed to create zstd decoder.
    #[snafu(display("Failed to create zstd decoder for {path}: {source}"))]
    ZstdDecompression {
        path: String,
        source: std::io::Error,
    },

    /// Failed to build Arrow decoder.
    #[snafu(display("Failed to build Arrow JSON decoder: {message}"))]
    DecoderBuild { message: String },

    /// Failed to decode JSON.
    #[snafu(display("Failed to decode JSON in {path}: {message}"))]
    JsonDecode { path: String, message: String },
}

/// Errors that can occur during Parquet writing.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ParquetError {
    /// Failed to write to Parquet.
    #[snafu(display("Failed to write to Parquet: {source}"))]
    ParquetWrite {
        source: deltalake::parquet::errors::ParquetError,
    },

    /// Failed to create Parquet writer.
    #[snafu(display("Failed to create Parquet writer: {source}"))]
    WriterCreate {
        source: deltalake::parquet::errors::ParquetError,
    },

    /// Writer unavailable (already closed).
    #[snafu(display("Parquet writer unavailable (already closed)"))]
    WriterUnavailable,

    /// Buffer lock failed.
    #[snafu(display("Failed to lock Parquet buffer"))]
    BufferLock,

    /// Buffer still in use.
    #[snafu(display("Parquet buffer still in use by another task"))]
    BufferInUse,
}

/// Errors that can occur during staging directory operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum StagingError {
    /// Failed to write staging file.
    #[snafu(display("Failed to write staging file: {source}"))]
    StagingWrite { source: StorageError },

    /// Failed to serialize metadata.
    #[snafu(display("Failed to serialize staging metadata: {source}"))]
    Serialize { source: serde_json::Error },

    /// Failed to create staging directory.
    #[snafu(display("Failed to create staging directory: {source}"))]
    CreateDir { source: std::io::Error },
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

    /// Schema inference error.
    #[snafu(display("Schema inference error: {source}"))]
    Inference { source: InferenceError },

    /// Reader error.
    #[snafu(display("Reader error: {source}"))]
    Reader { source: ReaderError },

    /// Parquet error.
    #[snafu(display("Parquet error: {source}"))]
    Parquet { source: ParquetError },

    /// DLQ error.
    #[snafu(display("DLQ error: {source}"))]
    Dlq { source: DlqError },

    /// Staging error.
    #[snafu(display("Staging error: {source}"))]
    Staging { source: StagingError },

    /// Task join error.
    #[snafu(display("Task join error: {source}"))]
    TaskJoin { source: tokio::task::JoinError },

    /// Channel closed.
    #[snafu(display("Channel closed unexpectedly"))]
    ChannelClosed,

    /// Failed to parse metrics address.
    #[snafu(display("Failed to parse metrics address: {source}"))]
    AddressParse { source: std::net::AddrParseError },

    /// Maximum failures exceeded.
    #[snafu(display("Maximum failures exceeded: {count} failures"))]
    MaxFailures { count: usize },

    /// Metrics error.
    #[snafu(display("Metrics error: {source}"))]
    Metrics {
        source: blizzard_common::MetricsError,
    },
}

impl PipelineError {
    /// Check if this error represents a "not found" condition.
    pub fn is_not_found(&self) -> bool {
        match self {
            PipelineError::Storage { source } => source.is_not_found(),
            _ => false,
        }
    }
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

impl From<ParquetError> for PipelineError {
    fn from(source: ParquetError) -> Self {
        PipelineError::Parquet { source }
    }
}

impl From<StagingError> for PipelineError {
    fn from(source: StagingError) -> Self {
        PipelineError::Staging { source }
    }
}

impl From<DlqError> for PipelineError {
    fn from(source: DlqError) -> Self {
        PipelineError::Dlq { source }
    }
}

impl From<ReaderError> for PipelineError {
    fn from(source: ReaderError) -> Self {
        PipelineError::Reader { source }
    }
}

impl From<InferenceError> for PipelineError {
    fn from(source: InferenceError) -> Self {
        PipelineError::Inference { source }
    }
}
