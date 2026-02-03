//! Error types for the penguin delta checkpointer.

use snafu::prelude::*;

// Re-export common errors
pub use blizzard_core::error::{ConfigError, StorageError};

/// Errors that can occur during schema inference and evolution.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
#[snafu(module)]
pub enum SchemaError {
    /// Failed to decode parquet footer.
    #[snafu(display("Failed to decode parquet footer: {source}"))]
    ParquetFooter {
        source: deltalake::parquet::errors::ParquetError,
    },

    /// Failed to decode parquet metadata.
    #[snafu(display("Failed to decode parquet metadata: {source}"))]
    ParquetMetadata {
        source: deltalake::parquet::errors::ParquetError,
    },

    /// Failed to convert parquet schema to Arrow schema.
    #[snafu(display("Failed to convert schema: {source}"))]
    ArrowConversion {
        source: deltalake::parquet::errors::ParquetError,
    },

    /// No files available for schema inference.
    #[snafu(display("No files available for schema inference"))]
    NoFilesAvailable,

    /// Storage error while reading file for schema inference.
    #[snafu(display("Storage error during schema inference: {source}"))]
    StorageRead { source: StorageError },

    /// Incoming schema is incompatible with table schema.
    #[snafu(display("Incompatible schema: {details}"))]
    IncompatibleSchema { details: String },

    /// Attempted to add a required (non-nullable) field.
    #[snafu(display("Cannot add required field '{field_name}' - new fields must be nullable"))]
    RequiredFieldAddition { field_name: String },

    /// Type change is not allowed between schemas.
    #[snafu(display("Type change not allowed for field '{field}': {from} -> {to}"))]
    TypeChangeNotAllowed {
        field: String,
        from: String,
        to: String,
    },
}

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

impl DeltaError {
    /// Check if this error indicates that the table was not found.
    pub fn is_table_not_found(&self) -> bool {
        match self {
            DeltaError::DeltaOperation { source } => {
                // Check for common "not found" patterns in the error message
                let msg = source.to_string().to_lowercase();
                msg.contains("not found")
                    || msg.contains("no such file")
                    || msg.contains("does not exist")
                    || msg.contains("no log files")
                    || msg.contains("no files in log")
                    || msg.contains("not a table")
            }
            _ => false,
        }
    }
}

/// Errors that can occur during incoming file operations.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
#[snafu(module)]
pub enum IncomingError {
    /// Failed to list files.
    #[snafu(display("Failed to list incoming files: {source}"))]
    List { source: StorageError },

    /// Failed to read parquet metadata.
    #[snafu(display("Failed to read parquet metadata for {path}: {source}"))]
    ParquetMetadata {
        path: String,
        source: deltalake::parquet::errors::ParquetError,
    },

    /// Failed to read file.
    #[snafu(display("Failed to read incoming file {path}: {source}"))]
    Read { path: String, source: StorageError },

    /// Invalid watermark format.
    #[snafu(display("Invalid watermark format: {watermark}"))]
    InvalidWatermark { watermark: String },
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

    /// Incoming error.
    #[snafu(display("Incoming error: {source}"))]
    Incoming { source: IncomingError },

    /// Schema inference error.
    #[snafu(display("Schema error: {source}"))]
    Schema { source: SchemaError },

    /// Task join error.
    #[snafu(display("Task join error: {source}"))]
    TaskJoin { source: tokio::task::JoinError },

    /// Failed to parse metrics address.
    #[snafu(display("Failed to parse metrics address: {source}"))]
    AddressParse { source: std::net::AddrParseError },

    /// Metrics error.
    #[snafu(display("Metrics error: {source}"))]
    Metrics {
        source: blizzard_core::MetricsError,
    },

    /// Internal state error - delta sink not initialized when expected.
    #[snafu(display("Internal error: delta sink not initialized"))]
    DeltaSinkNotInitialized,
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

impl From<SchemaError> for PipelineError {
    fn from(source: SchemaError) -> Self {
        PipelineError::Schema { source }
    }
}

impl From<IncomingError> for PipelineError {
    fn from(source: IncomingError) -> Self {
        PipelineError::Incoming { source }
    }
}

impl From<blizzard_core::PipelineSetupError> for PipelineError {
    fn from(source: blizzard_core::PipelineSetupError) -> Self {
        match source {
            blizzard_core::PipelineSetupError::AddressParse { source } => {
                PipelineError::AddressParse { source }
            }
            blizzard_core::PipelineSetupError::Metrics { source } => {
                PipelineError::Metrics { source }
            }
        }
    }
}
