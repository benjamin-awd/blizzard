//! Blizzard: File loader for streaming NDJSON.gz files to Parquet.
//!
//! This crate handles:
//! - Reading compressed NDJSON files from cloud storage (S3, GCS, Azure, local)
//! - Parsing and validating records against a schema
//! - Writing Parquet files with rolling policies
//! - Writing finished files directly to Delta table directories
//! - Dead letter queue for failed records

pub mod checkpoint;
pub mod config;
pub mod dlq;
pub mod error;
pub mod pipeline;
pub mod sink;
pub mod source;

/// Re-export storage module from blizzard-core for convenience
pub mod storage {
    pub use blizzard_core::storage::*;
}

// Re-export commonly used items
pub use config::Config;
pub use error::PipelineError;
pub use pipeline::Pipeline;

// Re-export from blizzard-core
pub use blizzard_core::{
    CliArgs, KB, MB, MetricsConfig, ParquetCompression, PipelineContext, StorageProvider,
    StorageProviderRef, init_metrics, init_tracing, run_pipelines, shutdown_signal,
};
