//! Blizzard: File loader for streaming NDJSON.gz files to Parquet staging.
//!
//! This crate handles:
//! - Reading compressed NDJSON files from cloud storage (S3, GCS, Azure, local)
//! - Parsing and validating records against a schema
//! - Writing Parquet files with rolling policies
//! - Writing finished files to a staging directory for penguin to commit
//! - Dead letter queue for failed records

pub mod config;
pub mod dlq;
pub mod error;
pub mod pipeline;
pub mod sink;
pub mod source;
pub mod staging;

// Re-export commonly used items
pub use config::Config;
pub use error::PipelineError;
pub use pipeline::run_pipeline;

// Re-export from blizzard-common
pub use blizzard_common::{
    KB, MB, MetricsConfig, ParquetCompression, StorageProvider, StorageProviderRef, init_metrics,
    shutdown_signal,
};
