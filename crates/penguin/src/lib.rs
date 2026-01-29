//! Penguin: Delta Lake checkpointer that watches staging and commits to Delta Lake.
//!
//! This crate handles:
//! - Watching the staging directory for new Parquet files
//! - Committing Parquet files to Delta Lake with atomic checkpoints
//! - Moving committed files from pending/ to committed/

pub mod checkpoint;
pub mod config;
pub mod error;
pub mod pipeline;
pub mod schema;
pub mod sink;
pub mod staging;

// Re-export commonly used items
pub use config::Config;
pub use error::PipelineError;
pub use pipeline::run_pipeline;

// Re-export from blizzard-common
pub use blizzard_common::{
    KB, MB, MetricsConfig, StorageProvider, StorageProviderRef, init_metrics, shutdown_signal,
};
