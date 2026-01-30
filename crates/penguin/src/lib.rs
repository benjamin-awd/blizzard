//! Penguin: Delta Lake checkpointer that watches staging and commits to Delta Lake.
//!
//! This crate handles:
//! - Watching the staging directory for new Parquet files
//! - Committing Parquet files to Delta Lake with atomic checkpoints
//! - Moving committed files from pending/ to committed/

pub mod checkpoint;
pub mod config;
pub mod error;
pub mod metrics;
pub mod pipeline;
pub mod schema;
pub mod sink;
pub mod staging;

// Re-export commonly used items
pub use config::Config;
pub use error::PipelineError;
pub use pipeline::{MultiTableStats, PipelineStats, run_pipeline};
pub use schema::{SchemaComparison, SchemaEvolutionMode, compare_schemas, merge_schemas};

// Re-export from blizzard-common
pub use blizzard_common::{
    KB, MB, MetricsConfig, StorageProvider, StorageProviderRef, init_metrics, shutdown_signal,
};
