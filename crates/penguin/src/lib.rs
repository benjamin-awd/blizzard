//! Penguin: Delta Lake checkpointer that discovers parquet files and commits to Delta Lake.
//!
//! This crate handles:
//! - Discovering uncommitted Parquet files in table directories using watermark tracking
//! - Committing Parquet files to Delta Lake with atomic checkpoints
//! - Schema inference and evolution

pub mod checkpoint;
pub mod config;
pub mod error;
pub mod incoming;
pub mod metrics;
pub mod pipeline;
pub mod schema;
pub mod sink;

// Re-export commonly used items
pub use config::Config;
pub use error::PipelineError;
pub use pipeline::{MultiTableStats, PipelineStats, run_pipeline};
pub use schema::{SchemaComparison, SchemaEvolutionMode, compare_schemas, merge_schemas};

// Re-export from blizzard-common
pub use blizzard_common::{
    KB, MB, MetricsConfig, StorageProvider, StorageProviderRef, init_metrics, shutdown_signal,
};
