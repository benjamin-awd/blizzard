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
pub use pipeline::Pipeline;
pub use schema::{SchemaComparison, SchemaEvolutionMode, SchemaManager, compare_schemas, merge_schemas};
pub use sink::{DeltaSink, TableSink};

// Re-export from blizzard-core
pub use blizzard_core::{
    Application, CliArgs, KB, MB, MetricsConfig, PipelineContext, StorageProvider,
    StorageProviderRef, init_metrics, init_tracing, run_pipelines, shutdown_signal,
};
