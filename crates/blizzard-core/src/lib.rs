//! blizzard-core: Shared components for blizzard and penguin crates.
//!
//! This crate contains common functionality used by both the file loader (blizzard)
//! and the delta checkpointer (penguin):
//!
//! - `storage/` - Multi-cloud storage abstraction (S3, GCS, Azure, local)
//! - `metrics/` - Prometheus metrics infrastructure
//! - `config/` - Common configuration types and environment variable interpolation
//! - `topology/` - Shared orchestration primitives for multi-component pipelines
//! - `resource/` - Resource management (storage pool, memory budget)
//! - `polling` - Generic polling loop trait and runner
//! - `signal` - Signal handling for graceful shutdown
//! - `types` - Common types like `FinishedFile`
//! - `error` - Common error types
//! - `app` - Application abstraction for reducing main.rs boilerplate

pub mod app;
pub mod config;
pub mod error;
pub mod metrics;
pub mod partition;
pub mod polling;
pub mod resource;
pub mod signal;
pub mod storage;
pub mod topology;
pub mod tracing;
pub mod types;
pub mod watermark;

// Re-export commonly used items
pub use app::{AppConfig, Application};
pub use config::{
    CliArgs, ComponentKey, ErrorHandlingConfig, GlobalConfig, KB, MB, MetricsConfig,
    ParquetCompression, Resource,
};
pub use error::{ConfigError, DlqError, MetricsError, PipelineSetupError, StorageError};
pub use metrics::{
    DEFAULT_METRICS_ADDR, MetricsController, UtilizationTimer, init_global as init_metrics,
    init_test as init_metrics_test,
};
pub use partition::PartitionExtractor;
pub use polling::{IterationResult, PollingProcessor, run_polling_loop};
pub use resource::{StoragePool, StoragePoolRef};
pub use signal::shutdown_signal;
pub use storage::{StorageProvider, StorageProviderRef, get_or_create_storage};
pub use topology::{
    Pipeline, PipelineContext, PipelineRunner, RunningTopology, Task, TaskError, TaskOutput,
    TaskResult, random_jitter, run_pipelines,
};
pub use tracing::init_tracing;
pub use types::FinishedFile;
pub use watermark::{
    FileListingConfig, generate_prefixes, list_files_above_watermark, list_files_cold_start,
    parse_watermark,
};
