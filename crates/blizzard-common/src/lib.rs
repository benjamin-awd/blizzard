//! blizzard-common: Shared components for blizzard and penguin crates.
//!
//! This crate contains common functionality used by both the file loader (blizzard)
//! and the delta checkpointer (penguin):
//!
//! - `storage/` - Multi-cloud storage abstraction (S3, GCS, Azure, local)
//! - `metrics/` - Prometheus metrics infrastructure
//! - `config/` - Common configuration types and environment variable interpolation
//! - `polling` - Generic polling loop trait and runner
//! - `signal` - Signal handling for graceful shutdown
//! - `types` - Common types like `FinishedFile`
//! - `error` - Common error types

pub mod config;
pub mod error;
pub mod metrics;
pub mod polling;
pub mod signal;
pub mod storage;
pub mod types;

// Re-export commonly used items
pub use config::{ErrorHandlingConfig, KB, MB, MetricsConfig, ParquetCompression};
pub use error::{ConfigError, DlqError, MetricsError, StorageError};
pub use metrics::{UtilizationTimer, init as init_metrics};
pub use polling::{IterationResult, PollingProcessor, run_polling_loop};
pub use signal::shutdown_signal;
pub use storage::{StorageProvider, StorageProviderRef};
pub use types::FinishedFile;
