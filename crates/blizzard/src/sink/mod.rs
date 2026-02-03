//! Sink components for writing Parquet files to storage.

mod parquet;
mod storage;
mod traits;

pub use parquet::{ParquetWriter, ParquetWriterConfig, RollingPolicy, WriterStats};
pub use storage::StorageWriter;
pub use traits::{BatchWriter, BatchWriterError};

// Re-export FinishedFile from blizzard-core
pub use blizzard_core::FinishedFile;
