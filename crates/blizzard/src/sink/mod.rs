//! Sink components for writing Parquet files.

mod parquet;

pub use parquet::{ParquetWriter, ParquetWriterConfig, RollingPolicy, WriterStats};

// Re-export FinishedFile from blizzard-common
pub use blizzard_common::FinishedFile;
