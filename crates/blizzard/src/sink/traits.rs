//! Traits for sink batch writers.
//!
//! Provides extensibility for different output formats through the BatchWriter trait.

use std::collections::HashMap;

use deltalake::arrow::array::RecordBatch;

use blizzard_core::FinishedFile;

/// Error type for batch writer operations.
pub type BatchWriterError = Box<dyn std::error::Error + Send + Sync>;

/// Trait for writers that convert Arrow RecordBatches to output files.
///
/// Implementations can support different output formats (Parquet, ORC, etc.)
/// while presenting a unified interface to the pipeline.
pub trait BatchWriter: Send {
    /// Set partition context for the current and subsequent files.
    ///
    /// When partition values change, the current file may be rolled and a new
    /// file started with the updated partition prefix.
    fn set_partition_context(
        &mut self,
        values: HashMap<String, String>,
    ) -> Result<(), BatchWriterError>;

    /// Write a batch to the current file.
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), BatchWriterError>;

    /// Take finished files without closing the writer.
    fn take_finished_files(&mut self) -> Vec<FinishedFile>;

    /// Close the writer and return all remaining finished files.
    fn close(self: Box<Self>) -> Result<Vec<FinishedFile>, BatchWriterError>;
}
