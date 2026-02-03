//! Traits for source file readers.
//!
//! Provides extensibility for different file format readers through the FileReader trait.

use bytes::Bytes;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;

use crate::error::ReaderError;

/// Result of reading and parsing a file.
#[derive(Debug)]
pub struct ReadResult {
    /// Parsed record batches.
    pub batches: Vec<RecordBatch>,
    /// Total number of records read.
    pub total_records: usize,
}

/// Trait for file readers that convert raw bytes to Arrow RecordBatches.
///
/// Implementations can support different file formats (NDJSON, CSV, etc.)
/// while presenting a unified interface to the pipeline.
pub trait FileReader: Send + Sync {
    /// Read raw bytes and parse them into record batches.
    ///
    /// # Arguments
    /// * `data` - The raw file data (may be compressed)
    /// * `path` - File path (used for error messages and logging)
    fn read(&self, data: Bytes, path: &str) -> Result<ReadResult, ReaderError>;

    /// Get the schema used by this reader.
    fn schema(&self) -> &SchemaRef;
}
