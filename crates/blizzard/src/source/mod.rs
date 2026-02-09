//! Source coordinator for reading NDJSON.gz files.
//!
//! Provides a unified interface for listing and reading compressed NDJSON files
//! from various storage backends.

mod compression;
mod inference;
mod listing;
mod reader;

pub use compression::{
    CompressionCodec, CompressionCodecExt, DecompressionError, GzipCodec, NoopCodec, ZstdCodec,
};
pub use inference::infer_schema_from_source;
pub use listing::{list_ndjson_files_above_watermark, list_ndjson_files_with_partition_watermarks};
pub use reader::{NdjsonReader, NdjsonReaderConfig};

// Re-export SourceState from blizzard-core
pub use blizzard_core::types::SourceState;

use std::ops::ControlFlow;

use bytes::Bytes;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;

use crate::error::ReaderError;

/// Trait for file readers that convert raw bytes to Arrow RecordBatches.
///
/// Implementations can support different file formats (NDJSON, CSV, etc.)
/// while presenting a unified interface to the pipeline.
pub trait FileReader: Send + Sync {
    /// Stream parsed batches via callback. Returns total records read.
    ///
    /// The callback returns `ControlFlow::Continue(())` to keep reading or
    /// `ControlFlow::Break(())` to stop early (e.g. when the receiver is dropped).
    ///
    /// # Arguments
    /// * `data` - The raw file data (may be compressed)
    /// * `path` - File path (used for error messages and logging)
    /// * `on_batch` - Callback invoked for each parsed batch
    fn read_batches(
        &self,
        data: Bytes,
        path: &str,
        on_batch: &mut dyn FnMut(RecordBatch) -> ControlFlow<()>,
    ) -> Result<usize, ReaderError>;

    /// Get the schema used by this reader.
    fn schema(&self) -> &SchemaRef;
}
