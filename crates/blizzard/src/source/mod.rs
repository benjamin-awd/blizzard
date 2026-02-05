//! Source coordinator for reading NDJSON.gz files.
//!
//! Provides a unified interface for listing and reading compressed NDJSON files
//! from various storage backends.

mod compression;
mod inference;
mod listing;
mod reader;
mod traits;

pub use compression::{
    CompressionCodec, CompressionCodecExt, DecompressionError, GzipCodec, NoopCodec, ZstdCodec,
};
pub use inference::infer_schema_from_source;
pub use listing::{list_ndjson_files_above_watermark, list_ndjson_files_with_partition_watermarks};
pub use reader::{NdjsonReader, NdjsonReaderConfig};
pub use traits::{FileReader, ReadResult};

// Re-export SourceState from blizzard-core
pub use blizzard_core::types::SourceState;
