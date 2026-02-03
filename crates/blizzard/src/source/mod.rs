//! Source coordinator for reading NDJSON.gz files.
//!
//! Provides a unified interface for listing and reading compressed NDJSON files
//! from various storage backends.

mod inference;
mod listing;
mod reader;

pub use inference::infer_schema_from_source;
pub use listing::list_ndjson_files_above_watermark;
pub use reader::{NdjsonReader, NdjsonReaderConfig, ReadResult};

// Re-export SourceState from blizzard-common
pub use blizzard_common::types::SourceState;
