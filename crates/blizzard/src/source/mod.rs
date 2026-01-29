//! Source coordinator for reading NDJSON.gz files.
//!
//! Provides a unified interface for listing and reading compressed NDJSON files
//! from various storage backends.

mod reader;

pub use reader::{NdjsonReader, NdjsonReaderConfig, ReadResult};

// Re-export SourceState from blizzard-common
pub use blizzard_common::types::SourceState;
