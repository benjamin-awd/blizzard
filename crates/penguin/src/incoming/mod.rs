//! Incoming file reader for discovering parquet files.
//!
//! This module handles scanning table directories for parquet files placed
//! directly by external writers in partition directories.

mod reader;
mod traits;

pub use reader::{IncomingConfig, IncomingFile, IncomingReader};
pub use traits::FileReader;
