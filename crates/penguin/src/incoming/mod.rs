//! Incoming file reader for discovering parquet files.
//!
//! This module handles scanning table directories for parquet files placed
//! directly by external writers in partition directories.

mod reader;

pub use reader::{IncomingConfig, IncomingFile, IncomingReader};

use std::collections::HashSet;

use async_trait::async_trait;

use blizzard_core::FinishedFile;

use crate::error::IncomingError;

/// Trait for file readers that discover and read file metadata.
///
/// This abstraction allows the pipeline to work with different file formats
/// and discovery mechanisms without depending on concrete implementations.
#[async_trait]
pub trait FileReader: Send + Sync {
    /// List uncommitted files above the watermark.
    ///
    /// # Arguments
    /// * `watermark` - Optional watermark path; only files after this are returned
    /// * `committed_paths` - Set of already committed file paths to exclude
    /// * `cold_start` - Whether this is the very first poll (controls log level for cold start scans)
    ///
    /// # Returns
    /// A list of uncommitted files, or an error if listing fails.
    async fn list_uncommitted_files(
        &self,
        watermark: Option<&str>,
        committed_paths: &HashSet<String>,
        cold_start: bool,
    ) -> Result<Vec<IncomingFile>, IncomingError>;

    /// Read metadata from a file and create a FinishedFile.
    ///
    /// Extracts record count, file size, and partition values from the file.
    ///
    /// # Arguments
    /// * `incoming` - The incoming file to read metadata from
    ///
    /// # Returns
    /// A FinishedFile with metadata, or an error if reading fails.
    async fn read_file_metadata(
        &self,
        incoming: &IncomingFile,
    ) -> Result<FinishedFile, IncomingError>;

    /// Get the table name/identifier for logging.
    fn table_name(&self) -> &str;
}
