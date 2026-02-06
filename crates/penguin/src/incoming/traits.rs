//! Traits for file reader abstractions.
//!
//! This module defines the `FileReader` trait that abstracts file discovery
//! and metadata reading operations, enabling dependency inversion and support
//! for multiple file formats.

use async_trait::async_trait;
use std::collections::HashSet;

use blizzard_core::FinishedFile;

use crate::error::IncomingError;

use super::IncomingFile;

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
