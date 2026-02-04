//! Background tasks for concurrent uploads and downloads.
//!
//! These tasks are spawned by the main pipeline to handle I/O-bound work
//! concurrently while the main loop processes files.

mod download;
mod upload;

use deltalake::arrow::array::RecordBatch;
use indexmap::IndexMap;
use snafu::ResultExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tracing::warn;

use crate::error::{PipelineError, ReaderSnafu, TaskJoinSnafu};
use crate::source::FileReader;

pub(super) use download::{DownloadTask, DownloadedFile};
pub(super) use upload::UploadTask;

/// Result of processing a downloaded file.
pub(super) struct ProcessedFile {
    /// Name of the source this file came from.
    pub source_name: String,
    /// Path to the file within the source.
    pub path: String,
    /// Parsed record batches.
    pub batches: Vec<RecordBatch>,
}

/// Future type for file processing operations.
pub(super) type ProcessFuture =
    Pin<Box<dyn Future<Output = Result<ProcessedFile, PipelineError>> + Send>>;

/// Spawn a blocking task to decompress and parse a downloaded file.
///
/// Selects the appropriate reader based on source name (compression may differ).
pub(super) fn spawn_read_task(
    downloaded: DownloadedFile,
    readers: &IndexMap<String, Arc<dyn FileReader>>,
) -> ProcessFuture {
    let DownloadedFile {
        source_name,
        path,
        compressed_data,
    } = downloaded;

    // Get the reader for this source
    let reader = match readers.get(&source_name) {
        Some(r) => r.clone(),
        None => {
            warn!(
                "No reader for source '{}', using first available reader",
                source_name
            );
            readers.values().next().unwrap().clone()
        }
    };

    Box::pin(async move {
        // Move path and source_name into spawn_blocking and return them with the result
        let (source_name, path, result) = tokio::task::spawn_blocking(move || {
            let result = reader.read(compressed_data, &path);
            (source_name, path, result)
        })
        .await
        .context(TaskJoinSnafu)?;

        let result = result.context(ReaderSnafu)?;

        Ok(ProcessedFile {
            source_name,
            path,
            batches: result.batches,
        })
    })
}
