//! Background tasks for concurrent uploads and downloads.
//!
//! These tasks are spawned by the main pipeline to handle I/O-bound work
//! concurrently while the main loop processes files.

mod download;
mod upload;

use deltalake::arrow::array::RecordBatch;
use snafu::ResultExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::{PipelineError, ReaderSnafu, TaskJoinSnafu};
use crate::source::FileReader;

pub(super) use download::{DownloadTask, DownloadedFile};
pub(super) use upload::UploadTask;

/// Result of processing a downloaded file.
pub(super) struct ProcessedFile {
    pub path: String,
    pub batches: Vec<RecordBatch>,
}

/// Future type for file processing operations.
pub(super) type ProcessFuture =
    Pin<Box<dyn Future<Output = Result<ProcessedFile, PipelineError>> + Send>>;

/// Spawn a blocking task to decompress and parse a downloaded file.
pub(super) fn spawn_read_task(
    downloaded: DownloadedFile,
    reader: Arc<dyn FileReader>,
) -> ProcessFuture {
    let DownloadedFile {
        path,
        compressed_data,
    } = downloaded;
    Box::pin(async move {
        // Move path into spawn_blocking and return it with the result to avoid cloning
        let (path, result) = tokio::task::spawn_blocking(move || {
            let result = reader.read(compressed_data, &path);
            (path, result)
        })
        .await
        .context(TaskJoinSnafu)?;

        let result = result.context(ReaderSnafu)?;

        Ok(ProcessedFile {
            path,
            batches: result.batches,
        })
    })
}
