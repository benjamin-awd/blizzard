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
use crate::source::NdjsonReader;

pub(super) use download::{DownloadedFile, Downloader};
pub(super) use upload::Uploader;

/// Result of processing a downloaded file.
pub(super) struct ProcessedFile {
    pub path: String,
    pub batches: Vec<RecordBatch>,
    pub total_records: usize,
}

/// Future type for file processing operations.
pub(super) type ProcessFuture =
    Pin<Box<dyn Future<Output = Result<ProcessedFile, PipelineError>> + Send>>;

/// Spawn a blocking task to decompress and parse a downloaded file.
pub(super) fn spawn_read_task(
    downloaded: DownloadedFile,
    reader: Arc<NdjsonReader>,
) -> ProcessFuture {
    let DownloadedFile {
        path,
        compressed_data,
        skip_records,
    } = downloaded;
    let path_for_blocking = path.clone();
    Box::pin(async move {
        let result = tokio::task::spawn_blocking(move || {
            reader.read(compressed_data, skip_records, &path_for_blocking)
        })
        .await
        .context(TaskJoinSnafu)?
        .context(ReaderSnafu)?;

        Ok(ProcessedFile {
            path,
            batches: result.batches,
            total_records: result.total_records,
        })
    })
}
