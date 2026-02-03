//! Download processing orchestration.
//!
//! Coordinates the download -> parse -> write pipeline with backpressure
//! and failure handling.

use std::sync::Arc;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use blizzard_core::emit;
use blizzard_core::metrics::events::{
    DecompressionQueueDepth, FailureStage, FileProcessed, FileStatus, SourceStateFiles,
};
use blizzard_core::polling::IterationResult;

use super::sink_writer::SinkWriter;
use super::tasks::{DownloadTask, ProcessFuture, ProcessedFile, spawn_read_task};
use super::tracker::StateTracker;
use crate::dlq::FailureTracker;
use crate::error::PipelineError;
use crate::source::NdjsonReader;

/// Orchestrates the download -> parse -> write pipeline.
///
/// Manages concurrent downloads and parsing with backpressure,
/// coordinating between the file downloader, reader, and sink writer.
pub(super) struct Downloader {
    reader: Arc<NdjsonReader>,
    max_in_flight: usize,
    pipeline_key: String,
}

impl Downloader {
    pub fn new(reader: Arc<NdjsonReader>, max_in_flight: usize, pipeline_key: String) -> Self {
        Self {
            reader,
            max_in_flight,
            pipeline_key,
        }
    }

    /// Run the download processing loop.
    ///
    /// Consumes downloads from the downloader, spawns read tasks, writes results
    /// to the sink, and tracks state/failures.
    pub async fn run(
        &self,
        mut download_task: DownloadTask,
        sink_writer: &mut SinkWriter,
        state_tracker: &mut dyn StateTracker,
        failure_tracker: &mut FailureTracker,
        shutdown: CancellationToken,
    ) -> Result<IterationResult, PipelineError> {
        let mut processing: FuturesUnordered<ProcessFuture> = FuturesUnordered::new();

        loop {
            emit!(DecompressionQueueDepth {
                count: processing.len(),
                target: self.pipeline_key.clone(),
            });
            emit!(SourceStateFiles {
                count: state_tracker.tracked_count(),
                target: self.pipeline_key.clone(),
            });

            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    info!(target = %self.pipeline_key, "Shutdown requested during processing");
                    download_task.abort();
                    return Ok(IterationResult::Shutdown);
                }

                Some(result) = processing.next(), if !processing.is_empty() => {
                    self.handle_processed_file(
                        result,
                        sink_writer,
                        state_tracker,
                        failure_tracker,
                    ).await?;
                }

                result = download_task.rx.recv(), if processing.len() < self.max_in_flight => {
                    match result {
                        Some(Ok(downloaded)) => {
                            let future = spawn_read_task(downloaded, self.reader.clone());
                            processing.push(future);
                        }
                        Some(Err(e)) => {
                            warn!(target = %self.pipeline_key, error = %e, "Download failed");
                            failure_tracker
                                .record_failure(&e.to_string(), FailureStage::Download)
                                .await?;
                        }
                        None => {
                            if processing.is_empty() {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(IterationResult::ProcessedItems)
    }

    async fn handle_processed_file(
        &self,
        result: Result<ProcessedFile, PipelineError>,
        sink_writer: &mut SinkWriter,
        state_tracker: &mut dyn StateTracker,
        failure_tracker: &mut FailureTracker,
    ) -> Result<(), PipelineError> {
        match result {
            Ok(ProcessedFile { path, batches }) => {
                sink_writer.write_file_batches(&path, batches).await?;
                state_tracker.mark_processed(&path);
                emit!(FileProcessed {
                    status: FileStatus::Success,
                    target: self.pipeline_key.clone(),
                });
            }
            Err(e) => {
                warn!(target = %self.pipeline_key, error = %e, "File processing failed");
                failure_tracker
                    .record_failure(&e.to_string(), FailureStage::Parse)
                    .await?;
            }
        }
        Ok(())
    }
}
