//! The BlizzardProcessor - core file processing logic.
//!
//! Implements the PollingProcessor trait for processing NDJSON files to Parquet.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use blizzard_common::metrics::events::{
    BatchesProcessed, BytesWritten, DecompressionQueueDepth, FileProcessed, FileStatus,
    RecordsProcessed, SourceStateFiles,
};
use blizzard_common::polling::{IterationResult, PollingProcessor};
use blizzard_common::storage::DatePrefixGenerator;
use blizzard_common::{StoragePool, StorageProviderRef, emit};

use crate::checkpoint::CheckpointManager;
use crate::config::{PipelineConfig, PipelineKey};
use crate::dlq::{DeadLetterQueue, FailureTracker};
use crate::error::PipelineError;
use crate::sink::{ParquetWriter, ParquetWriterConfig};
use crate::source::{NdjsonReader, NdjsonReaderConfig, infer_schema_from_source};
use crate::staging::TableWriter;

use super::tasks::{Downloader, ProcessFuture, ProcessedFile, spawn_read_task};
use super::tracker::{HashMapTracker, StateTracker, WatermarkTracker};
use super::{PipelineStats, create_storage};

/// Generate date prefixes for partition filtering based on pipeline config.
fn generate_date_prefixes(config: &PipelineConfig) -> Option<Vec<String>> {
    config
        .source
        .partition_filter
        .as_ref()
        .map(|pf| DatePrefixGenerator::new(&pf.prefix_template, pf.lookback).generate_prefixes())
}

/// Extract a partition value from a path for a given key.
///
/// Looks for `key=value` pattern and extracts the value (up to the next `/` or end of string).
fn extract_partition_value(path: &str, key: &str) -> Option<String> {
    let pattern = format!("{}=", key);
    let start = path.find(&pattern)? + pattern.len();
    let rest = &path[start..];
    let end = rest.find('/').unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

/// State prepared for a single processing iteration.
pub(super) struct PreparedState {
    pub pending_files: Vec<String>,
}

/// The blizzard file loader pipeline processor.
///
/// Each pipeline runs its own `BlizzardProcessor` instance.
pub(super) struct BlizzardProcessor {
    /// Identifier for this pipeline (used in logging and metrics).
    pipeline_key: PipelineKey,
    /// Configuration for this specific pipeline.
    pipeline_config: PipelineConfig,
    /// Storage provider for reading source files.
    source_storage: StorageProviderRef,
    /// Writer for parquet files directly to table directory.
    table_writer: TableWriter,
    /// Arrow schema (from config or inferred).
    schema: deltalake::arrow::datatypes::SchemaRef,
    /// NDJSON reader with schema validation.
    reader: Arc<NdjsonReader>,
    /// Tracks which source files have been processed.
    state_tracker: Box<dyn StateTracker>,
    /// Statistics for this pipeline's run.
    pub stats: PipelineStats,
    /// Tracks failures and manages DLQ.
    failure_tracker: FailureTracker,
    /// Shutdown signal for graceful termination.
    shutdown: CancellationToken,
}

impl BlizzardProcessor {
    pub async fn new(
        pipeline_key: PipelineKey,
        pipeline_config: PipelineConfig,
        storage_pool: Option<Arc<StoragePool>>,
        shutdown: CancellationToken,
    ) -> Result<Self, PipelineError> {
        // Create source storage provider - use pooled if available
        let source_storage = create_storage(
            &storage_pool,
            &pipeline_config.source.path,
            pipeline_config.source.storage_options.clone(),
        )
        .await?;

        // Create table writer (writes parquet directly to table directory)
        let table_writer = TableWriter::new(
            &pipeline_config.sink.table_uri,
            pipeline_config.sink.storage_options.clone(),
            pipeline_key.id().to_string(),
        )
        .await?;

        // Create state tracker based on configuration
        let state_tracker: Box<dyn StateTracker> = if pipeline_config.source.use_watermark {
            // Checkpoint manager needs its own storage provider (not pooled)
            let table_storage = create_storage(
                &None,
                &pipeline_config.sink.table_uri,
                pipeline_config.sink.storage_options.clone(),
            )
            .await?;
            let checkpoint_manager =
                CheckpointManager::new(table_storage, pipeline_key.id().to_string());
            Box::new(WatermarkTracker::new(checkpoint_manager))
        } else {
            Box::new(HashMapTracker::new())
        };

        // Get schema - either from explicit config or by inference
        let schema = if pipeline_config.schema.should_infer() {
            let prefixes = generate_date_prefixes(&pipeline_config);
            infer_schema_from_source(
                &source_storage,
                pipeline_config.source.compression,
                prefixes.as_deref(),
                pipeline_key.as_ref(),
            )
            .await?
        } else {
            pipeline_config.schema.to_arrow_schema()
        };

        // Create NDJSON reader
        let reader_config = NdjsonReaderConfig::new(
            pipeline_config.source.batch_size,
            pipeline_config.source.compression,
        );
        let reader = Arc::new(NdjsonReader::new(
            schema.clone(),
            reader_config,
            pipeline_key.id().to_string(),
        ));

        // Set up DLQ if configured
        let dlq = DeadLetterQueue::from_config(&pipeline_config.error_handling).await?;

        Ok(Self {
            failure_tracker: FailureTracker::new(
                pipeline_config.error_handling.max_failures,
                dlq.map(Arc::new),
                pipeline_key.id().to_string(),
            ),
            pipeline_key,
            pipeline_config,
            source_storage,
            table_writer,
            schema,
            reader,
            state_tracker,
            stats: PipelineStats::default(),
            shutdown,
        })
    }

    /// Extract partition values from a source path.
    ///
    /// Looks for `key=value` patterns in the path for each configured partition column.
    fn extract_partition_values(&self, path: &str) -> HashMap<String, String> {
        self.pipeline_config
            .sink
            .partition_by
            .as_ref()
            .map(|p| p.partition_columns())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|key| extract_partition_value(path, &key).map(|value| (key, value)))
            .collect()
    }
}

#[async_trait]
impl PollingProcessor for BlizzardProcessor {
    type State = PreparedState;
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        let prefixes = generate_date_prefixes(&self.pipeline_config);

        if cold_start {
            match self.state_tracker.init().await? {
                Some(msg) => info!(target = %self.pipeline_key, "{}", msg),
                None => info!(
                    target = %self.pipeline_key,
                    mode = self.state_tracker.mode_name(),
                    "Cold start - beginning fresh processing"
                ),
            }
        }

        let pending_files = self
            .state_tracker
            .list_pending(
                &self.source_storage,
                prefixes.as_deref(),
                self.pipeline_key.as_ref(),
            )
            .await?;

        if pending_files.is_empty() {
            return Ok(None);
        }

        info!(target = %self.pipeline_key, files = pending_files.len(), "Found files to process");

        Ok(Some(PreparedState { pending_files }))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let PreparedState { pending_files } = state;

        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(self.pipeline_config.sink.file_size_mb)
            .with_row_group_size_bytes(self.pipeline_config.sink.row_group_size_bytes)
            .with_compression(self.pipeline_config.sink.compression);
        let mut parquet_writer = ParquetWriter::new(
            self.schema.clone(),
            writer_config,
            self.pipeline_key.id().to_string(),
        )?;

        let downloader = Downloader::spawn(
            pending_files,
            self.source_storage.clone(),
            self.shutdown.clone(),
            self.pipeline_config.source.max_concurrent_files,
            self.pipeline_key.id().to_string(),
        );

        let result = self
            .process_downloads(downloader, &mut parquet_writer, self.shutdown.clone())
            .await;

        self.finalize_iteration(parquet_writer).await?;

        result
    }
}

impl BlizzardProcessor {
    async fn process_downloads(
        &mut self,
        mut downloader: Downloader,
        parquet_writer: &mut ParquetWriter,
        shutdown: CancellationToken,
    ) -> Result<IterationResult, PipelineError> {
        use blizzard_common::metrics::events::FailureStage;
        use futures::StreamExt;
        use futures::stream::FuturesUnordered;

        let mut processing: FuturesUnordered<ProcessFuture> = FuturesUnordered::new();
        let max_in_flight = self.pipeline_config.source.max_concurrent_files * 2;

        loop {
            emit!(DecompressionQueueDepth {
                count: processing.len(),
                target: self.pipeline_key.id().to_string(),
            });
            emit!(SourceStateFiles {
                count: self.state_tracker.tracked_count(),
                target: self.pipeline_key.id().to_string(),
            });

            tokio::select! {
                biased;

                _ = shutdown.cancelled() => {
                    info!(target = %self.pipeline_key, "Shutdown requested during processing");
                    downloader.abort();
                    return Ok(IterationResult::Shutdown);
                }

                Some(result) = processing.next(), if !processing.is_empty() => {
                    match result {
                        Ok(processed) => {
                            self.handle_processed_file(processed, parquet_writer).await?;
                        }
                        Err(e) => {
                            warn!(target = %self.pipeline_key, error = %e, "File processing failed");
                            self.failure_tracker
                                .record_failure(&e.to_string(), FailureStage::Parse)
                                .await?;
                        }
                    }
                }

                result = downloader.rx.recv(), if processing.len() < max_in_flight => {
                    match result {
                        Some(Ok(downloaded)) => {
                            let future = spawn_read_task(downloaded, self.reader.clone());
                            processing.push(future);
                        }
                        Some(Err(e)) => {
                            warn!(target = %self.pipeline_key, error = %e, "Download failed");
                            self.failure_tracker
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

    async fn finalize_iteration(
        &mut self,
        parquet_writer: ParquetWriter,
    ) -> Result<(), PipelineError> {
        let finished_files = parquet_writer.close()?;

        if !finished_files.is_empty() {
            info!(
                target = %self.pipeline_key,
                files = finished_files.len(),
                "Writing parquet files to table"
            );
            self.table_writer.write_files(&finished_files).await?;
            self.stats.parquet_files_written += finished_files.len();

            let bytes: usize = finished_files.iter().map(|f| f.size).sum();
            emit!(BytesWritten {
                bytes: bytes as u64,
                target: self.pipeline_key.id().to_string(),
            });
            self.stats.bytes_written += bytes;
        }

        self.failure_tracker.finalize_dlq().await;

        if let Err(e) = self.state_tracker.save().await {
            warn!(
                target = %self.pipeline_key,
                error = %e,
                "Failed to save state"
            );
        } else {
            debug!(
                target = %self.pipeline_key,
                mode = self.state_tracker.mode_name(),
                "Saved state"
            );
        }

        Ok(())
    }

    async fn handle_processed_file(
        &mut self,
        processed: ProcessedFile,
        parquet_writer: &mut ParquetWriter,
    ) -> Result<(), PipelineError> {
        let ProcessedFile {
            path,
            batches,
            total_records,
        } = processed;

        let partition_values = self.extract_partition_values(&path);
        parquet_writer.set_partition_context(partition_values)?;

        let batch_count = batches.len();
        for batch in batches {
            parquet_writer.write_batch(&batch)?;
        }

        self.state_tracker.mark_processed(&path);
        self.stats.files_processed += 1;
        self.stats.records_processed += total_records;

        emit!(FileProcessed {
            status: FileStatus::Success,
            target: self.pipeline_key.id().to_string(),
        });
        emit!(RecordsProcessed {
            count: total_records as u64,
            target: self.pipeline_key.id().to_string(),
        });
        emit!(BatchesProcessed {
            count: batch_count as u64,
            target: self.pipeline_key.id().to_string(),
        });

        let short_name = path.split('/').next_back().unwrap_or(&path);
        debug!(
            target = %self.pipeline_key,
            file = short_name,
            records = total_records,
            batches = batch_count,
            "Processed file"
        );

        let finished = parquet_writer.take_finished_files();
        if !finished.is_empty() {
            self.table_writer.write_files(&finished).await?;
            self.stats.parquet_files_written += finished.len();
        }

        Ok(())
    }
}
