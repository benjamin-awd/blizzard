//! Pipeline for processing NDJSON files to Parquet staging.
//!
//! This module implements the main processing loop using the PollingProcessor
//! trait from blizzard-common.

mod tasks;

use async_trait::async_trait;
use snafu::ResultExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use blizzard_common::metrics::events::{
    BatchesProcessed, BytesWritten, DecompressionQueueDepth, FileProcessed, FileStatus,
    RecordsProcessed, SourceStateFiles,
};
use blizzard_common::polling::{IterationResult, PollingProcessor, run_polling_loop};
use blizzard_common::storage::{DatePrefixGenerator, list_ndjson_files_with_prefixes};
use blizzard_common::types::SourceState;
use blizzard_common::{StorageProvider, StorageProviderRef, emit, shutdown_signal};

use crate::config::Config;
use crate::dlq::{DeadLetterQueue, FailureTracker};
use crate::error::{AddressParseSnafu, MetricsSnafu, PipelineError, StorageSnafu};
use crate::sink::{ParquetWriter, ParquetWriterConfig};
use crate::source::{NdjsonReader, NdjsonReaderConfig};
use crate::staging::StagingWriter;

use tasks::{Downloader, ProcessFuture, ProcessedFile, spawn_read_task};

/// Statistics from a pipeline run.
#[derive(Debug, Clone, Default)]
pub struct PipelineStats {
    /// Total files processed.
    pub files_processed: usize,
    /// Total records processed.
    pub records_processed: usize,
    /// Total bytes written to Parquet.
    pub bytes_written: usize,
    /// Files written to staging.
    pub staging_files_written: usize,
}

/// Run the pipeline with the given configuration.
pub async fn run_pipeline(config: Config) -> Result<PipelineStats, PipelineError> {
    // Initialize metrics if enabled
    if config.metrics.enabled {
        let addr = config.metrics.address.parse().context(AddressParseSnafu)?;
        blizzard_common::init_metrics(addr).context(MetricsSnafu)?;
        info!("Metrics server started on {}", config.metrics.address);
    }

    // Set up shutdown handling
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_clone.cancel();
    });

    // Create the pipeline processor
    let mut processor = BlizzardProcessor::new(config).await?;

    // Run the polling loop
    let poll_interval = Duration::from_secs(processor.config.source.poll_interval_secs);
    run_polling_loop(&mut processor, poll_interval, shutdown).await?;

    Ok(processor.stats)
}

/// State prepared for a single processing iteration.
struct PreparedState {
    pending_files: Vec<String>,
    skip_counts: HashMap<String, usize>,
}

/// The blizzard file loader pipeline processor.
struct BlizzardProcessor {
    config: Config,
    source_storage: StorageProviderRef,
    staging_writer: StagingWriter,
    reader: Arc<NdjsonReader>,
    source_state: SourceState,
    stats: PipelineStats,
    failure_tracker: FailureTracker,
}

impl BlizzardProcessor {
    async fn new(config: Config) -> Result<Self, PipelineError> {
        // Create source storage provider
        let source_storage = Arc::new(
            StorageProvider::for_url_with_options(
                &config.source.path,
                config.source.storage_options.clone(),
            )
            .await
            .context(StorageSnafu)?,
        );

        // Create staging writer (writes directly to table directory)
        let staging_writer = StagingWriter::new(
            &config.sink.table_uri,
            config.sink.storage_options.clone(),
        )
        .await?;

        // Create NDJSON reader
        let reader_config =
            NdjsonReaderConfig::new(config.source.batch_size, config.source.compression);
        let reader = Arc::new(NdjsonReader::new(
            config.schema.to_arrow_schema(),
            reader_config,
        ));

        // Set up DLQ if configured
        let dlq = DeadLetterQueue::from_config(&config.error_handling).await?;

        Ok(Self {
            failure_tracker: FailureTracker::new(
                config.error_handling.max_failures,
                dlq.map(Arc::new),
            ),
            config,
            source_storage,
            staging_writer,
            reader,
            source_state: SourceState::new(),
            stats: PipelineStats::default(),
        })
    }

    /// Generate date prefixes for partition filtering.
    fn generate_date_prefixes(&self) -> Option<Vec<String>> {
        self.config.source.partition_filter.as_ref().map(|pf| {
            DatePrefixGenerator::new(pf.prefix_template.clone(), pf.lookback).generate_prefixes()
        })
    }

    /// Extract partition values from a source path.
    fn extract_partition_values(&self, path: &str) -> HashMap<String, String> {
        let mut values = HashMap::new();
        for key in &self.config.sink.partition_by {
            // Look for key=value patterns in the path
            let pattern = format!("{}=", key);
            if let Some(idx) = path.find(&pattern) {
                let start = idx + pattern.len();
                let end = path[start..]
                    .find('/')
                    .map(|i| start + i)
                    .unwrap_or(path.len());
                values.insert(key.clone(), path[start..end].to_string());
            }
        }
        values
    }
}

#[async_trait]
impl PollingProcessor for BlizzardProcessor {
    type State = PreparedState;
    type Error = PipelineError;

    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
        // On cold start, we could potentially recover state from staging
        // For now, we start fresh
        if cold_start {
            info!("Cold start - beginning fresh processing");
        }

        // Generate date prefixes for efficient listing
        let prefixes = self.generate_date_prefixes();

        // List source files
        let all_files =
            list_ndjson_files_with_prefixes(&self.source_storage, prefixes.as_deref()).await?;

        // Filter to pending files
        let pending_files = self.source_state.filter_pending_files(all_files);

        if pending_files.is_empty() {
            return Ok(None);
        }

        // Get skip counts for partially processed files
        let skip_counts: HashMap<String, usize> = pending_files
            .iter()
            .filter_map(|f| {
                let skip = self.source_state.records_to_skip(f);
                if skip > 0 {
                    Some((f.clone(), skip))
                } else {
                    None
                }
            })
            .collect();

        info!("Found {} files to process", pending_files.len());

        Ok(Some(PreparedState {
            pending_files,
            skip_counts,
        }))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let PreparedState {
            pending_files,
            skip_counts,
        } = state;

        // Create Parquet writer
        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(self.config.sink.file_size_mb)
            .with_row_group_size_bytes(self.config.sink.row_group_size_bytes)
            .with_compression(self.config.sink.compression);
        let mut parquet_writer =
            ParquetWriter::new(self.config.schema.to_arrow_schema(), writer_config)?;

        // Create downloader
        let shutdown = CancellationToken::new();
        let downloader = Downloader::spawn(
            pending_files,
            skip_counts,
            self.source_storage.clone(),
            shutdown.clone(),
            self.config.source.max_concurrent_files,
        );

        // Process downloaded files
        let result = self
            .process_downloads(downloader, &mut parquet_writer, shutdown)
            .await;

        // Close the writer and get finished files
        let finished_files = parquet_writer.close()?;

        // Write to staging
        if !finished_files.is_empty() {
            self.staging_writer.write_files(&finished_files).await?;
            self.stats.staging_files_written += finished_files.len();

            let bytes: usize = finished_files.iter().map(|f| f.size).sum();
            emit!(BytesWritten {
                bytes: bytes as u64
            });
            self.stats.bytes_written += bytes;
        }

        // Finalize DLQ
        self.failure_tracker.finalize_dlq().await;

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
        let max_in_flight = self.config.source.max_concurrent_files * 2;

        loop {
            emit!(DecompressionQueueDepth {
                count: processing.len()
            });
            emit!(SourceStateFiles {
                count: self.source_state.files.len()
            });

            tokio::select! {
                biased;

                // Handle shutdown
                _ = shutdown.cancelled() => {
                    info!("Shutdown requested during processing");
                    downloader.abort();
                    return Ok(IterationResult::Shutdown);
                }

                // Handle completed processing
                Some(result) = processing.next(), if !processing.is_empty() => {
                    match result {
                        Ok(processed) => {
                            self.handle_processed_file(processed, parquet_writer).await?;
                        }
                        Err(e) => {
                            warn!("File processing failed: {}", e);
                            self.failure_tracker
                                .record_failure(&e.to_string(), FailureStage::Parse)
                                .await?;
                        }
                    }
                }

                // Accept new downloads if under limit
                result = downloader.rx.recv(), if processing.len() < max_in_flight => {
                    match result {
                        Some(Ok(downloaded)) => {
                            let future = spawn_read_task(downloaded, self.reader.clone());
                            processing.push(future);
                        }
                        Some(Err(e)) => {
                            warn!("Download failed: {}", e);
                            self.failure_tracker
                                .record_failure(&e.to_string(), FailureStage::Download)
                                .await?;
                        }
                        None => {
                            // Downloader finished
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
        &mut self,
        processed: ProcessedFile,
        parquet_writer: &mut ParquetWriter,
    ) -> Result<(), PipelineError> {
        let ProcessedFile {
            path,
            batches,
            total_records,
        } = processed;

        // Extract partition values from source path
        let partition_values = self.extract_partition_values(&path);
        parquet_writer.set_partition_context(partition_values)?;

        // Write batches to Parquet
        let batch_count = batches.len();
        for batch in batches {
            parquet_writer.write_batch(&batch)?;
        }

        // Update state
        self.source_state.mark_finished(&path);
        self.stats.files_processed += 1;
        self.stats.records_processed += total_records;

        emit!(FileProcessed {
            status: FileStatus::Success
        });
        emit!(RecordsProcessed {
            count: total_records as u64
        });
        emit!(BatchesProcessed {
            count: batch_count as u64
        });

        let short_name = path.split('/').next_back().unwrap_or(&path);
        info!(
            "Processed {} ({} records, {} batches)",
            short_name, total_records, batch_count
        );

        // Write any finished files to staging
        let finished = parquet_writer.take_finished_files();
        if !finished.is_empty() {
            self.staging_writer.write_files(&finished).await?;
            self.stats.staging_files_written += finished.len();
        }

        Ok(())
    }
}
