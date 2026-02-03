//! The Processor - core file processing logic.
//!
//! Implements the PollingProcessor trait for processing NDJSON files to Parquet.

use std::sync::Arc;

use async_trait::async_trait;
use deltalake::arrow::datatypes::SchemaRef;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use blizzard_common::polling::{IterationResult, PollingProcessor};
use blizzard_common::storage::DatePrefixGenerator;
use blizzard_common::{PartitionExtractor, StoragePoolRef, StorageProviderRef};

use super::create_storage;
use super::download::Downloader;
use super::tasks::DownloadTask;
use super::tracker::{HashMapTracker, StateTracker, WatermarkTracker};
use crate::checkpoint::CheckpointManager;
use crate::config::{PipelineConfig, PipelineKey};
use crate::dlq::{DeadLetterQueue, FailureTracker};
use crate::error::PipelineError;
use crate::sink::{ParquetWriterConfig, SinkWriter, StorageWriter};
use crate::source::{NdjsonReader, NdjsonReaderConfig, infer_schema_from_source};

/// Generate date prefixes for partition filtering based on pipeline config.
fn generate_date_prefixes(config: &PipelineConfig) -> Option<Vec<String>> {
    config
        .source
        .partition_filter
        .as_ref()
        .map(|pf| DatePrefixGenerator::new(&pf.prefix_template, pf.lookback).generate_prefixes())
}

/// Runtime dependencies shared across processing iterations.
///
/// Groups the components needed for reading source files and writing output,
/// reducing field count in the main processor struct.
pub(super) struct ProcessorContext {
    /// Storage provider for reading source files.
    pub source_storage: StorageProviderRef,
    /// Arrow schema (from config or inferred).
    pub schema: SchemaRef,
    /// NDJSON reader with schema validation.
    pub reader: Arc<NdjsonReader>,
    /// Storage writer for persisting parquet files (cloned per iteration).
    pub storage_writer: StorageWriter,
    /// Extracts partition values from source paths.
    pub partition_extractor: PartitionExtractor,
}

/// Encapsulates the state and logic for a single processing iteration.
///
/// Created fresh for each iteration, isolating per-iteration components
/// (writer, downloader, download task) from the long-lived processor state.
struct Iteration {
    writer: SinkWriter,
    downloader: Downloader,
    download_task: DownloadTask,
}

impl Iteration {
    /// Create a new iteration with all per-iteration components.
    fn new(
        pending_files: Vec<String>,
        ctx: &ProcessorContext,
        config: &PipelineConfig,
        shutdown: CancellationToken,
        pipeline_key: &str,
    ) -> Result<Self, PipelineError> {
        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(config.sink.file_size_mb)
            .with_row_group_size_bytes(config.sink.row_group_size_bytes)
            .with_compression(config.sink.compression);

        let writer = SinkWriter::new(
            ctx.schema.clone(),
            writer_config,
            ctx.storage_writer.clone(),
            ctx.partition_extractor.clone(),
            pipeline_key.to_string(),
        )?;

        let download_task = DownloadTask::spawn(
            pending_files,
            ctx.source_storage.clone(),
            shutdown,
            config.source.max_concurrent_files,
            pipeline_key.to_string(),
        );

        let max_in_flight = config.source.max_concurrent_files * 2;
        let downloader =
            Downloader::new(ctx.reader.clone(), max_in_flight, pipeline_key.to_string());

        Ok(Self {
            writer,
            downloader,
            download_task,
        })
    }

    /// Run the iteration: download, parse, and write files.
    async fn run(
        mut self,
        state_tracker: &mut dyn StateTracker,
        failure_tracker: &mut FailureTracker,
        shutdown: CancellationToken,
    ) -> Result<(IterationResult, SinkWriter), PipelineError> {
        let result = self
            .downloader
            .run(
                self.download_task,
                &mut self.writer,
                state_tracker,
                failure_tracker,
                shutdown,
            )
            .await;

        // Return the writer so the processor can finalize it
        Ok((result?, self.writer))
    }
}

/// The blizzard file loader pipeline processor.
///
/// Each pipeline runs its own `Processor` instance.
pub(super) struct Processor {
    /// Identifier for this pipeline (used in logging and metrics).
    pipeline_key: PipelineKey,
    /// Configuration for this specific pipeline.
    pipeline_config: PipelineConfig,
    /// Runtime dependencies for reading and writing.
    ctx: ProcessorContext,
    /// Tracks which source files have been processed.
    state_tracker: Box<dyn StateTracker>,
    /// Tracks failures and manages DLQ.
    failure_tracker: FailureTracker,
    /// Shutdown signal for graceful termination.
    shutdown: CancellationToken,
}

impl Processor {
    pub async fn new(
        pipeline_key: PipelineKey,
        pipeline_config: PipelineConfig,
        storage_pool: Option<StoragePoolRef>,
        shutdown: CancellationToken,
    ) -> Result<Self, PipelineError> {
        // Create source storage provider - use pooled if available
        let source_storage = create_storage(&storage_pool, &pipeline_config.source).await?;

        // Create storage writer (writes parquet to destination)
        let storage_writer = StorageWriter::new(
            &pipeline_config.sink.table_uri,
            pipeline_config.sink.storage_options.clone(),
            pipeline_key.id().to_string(),
        )
        .await?;

        // Create state tracker based on configuration
        let state_tracker: Box<dyn StateTracker> = if pipeline_config.source.use_watermark {
            // Checkpoint manager needs its own storage provider (not pooled)
            let checkpoint_storage = create_storage(&None, &pipeline_config.sink).await?;
            let checkpoint_manager =
                CheckpointManager::new(checkpoint_storage, pipeline_key.id().to_string());
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

        // Create partition extractor from config
        let partition_columns = pipeline_config
            .sink
            .partition_by
            .as_ref()
            .map(|p| p.partition_columns())
            .unwrap_or_default();
        let partition_extractor = PartitionExtractor::new(partition_columns);

        // Build the processor context
        let ctx = ProcessorContext {
            source_storage,
            schema,
            reader,
            storage_writer,
            partition_extractor,
        };

        Ok(Self {
            failure_tracker: FailureTracker::new(
                pipeline_config.error_handling.max_failures,
                dlq.map(Arc::new),
                pipeline_key.id().to_string(),
            ),
            pipeline_key,
            pipeline_config,
            ctx,
            state_tracker,
            shutdown,
        })
    }
}

#[async_trait]
impl PollingProcessor for Processor {
    type State = Vec<String>;
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
                &self.ctx.source_storage,
                prefixes.as_deref(),
                self.pipeline_key.as_ref(),
            )
            .await?;

        if pending_files.is_empty() {
            return Ok(None);
        }

        info!(target = %self.pipeline_key, files = pending_files.len(), "Found files to process");

        Ok(Some(pending_files))
    }

    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error> {
        let iteration = Iteration::new(
            state,
            &self.ctx,
            &self.pipeline_config,
            self.shutdown.clone(),
            self.pipeline_key.id(),
        )?;

        let (result, writer) = iteration
            .run(
                self.state_tracker.as_mut(),
                &mut self.failure_tracker,
                self.shutdown.clone(),
            )
            .await?;

        self.finalize_iteration(writer).await?;

        Ok(result)
    }
}

impl Processor {
    async fn finalize_iteration(&mut self, writer: SinkWriter) -> Result<(), PipelineError> {
        writer.finalize().await?;

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
}
