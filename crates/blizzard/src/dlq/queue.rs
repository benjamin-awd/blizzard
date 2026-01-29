//! Dead Letter Queue implementation.
//!
//! Records failed files to a configurable location for later inspection
//! and reprocessing. Writes failures as NDJSON for easy parsing.

use bytes::Bytes;
use chrono::Utc;
use object_store::PutPayload;
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use blizzard_common::StorageProvider;
use blizzard_common::config::ErrorHandlingConfig;
use blizzard_common::error::{DlqError, DlqSerializeSnafu, DlqStorageSnafu, DlqWriteSnafu};
use blizzard_common::metrics::events::FailureStage;

use super::types::{FailedFile, FailureStats};

/// Dead Letter Queue for recording failed files.
///
/// Writes failed file records as NDJSON to a configured storage location.
/// Each run creates a new file with a timestamp suffix.
pub struct DeadLetterQueue {
    storage: Arc<StorageProvider>,
    filename: String,
    buffer: Mutex<Vec<FailedFile>>,
    stats: Mutex<FailureStats>,
    buffer_size: usize,
}

impl DeadLetterQueue {
    /// Create a new DLQ from configuration.
    ///
    /// Returns `None` if no DLQ path is configured.
    pub async fn from_config(config: &ErrorHandlingConfig) -> Result<Option<Self>, DlqError> {
        let Some(dlq_path) = &config.dlq_path else {
            return Ok(None);
        };

        let storage =
            StorageProvider::for_url_with_options(dlq_path, config.dlq_storage_options.clone())
                .await
                .context(DlqStorageSnafu)?;

        // Generate a unique filename for this run
        let timestamp = Utc::now().format("%Y%m%d-%H%M%S");
        let filename = format!("failures-{}.ndjson", timestamp);

        info!("DLQ enabled: {}/{}", dlq_path, filename);

        Ok(Some(Self {
            storage: Arc::new(storage),
            filename,
            buffer: Mutex::new(Vec::new()),
            stats: Mutex::new(FailureStats::default()),
            buffer_size: 100, // Flush every 100 records
        }))
    }

    /// Record a file failure.
    pub async fn record_failure(&self, path: &str, error: &str, stage: FailureStage) {
        let failed = FailedFile {
            path: path.to_string(),
            error: error.to_string(),
            stage,
            timestamp: Utc::now(),
            retry_count: 0,
        };

        debug!(
            "Recording DLQ failure: {} at stage {:?}",
            path,
            stage.as_str()
        );

        // Update stats
        {
            let mut stats = self.stats.lock().await;
            stats.increment(stage);
        }

        // Add to buffer
        let should_flush = {
            let mut buffer = self.buffer.lock().await;
            buffer.push(failed);
            buffer.len() >= self.buffer_size
        };

        // Flush if buffer is full
        if should_flush && let Err(e) = self.flush().await {
            error!("Failed to flush DLQ: {}", e);
        }
    }

    /// Flush buffered records to storage.
    pub async fn flush(&self) -> Result<(), DlqError> {
        let records = {
            let mut buffer = self.buffer.lock().await;
            if buffer.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *buffer)
        };

        let count = records.len();
        debug!("Flushing {} DLQ records", count);

        // Serialize records to NDJSON
        let mut ndjson = String::new();
        for record in &records {
            let line = serde_json::to_string(record).context(DlqSerializeSnafu)?;
            ndjson.push_str(&line);
            ndjson.push('\n');
        }

        // Write to storage
        let path = object_store::path::Path::from(self.filename.as_str());
        let payload = PutPayload::from(Bytes::from(ndjson));
        self.storage
            .put_payload(&path, payload)
            .await
            .context(DlqWriteSnafu)?;

        info!("Flushed {} records to DLQ", count);
        Ok(())
    }

    /// Finalize the DLQ, flushing any remaining records.
    pub async fn finalize(&self) -> Result<(), DlqError> {
        self.flush().await?;
        let stats = self.stats.lock().await;
        info!(
            "DLQ finalized: {} total failures (download={}, decompress={}, parse={}, upload={})",
            stats.total(),
            stats.download,
            stats.decompress,
            stats.parse,
            stats.upload
        );
        Ok(())
    }
}
