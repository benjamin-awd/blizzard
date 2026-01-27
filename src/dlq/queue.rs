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

use crate::config::ErrorHandlingConfig;
use crate::error::{DlqError, DlqSerializeSnafu, DlqStorageSnafu, DlqWriteSnafu};
use crate::metrics::events::FailureStage;
use crate::storage::StorageProvider;

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

        // Write to storage (append mode via unique filename per run)
        // For now, we use put which overwrites - in a real implementation
        // we'd want to append or use a new file for each flush
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_dlq_from_config_none_when_no_path() {
        let config = ErrorHandlingConfig {
            max_failures: 0,
            dlq_path: None,
            dlq_storage_options: HashMap::new(),
        };

        let dlq = DeadLetterQueue::from_config(&config).await.unwrap();
        assert!(dlq.is_none());
    }

    #[tokio::test]
    async fn test_dlq_records_failures() {
        let temp_dir = TempDir::new().unwrap();
        let dlq_path = temp_dir.path().to_str().unwrap().to_string();

        let config = ErrorHandlingConfig {
            max_failures: 0,
            dlq_path: Some(dlq_path.clone()),
            dlq_storage_options: HashMap::new(),
        };

        let dlq = DeadLetterQueue::from_config(&config)
            .await
            .unwrap()
            .unwrap();

        // Record some failures
        dlq.record_failure(
            "file1.ndjson.gz",
            "connection timeout",
            FailureStage::Download,
        )
        .await;
        dlq.record_failure("file2.ndjson.gz", "invalid gzip", FailureStage::Decompress)
            .await;
        dlq.record_failure("file3.ndjson.gz", "malformed JSON", FailureStage::Parse)
            .await;

        // Finalize and verify output
        dlq.finalize().await.unwrap();

        let entries: Vec<_> = std::fs::read_dir(&dlq_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        let content = std::fs::read_to_string(entries[0].path()).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test]
    async fn test_dlq_flushes_to_storage() {
        let temp_dir = TempDir::new().unwrap();
        let dlq_path = temp_dir.path().to_str().unwrap().to_string();

        let config = ErrorHandlingConfig {
            max_failures: 0,
            dlq_path: Some(dlq_path.clone()),
            dlq_storage_options: HashMap::new(),
        };

        let dlq = DeadLetterQueue::from_config(&config)
            .await
            .unwrap()
            .unwrap();

        // Record a failure
        dlq.record_failure("test.ndjson.gz", "test error", FailureStage::Parse)
            .await;

        // Finalize to flush
        dlq.finalize().await.unwrap();

        // Verify file was written
        let entries: Vec<_> = std::fs::read_dir(&dlq_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();

        assert_eq!(entries.len(), 1);

        // Read and verify content
        let content = std::fs::read_to_string(entries[0].path()).unwrap();
        assert!(content.contains("test.ndjson.gz"));
        assert!(content.contains("test error"));
        assert!(content.contains("parse"));
    }

    #[tokio::test]
    async fn test_dlq_ndjson_format() {
        let temp_dir = TempDir::new().unwrap();
        let dlq_path = temp_dir.path().to_str().unwrap().to_string();

        let config = ErrorHandlingConfig {
            max_failures: 0,
            dlq_path: Some(dlq_path.clone()),
            dlq_storage_options: HashMap::new(),
        };

        let dlq = DeadLetterQueue::from_config(&config)
            .await
            .unwrap()
            .unwrap();

        // Record multiple failures
        dlq.record_failure("file1.ndjson.gz", "error1", FailureStage::Download)
            .await;
        dlq.record_failure("file2.ndjson.gz", "error2", FailureStage::Parse)
            .await;
        dlq.finalize().await.unwrap();

        // Read the file
        let entries: Vec<_> = std::fs::read_dir(&dlq_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();

        let content = std::fs::read_to_string(entries[0].path()).unwrap();

        // Verify it's valid NDJSON (one JSON object per line)
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);

        // Each line should parse as valid JSON with expected fields
        for line in lines {
            let record: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(record.get("path").is_some());
            assert!(record.get("error").is_some());
            assert!(record.get("stage").is_some());
            assert!(record.get("timestamp").is_some());
        }
    }
}
