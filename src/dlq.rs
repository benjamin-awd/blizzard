//! Dead Letter Queue for failed file tracking.
//!
//! Records failed files to a configurable location for later inspection
//! and reprocessing. Writes failures as NDJSON for easy parsing.

use bytes::Bytes;
use chrono::{DateTime, Utc};
use object_store::PutPayload;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::config::ErrorHandlingConfig;
use crate::error::{DlqError, DlqSerializeSnafu, DlqStorageSnafu, DlqWriteSnafu};
use crate::metrics::events::FailureStage;
use crate::storage::StorageProvider;

/// A record representing a failed file in the DLQ.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedFile {
    /// Path to the file that failed.
    pub path: String,
    /// Error message describing the failure.
    pub error: String,
    /// Stage at which the failure occurred.
    pub stage: FailureStage,
    /// Timestamp when the failure was recorded.
    pub timestamp: DateTime<Utc>,
    /// Number of retry attempts (for future use).
    pub retry_count: usize,
}

/// Statistics about failures by stage.
#[derive(Debug, Clone, Default)]
pub struct FailureStats {
    pub download: usize,
    pub decompress: usize,
    pub parse: usize,
    pub upload: usize,
}

impl FailureStats {
    /// Increment the count for a specific stage.
    pub fn increment(&mut self, stage: FailureStage) {
        match stage {
            FailureStage::Download => self.download += 1,
            FailureStage::Decompress => self.decompress += 1,
            FailureStage::Parse => self.parse += 1,
            FailureStage::Upload => self.upload += 1,
        }
    }

    /// Get total failure count.
    pub fn total(&self) -> usize {
        self.download + self.decompress + self.parse + self.upload
    }
}

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

    /// Get current failure statistics.
    pub async fn get_stats(&self) -> FailureStats {
        self.stats.lock().await.clone()
    }

    /// Finalize the DLQ, flushing any remaining records.
    pub async fn finalize(&self) -> Result<FailureStats, DlqError> {
        self.flush().await?;
        let stats = self.get_stats().await;
        info!(
            "DLQ finalized: {} total failures (download={}, decompress={}, parse={}, upload={})",
            stats.total(),
            stats.download,
            stats.decompress,
            stats.parse,
            stats.upload
        );
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    #[test]
    fn test_failure_stats_increment() {
        let mut stats = FailureStats::default();
        stats.increment(FailureStage::Download);
        stats.increment(FailureStage::Download);
        stats.increment(FailureStage::Parse);

        assert_eq!(stats.download, 2);
        assert_eq!(stats.parse, 1);
        assert_eq!(stats.total(), 3);
    }

    #[test]
    fn test_failed_file_serialization() {
        let failed = FailedFile {
            path: "s3://bucket/file.ndjson.gz".to_string(),
            error: "invalid JSON at line 42".to_string(),
            stage: FailureStage::Parse,
            timestamp: Utc::now(),
            retry_count: 0,
        };

        let json = serde_json::to_string(&failed).unwrap();
        assert!(json.contains("parse"));
        assert!(json.contains("s3://bucket/file.ndjson.gz"));
    }

    #[test]
    fn test_failed_file_deserialization() {
        let json = r#"{"path":"s3://bucket/file.ndjson.gz","error":"invalid JSON","stage":"decompress","timestamp":"2025-01-26T10:30:00Z","retry_count":1}"#;
        let failed: FailedFile = serde_json::from_str(json).unwrap();

        assert_eq!(failed.path, "s3://bucket/file.ndjson.gz");
        assert_eq!(failed.error, "invalid JSON");
        assert!(matches!(failed.stage, FailureStage::Decompress));
        assert_eq!(failed.retry_count, 1);
    }

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
    async fn test_dlq_records_and_tracks_failures() {
        let temp_dir = TempDir::new().unwrap();
        let dlq_path = temp_dir.path().to_str().unwrap().to_string();

        let config = ErrorHandlingConfig {
            max_failures: 0,
            dlq_path: Some(dlq_path),
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

        // Check stats
        let stats = dlq.get_stats().await;
        assert_eq!(stats.download, 1);
        assert_eq!(stats.decompress, 1);
        assert_eq!(stats.parse, 1);
        assert_eq!(stats.upload, 0);
        assert_eq!(stats.total(), 3);
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
        let stats = dlq.finalize().await.unwrap();
        assert_eq!(stats.total(), 1);

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

        // Each line should parse as a valid FailedFile
        for line in lines {
            let _: FailedFile = serde_json::from_str(line).unwrap();
        }
    }
}
