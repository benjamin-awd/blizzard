//! Internal events for blizzard metrics emission.
//!
//! Each event struct represents a measurable occurrence in the blizzard pipeline.
//! Events implement the `InternalEvent` trait which emits the corresponding
//! Prometheus metric.
//!
//! ## Target Labels
//!
//! For multi-target deployments, metrics include a `target` label to enable
//! per-target observability (e.g., `"orderbooks"`, `"trades"`).

use metrics::{counter, gauge, histogram};
use std::time::Duration;
use tracing::trace;

/// Trait for internal events that can be emitted as metrics.
pub trait InternalEvent {
    /// Emit this event as a metric.
    fn emit(self);
}

/// Event emitted when records are processed through the pipeline.
pub struct RecordsProcessed {
    pub count: u64,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for RecordsProcessed {
    fn emit(self) {
        trace!(count = self.count, target = %self.target, "Records processed");
        counter!("blizzard_records_processed_total", "target" => self.target).increment(self.count);
    }
}

/// Event emitted when compressed bytes are read from source.
pub struct BytesRead {
    pub bytes: u64,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for BytesRead {
    fn emit(self) {
        trace!(bytes = self.bytes, target = %self.target, "Bytes read");
        counter!("blizzard_bytes_read_total", "target" => self.target).increment(self.bytes);
    }
}

/// Event emitted when bytes are written to Parquet files.
pub struct BytesWritten {
    pub bytes: u64,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for BytesWritten {
    fn emit(self) {
        trace!(bytes = self.bytes, target = %self.target, "Bytes written");
        counter!("blizzard_bytes_written_total", "target" => self.target).increment(self.bytes);
    }
}

/// Status of a processed file.
#[derive(Debug, Clone, Copy)]
pub enum FileStatus {
    Success,
    Skipped,
    Failed,
}

impl FileStatus {
    fn as_str(&self) -> &'static str {
        match self {
            FileStatus::Success => "success",
            FileStatus::Skipped => "skipped",
            FileStatus::Failed => "failed",
        }
    }
}

/// Stage at which a file failure occurred.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FailureStage {
    Download,
    Decompress,
    Parse,
    Upload,
}

impl FailureStage {
    pub fn as_str(&self) -> &'static str {
        match self {
            FailureStage::Download => "download",
            FailureStage::Decompress => "decompress",
            FailureStage::Parse => "parse",
            FailureStage::Upload => "upload",
        }
    }
}

/// Event emitted when a file fails processing.
pub struct FileFailed {
    pub stage: FailureStage,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for FileFailed {
    fn emit(self) {
        trace!(stage = self.stage.as_str(), target = %self.target, "File failed");
        counter!("blizzard_files_failed_total", "stage" => self.stage.as_str(), "target" => self.target)
            .increment(1);
    }
}

/// Event emitted when an input file is processed.
pub struct FileProcessed {
    pub status: FileStatus,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for FileProcessed {
    fn emit(self) {
        trace!(status = self.status.as_str(), target = %self.target, "File processed");
        counter!("blizzard_files_processed_total", "status" => self.status.as_str(), "target" => self.target)
            .increment(1);
    }
}

/// Event emitted when files are discovered during source listing.
pub struct FilesDiscovered {
    pub count: u64,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for FilesDiscovered {
    fn emit(self) {
        trace!(count = self.count, target = %self.target, "Files discovered");
        counter!("blizzard_files_discovered_total", "target" => self.target).increment(self.count);
    }
}

/// Event emitted to track the current number of files pending processing.
///
/// This gauge shows files that have been discovered but not yet processed,
/// providing visibility into the processing backlog.
pub struct PendingFiles {
    pub count: usize,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for PendingFiles {
    fn emit(self) {
        trace!(count = self.count, target = %self.target, "Pending files");
        gauge!("blizzard_pending_files", "target" => self.target).set(self.count as f64);
    }
}

/// Event emitted when an Arrow RecordBatch is created.
pub struct BatchesProcessed {
    pub count: u64,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for BatchesProcessed {
    fn emit(self) {
        trace!(count = self.count, target = %self.target, "Batches processed");
        counter!("blizzard_batches_processed_total", "target" => self.target).increment(self.count);
    }
}

// ============================================================================
// Histogram events for timing
// ============================================================================

/// Event emitted when a file download completes.
pub struct FileDownloadCompleted {
    pub duration: Duration,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for FileDownloadCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            target = %self.target,
            "File download completed"
        );
        histogram!("blizzard_file_download_duration_seconds", "target" => self.target)
            .record(self.duration.as_secs_f64());
    }
}

/// Event emitted when file decompression completes.
pub struct FileDecompressionCompleted {
    pub duration: Duration,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for FileDecompressionCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            target = %self.target,
            "File decompression completed"
        );
        histogram!("blizzard_file_decompression_duration_seconds", "target" => self.target)
            .record(self.duration.as_secs_f64());
    }
}

/// Event emitted when a Parquet file write completes.
pub struct ParquetWriteCompleted {
    pub duration: Duration,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for ParquetWriteCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            target = %self.target,
            "Parquet write completed"
        );
        histogram!("blizzard_parquet_write_duration_seconds", "target" => self.target)
            .record(self.duration.as_secs_f64());
    }
}

// ============================================================================
// Gauge events for concurrency and backpressure
// ============================================================================

/// Event emitted when the number of active downloads changes.
pub struct ActiveDownloads {
    pub count: usize,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for ActiveDownloads {
    fn emit(self) {
        trace!(count = self.count, target = %self.target, "Active downloads");
        gauge!("blizzard_active_downloads", "target" => self.target).set(self.count as f64);
    }
}

/// Event emitted when the number of active uploads changes.
pub struct ActiveUploads {
    pub count: usize,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for ActiveUploads {
    fn emit(self) {
        trace!(count = self.count, target = %self.target, "Active uploads");
        gauge!("blizzard_active_uploads", "target" => self.target).set(self.count as f64);
    }
}

/// Event emitted when the number of in-flight multipart parts changes.
/// Note: This is a storage-level metric without pipeline label since it's
/// emitted from the shared storage layer that doesn't have pipeline context.
pub struct ActiveMultipartParts {
    pub count: usize,
}

impl InternalEvent for ActiveMultipartParts {
    fn emit(self) {
        trace!(count = self.count, "Active multipart parts");
        gauge!("blizzard_active_multipart_parts").set(self.count as f64);
    }
}

/// Event emitted when the number of pending batches changes.
pub struct PendingBatches {
    pub count: usize,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for PendingBatches {
    fn emit(self) {
        trace!(count = self.count, target = %self.target, "Pending batches");
        gauge!("blizzard_pending_batches", "target" => self.target).set(self.count as f64);
    }
}

/// Event emitted when the decompression queue depth changes.
pub struct DecompressionQueueDepth {
    pub count: usize,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for DecompressionQueueDepth {
    fn emit(self) {
        trace!(count = self.count, target = %self.target, "Decompression queue depth");
        gauge!("blizzard_decompression_queue_depth", "target" => self.target)
            .set(self.count as f64);
    }
}

// ============================================================================
// Storage operation events
// ============================================================================

/// Storage operation types.
#[derive(Debug, Clone, Copy)]
pub enum StorageOperation {
    Get,
    Put,
    Delete,
    List,
    Rename,
    CreateMultipart,
    PutPart,
    CompleteMultipart,
}

impl StorageOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageOperation::Get => "get",
            StorageOperation::Put => "put",
            StorageOperation::Delete => "delete",
            StorageOperation::List => "list",
            StorageOperation::Rename => "rename",
            StorageOperation::CreateMultipart => "create_multipart",
            StorageOperation::PutPart => "put_part",
            StorageOperation::CompleteMultipart => "complete_multipart",
        }
    }
}

/// Status of a storage request.
#[derive(Debug, Clone, Copy)]
pub enum RequestStatus {
    Success,
    Error,
}

impl RequestStatus {
    fn as_str(&self) -> &'static str {
        match self {
            RequestStatus::Success => "success",
            RequestStatus::Error => "error",
        }
    }
}

/// Event emitted when a storage request completes.
pub struct StorageRequest {
    pub operation: StorageOperation,
    pub status: RequestStatus,
}

impl InternalEvent for StorageRequest {
    fn emit(self) {
        trace!(
            operation = self.operation.as_str(),
            status = self.status.as_str(),
            "Storage request"
        );
        counter!(
            "blizzard_storage_requests_total",
            "operation" => self.operation.as_str(),
            "status" => self.status.as_str()
        )
        .increment(1);
    }
}

/// Event emitted when a storage request completes with duration.
pub struct StorageRequestDuration {
    pub operation: StorageOperation,
    pub duration: Duration,
}

impl InternalEvent for StorageRequestDuration {
    fn emit(self) {
        trace!(
            operation = self.operation.as_str(),
            duration_ms = self.duration.as_millis(),
            "Storage request duration"
        );
        histogram!(
            "blizzard_storage_request_duration_seconds",
            "operation" => self.operation.as_str()
        )
        .record(self.duration.as_secs_f64());
    }
}

/// Event emitted when a multipart upload completes.
pub struct MultipartUploadCompleted;

impl InternalEvent for MultipartUploadCompleted {
    fn emit(self) {
        trace!("Multipart upload completed");
        counter!("blizzard_multipart_uploads_total").increment(1);
    }
}

// ============================================================================
// Checkpointing & recovery events
// ============================================================================

/// Event emitted to track time since last checkpoint.
pub struct CheckpointAge {
    pub seconds: f64,
}

impl InternalEvent for CheckpointAge {
    fn emit(self) {
        trace!(seconds = self.seconds, "Checkpoint age");
        gauge!("blizzard_checkpoint_age_seconds").set(self.seconds);
    }
}

// ============================================================================
// Memory tracking events
// ============================================================================

/// Event emitted when the source state file count changes.
pub struct SourceStateFiles {
    pub count: usize,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for SourceStateFiles {
    fn emit(self) {
        trace!(count = self.count, target = %self.target, "Source state files tracked");
        gauge!("blizzard_source_state_files", "target" => self.target).set(self.count as f64);
    }
}

/// Event emitted to track bytes waiting in the upload queue.
pub struct UploadQueueBytes {
    pub bytes: usize,
}

impl InternalEvent for UploadQueueBytes {
    fn emit(self) {
        trace!(bytes = self.bytes, "Upload queue bytes");
        gauge!("blizzard_upload_queue_bytes").set(self.bytes as f64);
    }
}

/// Event emitted when the upload queue depth changes.
pub struct UploadQueueDepth {
    pub count: usize,
}

impl InternalEvent for UploadQueueDepth {
    fn emit(self) {
        trace!(count = self.count, "Upload queue depth");
        gauge!("blizzard_upload_queue_depth").set(self.count as f64);
    }
}

// ============================================================================
// Staging directory events
// ============================================================================

/// Event emitted when a parquet file is written to the table directory.
pub struct ParquetFileWritten {
    pub bytes: usize,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for ParquetFileWritten {
    fn emit(self) {
        trace!(bytes = self.bytes, target = %self.target, "Parquet file written");
        counter!("blizzard_parquet_files_written_total", "target" => self.target.clone())
            .increment(1);
        counter!("blizzard_parquet_bytes_written_total", "target" => self.target)
            .increment(self.bytes as u64);
    }
}

/// Event emitted when the number of buffered records changes.
///
/// Tracks records waiting in the parquet writer buffer before the file
/// size policy triggers a roll.
pub struct BufferedRecords {
    pub count: usize,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for BufferedRecords {
    fn emit(self) {
        trace!(count = self.count, target = %self.target, "Buffered records");
        gauge!("blizzard_buffered_records", "target" => self.target).set(self.count as f64);
    }
}

// ============================================================================
// Polling iteration events
// ============================================================================

/// Result type for iteration metrics.
#[derive(Debug, Clone, Copy)]
pub enum IterationResultType {
    Processed,
    NoItems,
}

impl IterationResultType {
    fn as_str(&self) -> &'static str {
        match self {
            IterationResultType::Processed => "processed",
            IterationResultType::NoItems => "no_items",
        }
    }
}

/// Event emitted when a polling iteration completes.
///
/// Tracks iteration outcomes across both blizzard and penguin services.
pub struct IterationCompleted {
    /// Service identifier ("blizzard" or "penguin").
    pub service: &'static str,
    /// Result of the iteration.
    pub result: IterationResultType,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for IterationCompleted {
    fn emit(self) {
        trace!(
            service = self.service,
            result = self.result.as_str(),
            target = %self.target,
            "Iteration completed"
        );
        counter!(
            "polling_iterations_total",
            "service" => self.service,
            "result" => self.result.as_str(),
            "target" => self.target
        )
        .increment(1);
    }
}

/// Event emitted to track polling iteration duration.
///
/// Measures time spent in prepare + process phases.
pub struct IterationDuration {
    /// Service identifier ("blizzard" or "penguin").
    pub service: &'static str,
    /// Duration of the iteration.
    pub duration: Duration,
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for IterationDuration {
    fn emit(self) {
        trace!(
            service = self.service,
            duration_ms = self.duration.as_millis(),
            target = %self.target,
            "Iteration duration"
        );
        histogram!(
            "polling_iteration_duration_seconds",
            "service" => self.service,
            "target" => self.target
        )
        .record(self.duration.as_secs_f64());
    }
}
