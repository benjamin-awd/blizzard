//! Internal events for metrics emission.
//!
//! Each event struct represents a measurable occurrence in the pipeline.
//! Events implement the `InternalEvent` trait which emits the corresponding
//! Prometheus counter metric.
//!
//! ## Component Labels
//!
//! For multi-pipeline (blizzard) and multi-table (penguin) deployments,
//! metrics include component labels to enable per-component observability:
//!
//! - Blizzard metrics: `pipeline` label (e.g., `"orderbooks"`, `"trades"`)
//! - Penguin metrics: `table` label (e.g., `"events"`, `"users"`)

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
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for RecordsProcessed {
    fn emit(self) {
        trace!(count = self.count, pipeline = %self.pipeline, "Records processed");
        counter!("blizzard_records_processed_total", "pipeline" => self.pipeline).increment(self.count);
    }
}

/// Event emitted when compressed bytes are read from source.
pub struct BytesRead {
    pub bytes: u64,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for BytesRead {
    fn emit(self) {
        trace!(bytes = self.bytes, pipeline = %self.pipeline, "Bytes read");
        counter!("blizzard_bytes_read_total", "pipeline" => self.pipeline).increment(self.bytes);
    }
}

/// Event emitted when bytes are written to Parquet files.
pub struct BytesWritten {
    pub bytes: u64,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for BytesWritten {
    fn emit(self) {
        trace!(bytes = self.bytes, pipeline = %self.pipeline, "Bytes written");
        counter!("blizzard_bytes_written_total", "pipeline" => self.pipeline).increment(self.bytes);
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
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for FileFailed {
    fn emit(self) {
        trace!(stage = self.stage.as_str(), pipeline = %self.pipeline, "File failed");
        counter!("blizzard_files_failed_total", "stage" => self.stage.as_str(), "pipeline" => self.pipeline).increment(1);
    }
}

/// Event emitted when an input file is processed.
pub struct FileProcessed {
    pub status: FileStatus,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for FileProcessed {
    fn emit(self) {
        trace!(status = self.status.as_str(), pipeline = %self.pipeline, "File processed");
        counter!("blizzard_files_processed_total", "status" => self.status.as_str(), "pipeline" => self.pipeline).increment(1);
    }
}

/// Event emitted when an Arrow RecordBatch is created.
pub struct BatchesProcessed {
    pub count: u64,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for BatchesProcessed {
    fn emit(self) {
        trace!(count = self.count, pipeline = %self.pipeline, "Batches processed");
        counter!("blizzard_batches_processed_total", "pipeline" => self.pipeline).increment(self.count);
    }
}

// ============================================================================
// Histogram events for timing
// ============================================================================

/// Event emitted when a file download completes.
pub struct FileDownloadCompleted {
    pub duration: Duration,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for FileDownloadCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            pipeline = %self.pipeline,
            "File download completed"
        );
        histogram!("blizzard_file_download_duration_seconds", "pipeline" => self.pipeline).record(self.duration.as_secs_f64());
    }
}

/// Event emitted when file decompression completes.
pub struct FileDecompressionCompleted {
    pub duration: Duration,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for FileDecompressionCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            pipeline = %self.pipeline,
            "File decompression completed"
        );
        histogram!("blizzard_file_decompression_duration_seconds", "pipeline" => self.pipeline).record(self.duration.as_secs_f64());
    }
}

/// Event emitted when a Parquet file write completes.
pub struct ParquetWriteCompleted {
    pub duration: Duration,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for ParquetWriteCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            pipeline = %self.pipeline,
            "Parquet write completed"
        );
        histogram!("blizzard_parquet_write_duration_seconds", "pipeline" => self.pipeline).record(self.duration.as_secs_f64());
    }
}

/// Event emitted when a Delta Lake commit completes.
pub struct DeltaCommitCompleted {
    pub duration: Duration,
    /// Table label for multi-table deployments (penguin).
    pub table: String,
}

impl InternalEvent for DeltaCommitCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            table = %self.table,
            "Delta commit completed"
        );
        histogram!("penguin_delta_commit_duration_seconds", "table" => self.table).record(self.duration.as_secs_f64());
    }
}

// ============================================================================
// Gauge events for concurrency and backpressure
// ============================================================================

/// Event emitted when the number of active downloads changes.
pub struct ActiveDownloads {
    pub count: usize,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for ActiveDownloads {
    fn emit(self) {
        trace!(count = self.count, pipeline = %self.pipeline, "Active downloads");
        gauge!("blizzard_active_downloads", "pipeline" => self.pipeline).set(self.count as f64);
    }
}

/// Event emitted when the number of active uploads changes.
pub struct ActiveUploads {
    pub count: usize,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for ActiveUploads {
    fn emit(self) {
        trace!(count = self.count, pipeline = %self.pipeline, "Active uploads");
        gauge!("blizzard_active_uploads", "pipeline" => self.pipeline).set(self.count as f64);
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
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for PendingBatches {
    fn emit(self) {
        trace!(count = self.count, pipeline = %self.pipeline, "Pending batches");
        gauge!("blizzard_pending_batches", "pipeline" => self.pipeline).set(self.count as f64);
    }
}

/// Event emitted when the decompression queue depth changes.
pub struct DecompressionQueueDepth {
    pub count: usize,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for DecompressionQueueDepth {
    fn emit(self) {
        trace!(count = self.count, pipeline = %self.pipeline, "Decompression queue depth");
        gauge!("blizzard_decompression_queue_depth", "pipeline" => self.pipeline).set(self.count as f64);
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

/// Event emitted when records are skipped during recovery.
pub struct RecoveredRecords {
    pub count: u64,
}

impl InternalEvent for RecoveredRecords {
    fn emit(self) {
        trace!(count = self.count, "Recovered records");
        counter!("blizzard_recovered_records_total").increment(self.count);
    }
}

// ============================================================================
// Memory tracking events
// ============================================================================

/// Event emitted when the source state file count changes.
pub struct SourceStateFiles {
    pub count: usize,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for SourceStateFiles {
    fn emit(self) {
        trace!(count = self.count, pipeline = %self.pipeline, "Source state files tracked");
        gauge!("blizzard_source_state_files", "pipeline" => self.pipeline).set(self.count as f64);
    }
}

/// Event emitted to track checkpoint state size in bytes.
pub struct CheckpointStateSize {
    pub bytes: usize,
}

impl InternalEvent for CheckpointStateSize {
    fn emit(self) {
        trace!(bytes = self.bytes, "Checkpoint state size");
        gauge!("blizzard_checkpoint_state_bytes").set(self.bytes as f64);
    }
}

/// Event emitted when the number of files pending Delta commit changes.
pub struct PendingCommitFiles {
    pub count: usize,
}

impl InternalEvent for PendingCommitFiles {
    fn emit(self) {
        trace!(count = self.count, "Pending commit files");
        gauge!("blizzard_pending_commit_files").set(self.count as f64);
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
// Staging directory events (for blizzard/penguin communication)
// ============================================================================

/// Event emitted when a file is written to the staging directory.
pub struct StagingFileWritten {
    pub bytes: usize,
    /// Pipeline label for multi-pipeline deployments.
    pub pipeline: String,
}

impl InternalEvent for StagingFileWritten {
    fn emit(self) {
        trace!(bytes = self.bytes, pipeline = %self.pipeline, "Staging file written");
        counter!("blizzard_staging_files_written_total", "pipeline" => self.pipeline.clone()).increment(1);
        counter!("blizzard_staging_bytes_written_total", "pipeline" => self.pipeline).increment(self.bytes as u64);
    }
}

/// Event emitted when a file is moved to committed in staging.
pub struct StagingFileCommitted {
    /// Table label for multi-table deployments (penguin).
    pub table: String,
}

impl InternalEvent for StagingFileCommitted {
    fn emit(self) {
        trace!(table = %self.table, "Staging file committed");
        counter!("penguin_staging_files_committed_total", "table" => self.table).increment(1);
    }
}

// ============================================================================
// Penguin-specific events (Delta Lake operations)
// ============================================================================

/// Event emitted when files are committed to Delta Lake.
pub struct FilesCommitted {
    /// Table identifier for multi-table deployments.
    pub table: String,
    /// Number of files committed.
    pub count: u64,
}

impl InternalEvent for FilesCommitted {
    fn emit(self) {
        trace!(table = %self.table, count = self.count, "Files committed");
        counter!("penguin_files_committed_total", "table" => self.table).increment(self.count);
    }
}

/// Event emitted when records are committed to Delta Lake.
pub struct RecordsCommitted {
    /// Table identifier for multi-table deployments.
    pub table: String,
    /// Number of records committed.
    pub count: u64,
}

impl InternalEvent for RecordsCommitted {
    fn emit(self) {
        trace!(table = %self.table, count = self.count, "Records committed");
        counter!("penguin_records_committed_total", "table" => self.table).increment(self.count);
    }
}

/// Event emitted when schema evolution occurs.
pub struct SchemaEvolved {
    /// Table identifier for multi-table deployments.
    pub table: String,
    /// The action taken: "merge", "overwrite", or "none".
    pub action: String,
}

impl InternalEvent for SchemaEvolved {
    fn emit(self) {
        trace!(table = %self.table, action = %self.action, "Schema evolved");
        counter!("penguin_schema_evolutions_total", "table" => self.table, "action" => self.action).increment(1);
    }
}

/// Event emitted when the number of pending files changes.
pub struct PendingFiles {
    /// Table identifier for multi-table deployments.
    pub table: String,
    /// Number of files pending commit.
    pub count: usize,
}

impl InternalEvent for PendingFiles {
    fn emit(self) {
        trace!(table = %self.table, count = self.count, "Pending files");
        gauge!("penguin_pending_files", "table" => self.table).set(self.count as f64);
    }
}

/// Event emitted to track the current Delta table version.
pub struct DeltaTableVersion {
    /// Table identifier for multi-table deployments.
    pub table: String,
    /// Current table version.
    pub version: i64,
}

impl InternalEvent for DeltaTableVersion {
    fn emit(self) {
        trace!(table = %self.table, version = self.version, "Delta table version");
        gauge!("penguin_delta_table_version", "table" => self.table).set(self.version as f64);
    }
}
