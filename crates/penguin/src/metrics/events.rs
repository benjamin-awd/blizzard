//! Internal events for penguin metrics emission.
//!
//! Each event struct represents a measurable occurrence in the penguin pipeline.
//! Events implement the `InternalEvent` trait which emits the corresponding
//! Prometheus metric.
//!
//! ## Table Labels
//!
//! For multi-table deployments, metrics include a `table` label to enable
//! per-table observability (e.g., `"events"`, `"users"`).

use metrics::{counter, gauge, histogram};
use std::time::Duration;
use tracing::trace;

/// Trait for internal events that can be emitted as metrics.
pub trait InternalEvent {
    /// Emit this event as a metric.
    fn emit(self);
}

// ============================================================================
// Delta Lake commit events
// ============================================================================

/// Event emitted when a Delta Lake commit completes.
pub struct DeltaCommitCompleted {
    pub duration: Duration,
    /// Table label for multi-table deployments.
    pub table: String,
}

impl InternalEvent for DeltaCommitCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            table = %self.table,
            "Delta commit completed"
        );
        histogram!("penguin_delta_commit_duration_seconds", "table" => self.table)
            .record(self.duration.as_secs_f64());
    }
}

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

// ============================================================================
// Schema evolution events
// ============================================================================

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
        counter!("penguin_schema_evolutions_total", "table" => self.table, "action" => self.action)
            .increment(1);
    }
}

// ============================================================================
// Staging events
// ============================================================================

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

/// Event emitted when a file is moved to committed in staging.
pub struct StagingFileCommitted {
    /// Table label for multi-table deployments.
    pub table: String,
}

impl InternalEvent for StagingFileCommitted {
    fn emit(self) {
        trace!(table = %self.table, "Staging file committed");
        counter!("penguin_staging_files_committed_total", "table" => self.table).increment(1);
    }
}

// ============================================================================
// Checkpoint events
// ============================================================================

/// Event emitted to track checkpoint state size in bytes.
pub struct CheckpointStateSize {
    pub bytes: usize,
}

impl InternalEvent for CheckpointStateSize {
    fn emit(self) {
        trace!(bytes = self.bytes, "Checkpoint state size");
        gauge!("penguin_checkpoint_state_bytes").set(self.bytes as f64);
    }
}
