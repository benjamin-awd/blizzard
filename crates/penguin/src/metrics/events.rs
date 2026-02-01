//! Internal events for penguin metrics emission.
//!
//! Each event struct represents a measurable occurrence in the penguin pipeline.
//! Events implement the `InternalEvent` trait which emits the corresponding
//! Prometheus metric.
//!
//! ## Target Labels
//!
//! For multi-target deployments, metrics include a `target` label to enable
//! per-target observability (e.g., `"events"`, `"users"`).

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
    /// Target label for multi-target deployments.
    pub target: String,
}

impl InternalEvent for DeltaCommitCompleted {
    fn emit(self) {
        trace!(
            duration_ms = self.duration.as_millis(),
            target = %self.target,
            "Delta commit completed"
        );
        histogram!("penguin_delta_commit_duration_seconds", "target" => self.target)
            .record(self.duration.as_secs_f64());
    }
}

/// Event emitted when files are committed to Delta Lake.
pub struct FilesCommitted {
    /// Target identifier for multi-target deployments.
    pub target: String,
    /// Number of files committed.
    pub count: u64,
}

impl InternalEvent for FilesCommitted {
    fn emit(self) {
        trace!(target = %self.target, count = self.count, "Files committed");
        counter!("penguin_files_committed_total", "target" => self.target).increment(self.count);
    }
}

/// Event emitted when records are committed to Delta Lake.
pub struct RecordsCommitted {
    /// Target identifier for multi-target deployments.
    pub target: String,
    /// Number of records committed.
    pub count: u64,
}

impl InternalEvent for RecordsCommitted {
    fn emit(self) {
        trace!(target = %self.target, count = self.count, "Records committed");
        counter!("penguin_records_committed_total", "target" => self.target).increment(self.count);
    }
}

/// Event emitted to track the current Delta table version.
pub struct DeltaTableVersion {
    /// Target identifier for multi-target deployments.
    pub target: String,
    /// Current table version.
    pub version: i64,
}

impl InternalEvent for DeltaTableVersion {
    fn emit(self) {
        trace!(target = %self.target, version = self.version, "Delta table version");
        gauge!("penguin_delta_table_version", "target" => self.target).set(self.version as f64);
    }
}

// ============================================================================
// Schema evolution events
// ============================================================================

/// Event emitted when schema evolution occurs.
pub struct SchemaEvolved {
    /// Target identifier for multi-target deployments.
    pub target: String,
    /// The action taken: "merge", "overwrite", or "none".
    pub action: String,
}

impl InternalEvent for SchemaEvolved {
    fn emit(self) {
        trace!(target = %self.target, action = %self.action, "Schema evolved");
        counter!("penguin_schema_evolutions_total", "target" => self.target, "action" => self.action)
            .increment(1);
    }
}

// ============================================================================
// File discovery events
// ============================================================================

/// Event emitted when the number of pending files changes.
pub struct PendingFiles {
    /// Target identifier for multi-target deployments.
    pub target: String,
    /// Number of files pending commit.
    pub count: usize,
}

impl InternalEvent for PendingFiles {
    fn emit(self) {
        trace!(target = %self.target, count = self.count, "Pending files");
        gauge!("penguin_pending_files", "target" => self.target).set(self.count as f64);
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
