//! Traits for checkpoint management abstractions.
//!
//! This module defines the `CheckpointManager` trait that abstracts checkpoint
//! coordination operations, enabling dependency inversion and improved testability.

use async_trait::async_trait;

use blizzard_core::FinishedFile;

use crate::error::DeltaError;
use crate::sink::TableSink;

use super::CheckpointState;

/// Trait for checkpoint managers that coordinate checkpoint state.
///
/// This abstraction allows the pipeline to work with different checkpoint
/// coordination strategies without depending on concrete implementations.
#[async_trait]
pub trait CheckpointManager: Send + Sync {
    /// Capture the current checkpoint state.
    ///
    /// Returns a snapshot of the checkpoint state that can be persisted.
    async fn capture_state(&self) -> CheckpointState;

    /// Restore the manager from a recovered checkpoint state.
    ///
    /// Called after recovering checkpoint state from the table.
    async fn restore_from_state(&self, checkpoint: CheckpointState);

    /// Get the current watermark, if any.
    async fn watermark(&self) -> Option<String>;

    /// Update the watermark to the given path.
    async fn update_watermark(&self, watermark: String);

    /// Mark a source file as finished.
    async fn mark_file_finished(&self, path: &str);

    /// Update the table version after a successful commit.
    async fn update_table_version(&self, version: i64);

    /// Mark that a checkpoint was just committed.
    ///
    /// This resets the checkpoint timer for timing-based operations.
    async fn mark_checkpoint_committed(&self);

    /// Restore checkpoint state from the table's transaction log.
    ///
    /// This is the primary entry point for cold start recovery.
    ///
    /// Returns `true` if a checkpoint was recovered, `false` otherwise.
    async fn restore_from_table_log(&self, sink: &mut dyn TableSink) -> Result<bool, DeltaError>;

    /// Commit files to the table with an atomic checkpoint.
    ///
    /// Returns the number of files committed.
    async fn commit_files(
        &self,
        sink: &mut dyn TableSink,
        files: &[FinishedFile],
        checkpoint_interval: usize,
    ) -> usize;

    /// Get the table identifier for logging.
    fn table_name(&self) -> &str;
}
