//! Checkpoint coordination for exactly-once processing.
//!
//! # Atomic Checkpointing with Delta Lake Txn Actions
//!
//! Checkpoints are stored atomically alongside data commits using Delta Lake's
//! `Txn` action. This eliminates the need for separate JSON checkpoint files
//! and ensures checkpoint state is always consistent with committed data.
//!
//! The checkpoint state is embedded in the `Txn.app_id` field as base64-encoded JSON,
//! with format: `blizzard:{base64_encoded_checkpoint_json}`

pub mod state;
mod traits;

pub use state::CheckpointState;
pub use traits::CheckpointManager;

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use blizzard_core::FinishedFile;
use blizzard_core::metrics::events::{CheckpointAge, SourceStateFiles};
use blizzard_core::types::SourceState;

use crate::error::DeltaError;
use crate::sink::TableSink;

/// Macro for emitting metrics events.
macro_rules! emit {
    ($event:expr) => {
        <_ as blizzard_core::metrics::events::InternalEvent>::emit($event)
    };
}

/// Consolidated checkpoint state protected by a single lock.
struct CheckpointStateInner {
    source_state: SourceState,
    delta_version: i64,
    /// High-watermark for incoming mode.
    watermark: Option<String>,
}

/// A coordinator that manages checkpoint state for atomic commits.
///
/// With Txn-based checkpointing, the coordinator captures state
/// that is then included in Delta commits via `DeltaSink::commit_files_with_checkpoint()`.
pub struct CheckpointCoordinator {
    state: Arc<Mutex<CheckpointStateInner>>,
    last_checkpoint: Arc<Mutex<Instant>>,
    /// Number of commits since last Delta checkpoint file was created.
    commits_since_delta_checkpoint: Arc<Mutex<usize>>,
    /// Table identifier for metrics labeling.
    table: String,
}

impl CheckpointCoordinator {
    /// Create a new checkpoint coordinator.
    pub fn new(table: String) -> Self {
        Self {
            state: Arc::new(Mutex::new(CheckpointStateInner {
                source_state: SourceState::new(),
                delta_version: -1,
                watermark: None,
            })),
            last_checkpoint: Arc::new(Mutex::new(Instant::now())),
            commits_since_delta_checkpoint: Arc::new(Mutex::new(0)),
            table,
        }
    }

    /// Restore state from a recovered checkpoint.
    ///
    /// Called after `DeltaSink::recover_checkpoint_from_log()` successfully
    /// recovers checkpoint state from the Delta transaction log.
    pub async fn restore_from_state(&self, checkpoint: CheckpointState) {
        let mut state = self.state.lock().await;
        let file_count = checkpoint.source_state.files.len();
        state.source_state = checkpoint.source_state;
        state.delta_version = checkpoint.delta_version;
        state.watermark = checkpoint.watermark;
        emit!(SourceStateFiles {
            count: file_count,
            target: self.table.clone(),
        });
        if let Some(ref wm) = state.watermark {
            info!(target = %self.table, watermark = %wm, "Restored watermark from checkpoint");
        }
    }

    /// Mark a source file as finished.
    pub async fn mark_file_finished(&self, path: &str) {
        let mut state = self.state.lock().await;
        state.source_state.mark_finished(path);
        emit!(SourceStateFiles {
            count: state.source_state.files.len(),
            target: self.table.clone(),
        });
    }

    /// Update the Delta version after a successful commit.
    pub async fn update_delta_version(&self, version: i64) {
        let mut state = self.state.lock().await;
        state.delta_version = version;
    }

    /// Mark that a checkpoint was just committed.
    ///
    /// Called after a successful `DeltaSink::commit_files_with_checkpoint()`
    /// to reset the checkpoint timer.
    pub async fn mark_checkpoint_committed(&self) {
        let mut last = self.last_checkpoint.lock().await;
        *last = Instant::now();
        emit!(CheckpointAge { seconds: 0.0 });
        debug!(target = %self.table, "Checkpoint committed, timer reset");
    }

    /// Capture the current state for checkpointing.
    ///
    /// Returns a snapshot of the checkpoint state that can be included
    /// in a Delta commit via `DeltaSink::commit_files_with_checkpoint()`.
    pub async fn capture_state(&self) -> CheckpointState {
        let state = self.state.lock().await;
        CheckpointState {
            schema_version: 2,
            source_state: state.source_state.clone(),
            delta_version: state.delta_version,
            watermark: state.watermark.clone(),
        }
    }

    /// Get the current watermark.
    pub async fn watermark(&self) -> Option<String> {
        let state = self.state.lock().await;
        state.watermark.clone()
    }

    /// Update the watermark to the given path.
    pub async fn update_watermark(&self, watermark: String) {
        let mut state = self.state.lock().await;
        debug!(target = %self.table, new_watermark = %watermark, "Updating watermark");
        state.watermark = Some(watermark);
    }

    /// Restore checkpoint state from the table's transaction log.
    ///
    /// This is the primary entry point for cold start recovery. It scans the
    /// table's transaction log for embedded checkpoint state and restores it to the coordinator.
    ///
    /// Returns `true` if a checkpoint was recovered, `false` otherwise.
    pub async fn restore_from_table_log(
        &self,
        sink: &mut dyn TableSink,
    ) -> Result<bool, DeltaError> {
        if let Some((checkpoint, version)) = sink.recover_checkpoint_from_log().await? {
            info!(
                target = %self.table,
                "Recovered checkpoint v{} from Delta log, delta_version: {}, files tracked: {}",
                version,
                checkpoint.delta_version,
                checkpoint.source_state.files.len()
            );
            self.restore_from_state(checkpoint).await;
            Ok(true)
        } else {
            info!(target = %self.table, "No checkpoint found in Delta log, starting fresh");
            Ok(false)
        }
    }

    /// Maybe create a table checkpoint file based on the commit interval.
    ///
    /// Table checkpoints are files that summarize the state of the table,
    /// allowing readers to skip reading all log files. This dramatically
    /// improves read performance for tables with many commits.
    ///
    /// # Arguments
    /// * `sink` - The table sink to create the checkpoint for
    /// * `interval` - Number of commits between checkpoints. Set to 0 to disable.
    async fn maybe_create_table_checkpoint(&self, sink: &dyn TableSink, interval: usize) {
        if interval == 0 {
            return;
        }

        let mut counter = self.commits_since_delta_checkpoint.lock().await;
        *counter += 1;

        if *counter >= interval {
            match sink.create_checkpoint().await {
                Ok(()) => {
                    info!(
                        target = %self.table,
                        "Created table checkpoint at version {}",
                        sink.version()
                    );
                    *counter = 0;
                }
                Err(e) => {
                    warn!(target = %self.table, "Failed to create table checkpoint: {e}");
                    // Don't reset counter on failure - will retry on next commit
                }
            }
        }
    }

    /// Commit files to the table with an atomic checkpoint.
    ///
    /// This centralizes the checkpoint commit logic:
    /// 1. Captures the current checkpoint state
    /// 2. Commits files with the checkpoint atomically
    /// 3. Updates the coordinator with the new table version
    /// 4. Marks the checkpoint as committed (resets timer)
    /// 5. Maybe creates a table checkpoint file (based on interval)
    ///
    /// Returns the number of files committed (0 if files list was empty).
    pub async fn commit_files(
        &self,
        sink: &mut dyn TableSink,
        files: &[FinishedFile],
        checkpoint_interval: usize,
    ) -> usize {
        if files.is_empty() {
            return 0;
        }

        let count = files.len();

        // Capture current checkpoint state
        let checkpoint_state = self.capture_state().await;

        // Commit with atomic checkpoint
        match sink
            .commit_files_with_checkpoint(files, &checkpoint_state)
            .await
        {
            Ok(Some(version)) => {
                info!(
                    target = %self.table,
                    "Committed {} files with checkpoint to table, version {}",
                    count, version
                );
                // Update coordinator with new table version and mark checkpoint committed
                self.update_delta_version(version).await;
                self.mark_checkpoint_committed().await;
                // Maybe create table checkpoint file
                self.maybe_create_table_checkpoint(sink, checkpoint_interval)
                    .await;
            }
            Ok(None) => {
                debug!(target = %self.table, "No commit needed (duplicate files)");
            }
            Err(e) => {
                tracing::error!(target = %self.table, "Failed to commit {} files to table: {}", count, e);
            }
        }
        count
    }
}

#[async_trait]
impl CheckpointManager for CheckpointCoordinator {
    async fn capture_state(&self) -> CheckpointState {
        CheckpointCoordinator::capture_state(self).await
    }

    async fn restore_from_state(&self, checkpoint: CheckpointState) {
        CheckpointCoordinator::restore_from_state(self, checkpoint).await;
    }

    async fn watermark(&self) -> Option<String> {
        CheckpointCoordinator::watermark(self).await
    }

    async fn update_watermark(&self, watermark: String) {
        CheckpointCoordinator::update_watermark(self, watermark).await;
    }

    async fn mark_file_finished(&self, path: &str) {
        CheckpointCoordinator::mark_file_finished(self, path).await;
    }

    async fn update_table_version(&self, version: i64) {
        self.update_delta_version(version).await;
    }

    async fn mark_checkpoint_committed(&self) {
        CheckpointCoordinator::mark_checkpoint_committed(self).await;
    }

    async fn restore_from_table_log(&self, sink: &mut dyn TableSink) -> Result<bool, DeltaError> {
        CheckpointCoordinator::restore_from_table_log(self, sink).await
    }

    async fn commit_files(
        &self,
        sink: &mut dyn TableSink,
        files: &[FinishedFile],
        checkpoint_interval: usize,
    ) -> usize {
        CheckpointCoordinator::commit_files(self, sink, files, checkpoint_interval).await
    }

    fn table_name(&self) -> &str {
        &self.table
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_state_serialization() {
        let mut source_state = SourceState::new();
        source_state.mark_finished("file1.ndjson.gz");
        source_state.mark_finished("file2.ndjson.gz");

        let state = CheckpointState {
            schema_version: 2,
            source_state,
            delta_version: 5,
            watermark: Some("date=2024-01-28/uuid.parquet".to_string()),
        };

        let json = serde_json::to_string(&state).unwrap();
        let restored: CheckpointState = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.schema_version, 2);
        assert_eq!(restored.delta_version, 5);
        assert!(restored.source_state.is_file_finished("file1.ndjson.gz"));
        assert!(restored.source_state.is_file_finished("file2.ndjson.gz"));
        assert_eq!(
            restored.watermark,
            Some("date=2024-01-28/uuid.parquet".to_string())
        );
    }

    #[tokio::test]
    async fn test_coordinator_capture_state() {
        let coordinator = CheckpointCoordinator::new("test".to_string());

        // Update some state
        coordinator.mark_file_finished("file1.ndjson.gz").await;
        coordinator.mark_file_finished("file2.ndjson.gz").await;
        coordinator.update_delta_version(5).await;

        // Capture state
        let captured = coordinator.capture_state().await;

        assert_eq!(captured.schema_version, 2);
        assert_eq!(captured.delta_version, 5);
        assert!(captured.source_state.is_file_finished("file1.ndjson.gz"));
        assert!(captured.source_state.is_file_finished("file2.ndjson.gz"));
    }
}
