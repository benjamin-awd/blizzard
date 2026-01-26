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

pub use state::CheckpointState;

use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::emit;
use crate::internal_events::CheckpointAge;
use crate::source::SourceState;

/// Consolidated checkpoint state protected by a single lock.
struct CheckpointStateInner {
    source_state: SourceState,
    delta_version: i64,
}

/// A coordinator that manages checkpoint state for atomic commits.
///
/// With Txn-based checkpointing, the coordinator no longer writes checkpoints
/// directly. Instead, it captures state that is then included in Delta commits
/// via `DeltaSink::commit_files_with_checkpoint()`.
pub struct CheckpointCoordinator {
    state: Arc<Mutex<CheckpointStateInner>>,
    last_checkpoint: Arc<Mutex<Instant>>,
}

impl Default for CheckpointCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl CheckpointCoordinator {
    /// Create a new checkpoint coordinator.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(CheckpointStateInner {
                source_state: SourceState::new(),
                delta_version: -1,
            })),
            last_checkpoint: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Restore state from a recovered checkpoint.
    ///
    /// Called after `DeltaSink::recover_checkpoint_from_log()` successfully
    /// recovers checkpoint state from the Delta transaction log.
    pub async fn restore_from_state(&self, checkpoint: CheckpointState) {
        let mut state = self.state.lock().await;
        state.source_state = checkpoint.source_state;
        state.delta_version = checkpoint.delta_version;

        info!(
            "Restored checkpoint state: delta_version={}, files={}",
            state.delta_version,
            state.source_state.files.len()
        );
    }

    /// Update the source state for a file.
    pub async fn update_source_state(&self, path: &str, records_read: usize, finished: bool) {
        let mut state = self.state.lock().await;
        if finished {
            state.source_state.mark_finished(path);
        } else {
            state.source_state.update_records(path, records_read);
        }
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
        debug!("Checkpoint committed, timer reset");
    }

    /// Capture the current state for checkpointing.
    ///
    /// Returns a snapshot of the checkpoint state that can be included
    /// in a Delta commit via `DeltaSink::commit_files_with_checkpoint()`.
    pub async fn capture_state(&self) -> CheckpointState {
        let state = self.state.lock().await;
        CheckpointState {
            schema_version: 1,
            source_state: state.source_state.clone(),
            delta_version: state.delta_version,
        }
    }

    /// Get the current source state.
    pub async fn get_source_state(&self) -> SourceState {
        let state = self.state.lock().await;
        state.source_state.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_state_serialization() {
        let mut source_state = SourceState::new();
        source_state.update_records("file1.ndjson.gz", 100);
        source_state.mark_finished("file2.ndjson.gz");

        let state = CheckpointState {
            schema_version: 1,
            source_state,
            delta_version: 5,
        };

        let json = serde_json::to_string(&state).unwrap();
        let restored: CheckpointState = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.schema_version, 1);
        assert_eq!(restored.delta_version, 5);
        assert!(restored.source_state.is_file_finished("file2.ndjson.gz"));
    }

    #[tokio::test]
    async fn test_coordinator_capture_state() {
        let coordinator = CheckpointCoordinator::new();

        // Update some state
        coordinator
            .update_source_state("file1.ndjson.gz", 100, false)
            .await;
        coordinator
            .update_source_state("file2.ndjson.gz", 200, true)
            .await;
        coordinator.update_delta_version(5).await;

        // Capture state
        let captured = coordinator.capture_state().await;

        assert_eq!(captured.schema_version, 1);
        assert_eq!(captured.delta_version, 5);
        assert!(captured.source_state.is_file_finished("file2.ndjson.gz"));
        assert!(!captured.source_state.is_file_finished("file1.ndjson.gz"));
    }
}
