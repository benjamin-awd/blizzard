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
use tracing::{debug, error, info, warn};

use crate::emit;
use crate::error::DeltaError;
use crate::metrics::events::{CheckpointAge, SourceStateFiles};
use crate::sink::FinishedFile;
use crate::sink::delta::DeltaSink;
use crate::source::SourceState;

/// Consolidated checkpoint state protected by a single lock.
struct CheckpointStateInner {
    source_state: SourceState,
    delta_version: i64,
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
            commits_since_delta_checkpoint: Arc::new(Mutex::new(0)),
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
        emit!(SourceStateFiles { count: file_count });
    }

    /// Update the source state for a file.
    pub async fn update_source_state(&self, path: &str, records_read: usize, finished: bool) {
        let mut state = self.state.lock().await;
        if finished {
            state.source_state.mark_finished(path);
        } else {
            state.source_state.update_records(path, records_read);
        }
        emit!(SourceStateFiles {
            count: state.source_state.files.len()
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

    /// Restore checkpoint state from the Delta transaction log.
    ///
    /// This is the primary entry point for cold start recovery. It scans the
    /// Delta log for embedded checkpoint state and restores it to the coordinator.
    ///
    /// Returns `true` if a checkpoint was recovered, `false` otherwise.
    pub async fn restore_from_delta_log(
        &self,
        delta_sink: &mut DeltaSink,
    ) -> Result<bool, DeltaError> {
        if let Some((checkpoint, version)) = delta_sink.recover_checkpoint_from_log().await? {
            info!(
                "Recovered checkpoint v{} from Delta log, delta_version: {}, files tracked: {}",
                version,
                checkpoint.delta_version,
                checkpoint.source_state.files.len()
            );
            self.restore_from_state(checkpoint).await;
            Ok(true)
        } else {
            info!("No checkpoint found in Delta log, starting fresh");
            Ok(false)
        }
    }

    /// Compact the checkpoint state by removing finished files outside the given prefixes.
    ///
    /// This should be called after `restore_from_delta_log` when using partition filtering.
    /// It removes finished files that are outside the partition filter window, reducing
    /// memory usage significantly for long-running pipelines.
    ///
    /// Returns the number of files removed.
    pub async fn compact_state(&self, prefixes: &[String]) -> usize {
        let mut state = self.state.lock().await;
        let before = state.source_state.files.len();
        let removed = state.source_state.compact(prefixes);

        if removed > 0 {
            info!(
                "Compacted checkpoint state: removed {} finished files outside partition window ({} -> {} files)",
                removed,
                before,
                state.source_state.files.len()
            );
        }

        emit!(SourceStateFiles {
            count: state.source_state.files.len()
        });

        removed
    }

    /// Maybe create a Delta Lake checkpoint file based on the commit interval.
    ///
    /// Delta checkpoints are Parquet files that summarize the state of the table,
    /// allowing readers to skip reading all JSON log files. This dramatically
    /// improves read performance for tables with many commits.
    ///
    /// # Arguments
    /// * `delta_sink` - The Delta sink to create the checkpoint for
    /// * `interval` - Number of commits between checkpoints. Set to 0 to disable.
    async fn maybe_create_delta_checkpoint(&self, delta_sink: &DeltaSink, interval: usize) {
        if interval == 0 {
            return;
        }

        let mut counter = self.commits_since_delta_checkpoint.lock().await;
        *counter += 1;

        if *counter >= interval {
            match deltalake::checkpoints::create_checkpoint(delta_sink.table(), None).await {
                Ok(()) => {
                    info!(
                        "Created Delta checkpoint at version {}",
                        delta_sink.version()
                    );
                    *counter = 0;
                }
                Err(e) => {
                    warn!("Failed to create Delta checkpoint: {}", e);
                    // Don't reset counter on failure - will retry on next commit
                }
            }
        }
    }

    /// Commit files to Delta Lake with an atomic checkpoint.
    ///
    /// This centralizes the checkpoint commit logic:
    /// 1. Captures the current checkpoint state
    /// 2. Commits files with the checkpoint atomically
    /// 3. Updates the coordinator with the new delta version
    /// 4. Marks the checkpoint as committed (resets timer)
    /// 5. Maybe creates a Delta checkpoint file (based on interval)
    ///
    /// Returns the number of files committed (0 if files list was empty).
    pub async fn commit_files(
        &self,
        delta_sink: &mut DeltaSink,
        files: &[FinishedFile],
        delta_checkpoint_interval: usize,
    ) -> usize {
        if files.is_empty() {
            return 0;
        }

        let count = files.len();

        // Capture current checkpoint state
        let checkpoint_state = self.capture_state().await;

        // Commit with atomic checkpoint
        match delta_sink
            .commit_files_with_checkpoint(files, &checkpoint_state)
            .await
        {
            Ok(Some(version)) => {
                info!(
                    "Committed {} files with checkpoint to Delta Lake, version {}",
                    count, version
                );
                // Update coordinator with new delta version and mark checkpoint committed
                self.update_delta_version(version).await;
                self.mark_checkpoint_committed().await;
                // Maybe create Delta checkpoint file
                self.maybe_create_delta_checkpoint(delta_sink, delta_checkpoint_interval)
                    .await;
            }
            Ok(None) => {
                debug!("No commit needed (duplicate files)");
            }
            Err(e) => {
                error!("Failed to commit {} files to Delta: {}", count, e);
            }
        }
        count
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
