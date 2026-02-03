//! Checkpoint management for source file tracking.
//!
//! Provides persistent watermark-based checkpointing for Blizzard pipelines.
//! Checkpoints are stored at `{table_uri}/_blizzard/{pipeline}_checkpoint.json`.
//!
//! # Atomic Writes
//!
//! Checkpoint updates use atomic write pattern:
//! 1. Write to temp file: `{pipeline}_checkpoint.json.tmp`
//! 2. Rename to final path: `{pipeline}_checkpoint.json`
//!
//! This ensures checkpoints are never partially written.

pub mod state;

pub use state::CheckpointState;

use object_store::PutPayload;
use object_store::path::Path;
use std::sync::Arc;
use tracing::{debug, info, warn};

use blizzard_core::storage::StorageProvider;

use crate::error::StorageError;

/// Manages checkpoint persistence for a Blizzard pipeline.
///
/// Handles loading and saving checkpoint state to cloud storage
/// with atomic write guarantees.
pub struct CheckpointManager {
    /// Storage provider for the table directory.
    storage: Arc<StorageProvider>,
    /// Pipeline identifier (used in checkpoint filename).
    pipeline_key: String,
    /// Current checkpoint state.
    state: CheckpointState,
}

impl CheckpointManager {
    /// Create a new checkpoint manager for the given pipeline.
    ///
    /// # Arguments
    /// * `storage` - Storage provider for the table directory
    /// * `pipeline_key` - Pipeline identifier (e.g., "events")
    pub fn new(storage: Arc<StorageProvider>, pipeline_key: String) -> Self {
        Self {
            storage,
            pipeline_key,
            state: CheckpointState::default(),
        }
    }

    /// Get the checkpoint file path.
    fn checkpoint_path(&self) -> Path {
        Path::from(format!("_blizzard/{}_checkpoint.json", self.pipeline_key))
    }

    /// Get the temporary checkpoint file path.
    fn temp_checkpoint_path(&self) -> Path {
        Path::from(format!(
            "_blizzard/{}_checkpoint.json.tmp",
            self.pipeline_key
        ))
    }

    /// Load checkpoint from storage.
    ///
    /// Returns `Ok(true)` if a checkpoint was loaded, `Ok(false)` if no checkpoint exists.
    /// Returns `Err` only for unexpected errors (not "not found").
    pub async fn load(&mut self) -> Result<bool, StorageError> {
        let path = self.checkpoint_path();

        match self.storage.get(path).await {
            Ok(bytes) => {
                let json = String::from_utf8_lossy(&bytes);
                match serde_json::from_str::<CheckpointState>(&json) {
                    Ok(state) => {
                        info!(
                            target = %self.pipeline_key,
                            watermark = ?state.watermark,
                            last_update_ts = state.last_update_ts,
                            "Loaded checkpoint"
                        );
                        self.state = state;
                        Ok(true)
                    }
                    Err(e) => {
                        warn!(
                            target = %self.pipeline_key,
                            error = %e,
                            "Failed to parse checkpoint JSON, starting fresh"
                        );
                        self.state = CheckpointState::default();
                        Ok(false)
                    }
                }
            }
            Err(e) if e.is_not_found() => {
                debug!(
                    target = %self.pipeline_key,
                    "No checkpoint found, starting fresh"
                );
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// Save checkpoint to storage using atomic write.
    ///
    /// Uses temp file + rename pattern for atomicity.
    pub async fn save(&self) -> Result<(), StorageError> {
        let json = serde_json::to_string_pretty(&self.state)
            .expect("checkpoint state should always serialize");

        let temp_path = self.temp_checkpoint_path();
        let final_path = self.checkpoint_path();

        // Write to temp file
        self.storage
            .put_payload(&temp_path, PutPayload::from(json.into_bytes()))
            .await?;

        // Atomic rename
        self.storage.rename(&temp_path, &final_path).await?;

        debug!(
            target = %self.pipeline_key,
            watermark = ?self.state.watermark,
            "Saved checkpoint"
        );

        Ok(())
    }

    /// Get the current watermark.
    pub fn watermark(&self) -> Option<&str> {
        self.state.watermark.as_deref()
    }

    /// Update the watermark to the given path if it's greater than the current watermark.
    ///
    /// Returns `true` if the watermark was updated.
    pub fn update_watermark(&mut self, path: &str) -> bool {
        self.state.update_watermark(path)
    }

    /// Get a reference to the current checkpoint state.
    pub fn state(&self) -> &CheckpointState {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    async fn create_test_storage(temp_dir: &TempDir) -> Arc<StorageProvider> {
        Arc::new(
            StorageProvider::for_url_with_options(
                temp_dir.path().to_str().unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_checkpoint_manager_load_no_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let storage = create_test_storage(&temp_dir).await;

        let mut manager = CheckpointManager::new(storage, "test_pipeline".to_string());
        let loaded = manager.load().await.unwrap();

        assert!(!loaded);
        assert!(manager.watermark().is_none());
    }

    #[tokio::test]
    async fn test_checkpoint_manager_save_and_load() {
        let temp_dir = TempDir::new().unwrap();

        // Create _blizzard directory
        std::fs::create_dir_all(temp_dir.path().join("_blizzard")).unwrap();

        let storage = create_test_storage(&temp_dir).await;

        // Save a checkpoint
        let mut manager = CheckpointManager::new(storage.clone(), "test_pipeline".to_string());
        manager.update_watermark("date=2026-01-28/file1.ndjson.gz");
        manager.save().await.unwrap();

        // Load it back with a new manager
        let mut manager2 = CheckpointManager::new(storage, "test_pipeline".to_string());
        let loaded = manager2.load().await.unwrap();

        assert!(loaded);
        assert_eq!(
            manager2.watermark(),
            Some("date=2026-01-28/file1.ndjson.gz")
        );
    }

    #[tokio::test]
    async fn test_checkpoint_manager_update_watermark() {
        let temp_dir = TempDir::new().unwrap();
        let storage = create_test_storage(&temp_dir).await;

        let mut manager = CheckpointManager::new(storage, "test_pipeline".to_string());

        // First update should succeed
        assert!(manager.update_watermark("date=2026-01-28/file1.ndjson.gz"));
        assert_eq!(manager.watermark(), Some("date=2026-01-28/file1.ndjson.gz"));

        // Greater watermark should update
        assert!(manager.update_watermark("date=2026-01-28/file2.ndjson.gz"));
        assert_eq!(manager.watermark(), Some("date=2026-01-28/file2.ndjson.gz"));

        // Lesser watermark should not update
        assert!(!manager.update_watermark("date=2026-01-28/file1.ndjson.gz"));
        assert_eq!(manager.watermark(), Some("date=2026-01-28/file2.ndjson.gz"));
    }

    #[tokio::test]
    async fn test_checkpoint_file_path() {
        let temp_dir = TempDir::new().unwrap();
        let storage = create_test_storage(&temp_dir).await;

        let manager = CheckpointManager::new(storage, "my_events".to_string());
        assert_eq!(
            manager.checkpoint_path().to_string(),
            "_blizzard/my_events_checkpoint.json"
        );
    }
}
