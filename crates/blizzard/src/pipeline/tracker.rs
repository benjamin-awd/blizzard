//! State tracking for processed source files.
//!
//! This module provides two strategies for tracking which files have been processed:
//! - `WatermarkTracker`: Persists a high-watermark to storage, efficient for sorted file names
//! - `HashMapTracker`: Keeps processed files in memory, works with any file naming scheme

use async_trait::async_trait;
use blizzard_common::storage::list_ndjson_files_with_prefixes;
use blizzard_common::types::SourceState;
use blizzard_common::StorageProviderRef;
use tracing::warn;

use crate::checkpoint::CheckpointManager;
use crate::error::PipelineError;
use crate::source::list_ndjson_files_above_watermark;

/// Trait for tracking which source files have been processed.
#[async_trait]
pub trait StateTracker: Send {
    /// Initialize on cold start - load state from storage if available.
    /// Returns a message describing the initialization result for logging.
    async fn init(&mut self) -> Result<Option<String>, PipelineError>;

    /// List pending files from storage, filtering out already-processed ones.
    async fn list_pending(
        &self,
        storage: &StorageProviderRef,
        prefixes: Option<&[String]>,
        pipeline_key: &str,
    ) -> Result<Vec<String>, PipelineError>;

    /// Mark a file as processed.
    fn mark_processed(&mut self, path: &str);

    /// Save state to storage (no-op for in-memory trackers).
    async fn save(&self) -> Result<(), PipelineError>;

    /// Get number of tracked files (for metrics).
    fn tracked_count(&self) -> usize;

    /// Describe the mode (for logging).
    fn mode_name(&self) -> &'static str;
}

/// Watermark-based state tracker that persists to storage.
///
/// Tracks progress by storing the lexicographically highest processed file path.
/// Efficient when source files are named in sorted order (e.g., by timestamp).
pub struct WatermarkTracker {
    checkpoint_manager: CheckpointManager,
}

impl WatermarkTracker {
    pub fn new(checkpoint_manager: CheckpointManager) -> Self {
        Self { checkpoint_manager }
    }
}

#[async_trait]
impl StateTracker for WatermarkTracker {
    async fn init(&mut self) -> Result<Option<String>, PipelineError> {
        match self.checkpoint_manager.load().await {
            Ok(true) => Ok(Some(format!(
                "Restored checkpoint from storage (watermark: {:?})",
                self.checkpoint_manager.watermark()
            ))),
            Ok(false) => Ok(None),
            Err(e) => {
                warn!(error = %e, "Failed to load checkpoint, starting fresh");
                Ok(None)
            }
        }
    }

    async fn list_pending(
        &self,
        storage: &StorageProviderRef,
        prefixes: Option<&[String]>,
        pipeline_key: &str,
    ) -> Result<Vec<String>, PipelineError> {
        list_ndjson_files_above_watermark(
            storage,
            self.checkpoint_manager.watermark(),
            prefixes,
            pipeline_key,
        )
        .await
        .map_err(Into::into)
    }

    fn mark_processed(&mut self, path: &str) {
        self.checkpoint_manager.update_watermark(path);
    }

    async fn save(&self) -> Result<(), PipelineError> {
        self.checkpoint_manager.save().await.map_err(|e| {
            warn!(error = %e, "Failed to save checkpoint");
            e.into()
        })
    }

    fn tracked_count(&self) -> usize {
        0 // Watermark mode doesn't track individual files
    }

    fn mode_name(&self) -> &'static str {
        "watermark"
    }
}

/// In-memory hash map state tracker.
///
/// Keeps track of all processed files in memory. Works with any file naming
/// scheme but doesn't persist across restarts.
pub struct HashMapTracker {
    source_state: SourceState,
}

impl HashMapTracker {
    pub fn new() -> Self {
        Self {
            source_state: SourceState::new(),
        }
    }
}

impl Default for HashMapTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateTracker for HashMapTracker {
    async fn init(&mut self) -> Result<Option<String>, PipelineError> {
        Ok(None) // No persistent state to load
    }

    async fn list_pending(
        &self,
        storage: &StorageProviderRef,
        prefixes: Option<&[String]>,
        pipeline_key: &str,
    ) -> Result<Vec<String>, PipelineError> {
        let all_files = list_ndjson_files_with_prefixes(storage, prefixes, pipeline_key).await?;
        Ok(self.source_state.filter_pending_files(all_files))
    }

    fn mark_processed(&mut self, path: &str) {
        self.source_state.mark_finished(path);
    }

    async fn save(&self) -> Result<(), PipelineError> {
        Ok(()) // No-op for in-memory tracker
    }

    fn tracked_count(&self) -> usize {
        self.source_state.files.len()
    }

    fn mode_name(&self) -> &'static str {
        "hashmap"
    }
}
