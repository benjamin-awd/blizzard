//! State tracking for processed source files.
//!
//! This module provides two strategies for tracking which files have been processed:
//! - `WatermarkTracker`: Persists a high-watermark to storage, efficient for sorted file names
//! - `HashMapTracker`: Keeps processed files in memory, works with any file naming scheme
//!
//! For multi-source pipelines, `MultiSourceTracker` aggregates per-source trackers.

use async_trait::async_trait;
use blizzard_core::StorageProviderRef;
use blizzard_core::storage::list_ndjson_files_with_prefixes;
use blizzard_core::types::SourceState;
use indexmap::IndexMap;
use tracing::{info, warn};

use crate::checkpoint::CheckpointManager;
use crate::config::SourceConfig;
use crate::error::{ConfigError, PipelineError};
use crate::source::list_ndjson_files_with_partition_watermarks;

/// Trait for tracking which source files have been processed.
#[async_trait]
pub trait StateTracker: Send + Sync {
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

    /// Transition to Idle state when no new files are found.
    ///
    /// Default implementation is a no-op. Watermark trackers override this
    /// to distinguish between "no files exist" vs "files filtered by watermark".
    fn mark_idle(&mut self) {}

    /// Save state to storage (no-op for in-memory trackers).
    async fn save(&self) -> Result<(), PipelineError>;

    /// Get number of tracked files (for metrics).
    fn tracked_count(&self) -> usize;

    /// Describe the mode (for logging).
    fn mode_name(&self) -> &'static str;

    /// Get the current watermark for logging purposes.
    /// Returns None for trackers that don't use watermarks.
    fn watermark(&self) -> Option<&str> {
        None
    }
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
            Ok(true) => Ok(self.checkpoint_manager.watermark().map(|w| {
                format!("Restored checkpoint from storage (watermark: {w})")
            })),
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
        // Use partition watermarks for more efficient per-partition filtering
        list_ndjson_files_with_partition_watermarks(
            storage,
            self.checkpoint_manager.watermark(),
            Some(self.checkpoint_manager.partition_watermarks()),
            prefixes,
            pipeline_key,
        )
        .await
        .map_err(Into::into)
    }

    fn mark_processed(&mut self, path: &str) {
        self.checkpoint_manager.update_watermark(path);
    }

    fn mark_idle(&mut self) {
        self.checkpoint_manager.mark_idle();
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

    fn watermark(&self) -> Option<&str> {
        self.checkpoint_manager.watermark()
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

/// A file with source provenance for multi-source pipelines.
#[derive(Debug, Clone)]
pub struct SourcedFile {
    /// Name of the source this file came from.
    pub source_name: String,
    /// Path to the file within the source.
    pub path: String,
}

/// Aggregates per-source state trackers for multi-source pipelines.
///
/// Each source has its own tracker with independent watermarks/state,
/// allowing sources to progress at different rates.
pub struct MultiSourceTracker {
    /// Per-source trackers, keyed by source name.
    trackers: IndexMap<String, Box<dyn StateTracker>>,
    /// Pipeline key for logging.
    pipeline_key: String,
}

impl MultiSourceTracker {
    /// Create a new multi-source tracker with the given per-source trackers.
    pub fn new(trackers: IndexMap<String, Box<dyn StateTracker>>, pipeline_key: String) -> Self {
        Self {
            trackers,
            pipeline_key,
        }
    }

    /// Initialize all source trackers on cold start.
    pub async fn init_all(
        &mut self,
        configs: &IndexMap<String, &SourceConfig>,
    ) -> Result<(), PipelineError> {
        for (source_name, tracker) in &mut self.trackers {
            let config = configs.get(source_name);
            let source_path = config.map(|c| c.path.as_str()).unwrap_or("unknown");

            match tracker.init().await? {
                Some(msg) => info!(
                    target = %self.pipeline_key,
                    source = %source_name,
                    "{msg}"
                ),
                None => {
                    let prefixes = config.and_then(|c| c.date_prefixes());
                    match prefixes {
                        Some(p) => info!(
                            target = %self.pipeline_key,
                            source = %source_name,
                            mode = tracker.mode_name(),
                            prefixes = ?p,
                            "Cold start - scanning with partition filter"
                        ),
                        None => info!(
                            target = %self.pipeline_key,
                            source = %source_name,
                            mode = tracker.mode_name(),
                            "Cold start - scanning {source_path}"
                        ),
                    }
                }
            }
        }
        Ok(())
    }

    /// List all pending files from all sources.
    ///
    /// Returns files tagged with their source name for proper routing during processing.
    pub async fn list_all_pending(
        &self,
        storages: &IndexMap<String, StorageProviderRef>,
        configs: &IndexMap<String, &SourceConfig>,
    ) -> Result<Vec<SourcedFile>, PipelineError> {
        let mut all_pending = Vec::new();

        for (source_name, tracker) in &self.trackers {
            let storage = storages
                .get(source_name)
                .ok_or_else(|| PipelineError::Config {
                    source: ConfigError::Internal {
                        message: format!("No storage provider for source '{source_name}'"),
                    },
                })?;

            let config = configs
                .get(source_name)
                .ok_or_else(|| PipelineError::Config {
                    source: ConfigError::Internal {
                        message: format!("No config for source '{source_name}'"),
                    },
                })?;

            let prefixes = config.date_prefixes();
            let pending = tracker
                .list_pending(storage, prefixes.as_deref(), &self.pipeline_key)
                .await?;

            all_pending.extend(pending.into_iter().map(|path| SourcedFile {
                source_name: source_name.clone(),
                path,
            }));
        }

        Ok(all_pending)
    }

    /// Mark a file as processed in the appropriate source tracker.
    pub fn mark_processed(&mut self, source_name: &str, path: &str) {
        if let Some(tracker) = self.trackers.get_mut(source_name) {
            tracker.mark_processed(path);
        } else {
            warn!(
                target = %self.pipeline_key,
                source = %source_name,
                path = %path,
                "Attempted to mark file processed for unknown source"
            );
        }
    }

    /// Mark all trackers as idle when no new files are found.
    ///
    /// Called when prepare() returns an empty pending file list to distinguish
    /// between "no files exist" vs "all files filtered by watermark".
    pub fn mark_all_idle(&mut self) {
        for tracker in self.trackers.values_mut() {
            tracker.mark_idle();
        }
    }

    /// Save all source trackers.
    pub async fn save_all(&self) -> Result<(), PipelineError> {
        for (source_name, tracker) in &self.trackers {
            match tracker.save().await {
                Ok(()) => {
                    if let Some(wm) = tracker.watermark() {
                        info!(
                            target = %self.pipeline_key,
                            source = %source_name,
                            watermark = %wm,
                            "Saved checkpoint"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        target = %self.pipeline_key,
                        source = %source_name,
                        error = %e,
                        "Failed to save tracker state"
                    );
                }
            }
        }
        Ok(())
    }

    /// Get total tracked count across all sources.
    pub fn tracked_count(&self) -> usize {
        self.trackers.values().map(|t| t.tracked_count()).sum()
    }
}
