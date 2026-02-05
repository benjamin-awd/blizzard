//! Checkpoint state for source file tracking.
//!
//! Defines the checkpoint state structure that captures watermark position
//! for efficient incremental file discovery.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::debug;

/// Default schema version for checkpoint state.
fn default_schema_version() -> u32 {
    1
}

/// Watermark state for operational visibility.
///
/// Tracks not just the position but also the activity state:
/// - `Initial`: Cold start, no watermark yet
/// - `Active`: Actively processing files at a specific position
/// - `Idle`: No new files found above the current watermark
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", content = "value")]
#[derive(Default)]
pub enum WatermarkState {
    /// Cold start - no watermark yet.
    #[default]
    Initial,
    /// Active processing - at specific file path.
    Active(String),
    /// Idle - no new files found above watermark.
    Idle(String),
}


impl WatermarkState {
    /// Extract the path from Active or Idle states.
    pub fn path(&self) -> Option<&str> {
        match self {
            WatermarkState::Initial => None,
            WatermarkState::Active(path) | WatermarkState::Idle(path) => Some(path),
        }
    }

    /// Check if this is the Initial state.
    pub fn is_initial(&self) -> bool {
        matches!(self, WatermarkState::Initial)
    }

    /// Transition to Idle state if currently Active.
    /// Returns true if the state was changed.
    pub fn mark_idle(&mut self) -> bool {
        match self {
            WatermarkState::Active(path) => {
                let path = std::mem::take(path);
                *self = WatermarkState::Idle(path);
                true
            }
            _ => false,
        }
    }
}

/// Checkpoint state for Blizzard source file tracking.
///
/// Uses a high-watermark approach to track the last processed file path.
/// Files are lexicographically sortable (e.g., using UUIDv7 or timestamp prefixes),
/// so we only need to list files above the watermark on each poll.
///
/// # Checkpoint File Location
///
/// Stored at: `{table_uri}/_blizzard/{pipeline_key}_checkpoint.json`
///
/// # Example
///
/// ```json
/// {
///   "schema_version": 1,
///   "watermark": {"state": "Active", "value": "date=2026-01-28/1738100400-uuid.ndjson.gz"},
///   "partition_watermarks": {
///     "date=2026-01-28": "1738100400-uuid.ndjson.gz",
///     "date=2026-01-29": "1738186800-uuid.ndjson.gz"
///   },
///   "last_update_ts": 1738100500
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointState {
    /// Schema version for forward compatibility.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// High-watermark state: tracks position and activity state.
    /// e.g., Active("date=2026-01-28/1738100400-uuid.ndjson.gz")
    #[serde(default)]
    pub watermark: WatermarkState,
    /// Per-partition watermarks for efficient filtering of concurrent partitions.
    /// Maps partition prefix (e.g., "date=2026-01-28") to filename watermark.
    #[serde(default)]
    pub partition_watermarks: HashMap<String, String>,
    /// Unix timestamp of last checkpoint update.
    #[serde(default)]
    pub last_update_ts: i64,
}

impl Default for CheckpointState {
    fn default() -> Self {
        Self {
            schema_version: 0, // Will be set to 1 by serde default when deserializing
            watermark: WatermarkState::Initial,
            partition_watermarks: HashMap::new(),
            last_update_ts: 0,
        }
    }
}

impl CheckpointState {
    /// Create a new checkpoint state with a watermark.
    pub fn with_watermark(watermark: String) -> Self {
        let (partition, filename) = crate::watermark::parse_watermark(&watermark);
        let mut partition_watermarks = HashMap::new();
        if !partition.is_empty() {
            partition_watermarks.insert(partition, filename);
        }

        Self {
            schema_version: 1,
            watermark: WatermarkState::Active(watermark),
            partition_watermarks,
            last_update_ts: chrono::Utc::now().timestamp(),
        }
    }

    /// Update the watermark if the new value is greater.
    ///
    /// This method:
    /// 1. Updates the partition-specific watermark for efficient per-partition filtering
    /// 2. Updates the global watermark for backward compat and progress tracking
    /// 3. Transitions Idle → Active when new files are processed
    ///
    /// Returns true if the watermark was updated.
    pub fn update_watermark(&mut self, new_watermark: &str) -> bool {
        let (partition, filename) = crate::watermark::parse_watermark(new_watermark);

        // Update partition-specific watermark
        let partition_updated = self.update_partition_watermark(&partition, &filename);

        // Update global watermark
        let global_updated = self.update_global_watermark(new_watermark);

        if partition_updated || global_updated {
            self.last_update_ts = chrono::Utc::now().timestamp();
        }

        partition_updated || global_updated
    }

    /// Update the partition-specific watermark if the new filename is greater.
    fn update_partition_watermark(&mut self, partition: &str, filename: &str) -> bool {
        if partition.is_empty() {
            return false;
        }

        let should_update = match self.partition_watermarks.get(partition) {
            None => true,
            Some(current) => filename > current.as_str(),
        };

        if should_update {
            debug!(
                partition = %partition,
                filename = %filename,
                "Partition watermark updated"
            );
            self.partition_watermarks
                .insert(partition.to_string(), filename.to_string());
        }

        should_update
    }

    /// Update the global watermark if the new value is greater.
    fn update_global_watermark(&mut self, new_watermark: &str) -> bool {
        let current_path = self.watermark.path();
        let should_update = match current_path {
            None => true,
            Some(current) => new_watermark > current,
        };

        if should_update {
            if let Some(old) = current_path {
                debug!(
                    old = %old,
                    new = %new_watermark,
                    "Watermark advanced"
                );
            }
            // Always transition to Active when processing new files
            self.watermark = WatermarkState::Active(new_watermark.to_string());
        }

        should_update
    }

    /// Transition to Idle state if currently Active.
    ///
    /// Called when no new files are found above the watermark.
    /// Returns true if the state was changed.
    pub fn mark_idle(&mut self) -> bool {
        self.watermark.mark_idle()
    }

    /// Get the watermark path for filtering (works for both Active and Idle states).
    pub fn watermark_path(&self) -> Option<&str> {
        self.watermark.path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_state_default() {
        let state = CheckpointState::default();
        assert_eq!(state.schema_version, 0); // Default is 0, then serde default kicks in
        assert!(state.watermark.is_initial());
        assert!(state.partition_watermarks.is_empty());
        assert_eq!(state.last_update_ts, 0);
    }

    #[test]
    fn test_checkpoint_state_serialization() {
        let mut state = CheckpointState::default();
        state.schema_version = 1;
        state.update_watermark("date=2026-01-28/1738100400-uuid.ndjson.gz");
        state.last_update_ts = 1738100500;

        let json = serde_json::to_string_pretty(&state).unwrap();
        let restored: CheckpointState = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.schema_version, 1);
        assert_eq!(
            restored.watermark,
            WatermarkState::Active("date=2026-01-28/1738100400-uuid.ndjson.gz".to_string())
        );
        assert_eq!(
            restored.partition_watermarks.get("date=2026-01-28"),
            Some(&"1738100400-uuid.ndjson.gz".to_string())
        );
        assert_eq!(restored.last_update_ts, 1738100500);
    }

    #[test]
    fn test_update_watermark_from_initial() {
        let mut state = CheckpointState::default();
        assert!(state.update_watermark("date=2026-01-28/file1.ndjson.gz"));
        assert_eq!(
            state.watermark,
            WatermarkState::Active("date=2026-01-28/file1.ndjson.gz".to_string())
        );
        assert_eq!(
            state.partition_watermarks.get("date=2026-01-28"),
            Some(&"file1.ndjson.gz".to_string())
        );
    }

    #[test]
    fn test_update_watermark_greater() {
        let mut state =
            CheckpointState::with_watermark("date=2026-01-28/file1.ndjson.gz".to_string());
        assert!(state.update_watermark("date=2026-01-28/file2.ndjson.gz"));
        assert_eq!(
            state.watermark,
            WatermarkState::Active("date=2026-01-28/file2.ndjson.gz".to_string())
        );
        assert_eq!(
            state.partition_watermarks.get("date=2026-01-28"),
            Some(&"file2.ndjson.gz".to_string())
        );
    }

    #[test]
    fn test_update_watermark_not_greater() {
        let mut state =
            CheckpointState::with_watermark("date=2026-01-28/file2.ndjson.gz".to_string());
        assert!(!state.update_watermark("date=2026-01-28/file1.ndjson.gz"));
        assert_eq!(
            state.watermark,
            WatermarkState::Active("date=2026-01-28/file2.ndjson.gz".to_string())
        );
    }

    #[test]
    fn test_update_watermark_equal() {
        let mut state =
            CheckpointState::with_watermark("date=2026-01-28/file1.ndjson.gz".to_string());
        assert!(!state.update_watermark("date=2026-01-28/file1.ndjson.gz"));
    }

    #[test]
    fn test_mark_idle_from_active() {
        let mut state =
            CheckpointState::with_watermark("date=2026-01-28/file1.ndjson.gz".to_string());
        assert!(state.mark_idle());
        assert_eq!(
            state.watermark,
            WatermarkState::Idle("date=2026-01-28/file1.ndjson.gz".to_string())
        );
    }

    #[test]
    fn test_mark_idle_from_idle() {
        let mut state =
            CheckpointState::with_watermark("date=2026-01-28/file1.ndjson.gz".to_string());
        state.mark_idle();
        // Second call should return false
        assert!(!state.mark_idle());
    }

    #[test]
    fn test_mark_idle_from_initial() {
        let mut state = CheckpointState::default();
        // Cannot mark idle from Initial state
        assert!(!state.mark_idle());
    }

    #[test]
    fn test_idle_to_active_on_update() {
        let mut state =
            CheckpointState::with_watermark("date=2026-01-28/file1.ndjson.gz".to_string());
        state.mark_idle();
        assert!(matches!(state.watermark, WatermarkState::Idle(_)));

        // New file should transition back to Active
        state.update_watermark("date=2026-01-28/file2.ndjson.gz");
        assert_eq!(
            state.watermark,
            WatermarkState::Active("date=2026-01-28/file2.ndjson.gz".to_string())
        );
    }

    #[test]
    fn test_watermark_path() {
        let mut state = CheckpointState::default();
        assert!(state.watermark_path().is_none());

        state.update_watermark("date=2026-01-28/file1.ndjson.gz");
        assert_eq!(
            state.watermark_path(),
            Some("date=2026-01-28/file1.ndjson.gz")
        );

        state.mark_idle();
        // Path is still accessible when Idle
        assert_eq!(
            state.watermark_path(),
            Some("date=2026-01-28/file1.ndjson.gz")
        );
    }

    #[test]
    fn test_partition_watermarks_multiple_partitions() {
        let mut state = CheckpointState::default();

        // Process files in first partition
        state.update_watermark("date=2026-01-28/file1.ndjson.gz");
        state.update_watermark("date=2026-01-28/file2.ndjson.gz");

        // Process files in second partition
        state.update_watermark("date=2026-01-29/file1.ndjson.gz");

        // Both partitions should have their own watermarks
        assert_eq!(
            state.partition_watermarks.get("date=2026-01-28"),
            Some(&"file2.ndjson.gz".to_string())
        );
        assert_eq!(
            state.partition_watermarks.get("date=2026-01-29"),
            Some(&"file1.ndjson.gz".to_string())
        );

        // Global watermark should be the highest overall
        assert_eq!(
            state.watermark_path(),
            Some("date=2026-01-29/file1.ndjson.gz")
        );
    }

    #[test]
    fn test_partition_watermarks_no_partition() {
        let mut state = CheckpointState::default();

        // Root-level file (no partition)
        state.update_watermark("file1.ndjson.gz");

        // No partition watermark for root-level files
        assert!(state.partition_watermarks.is_empty());

        // Global watermark should still work
        assert_eq!(state.watermark_path(), Some("file1.ndjson.gz"));
    }
}
