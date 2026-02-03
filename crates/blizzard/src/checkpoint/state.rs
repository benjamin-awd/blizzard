//! Checkpoint state for source file tracking.
//!
//! Defines the checkpoint state structure that captures watermark position
//! for efficient incremental file discovery.

use serde::{Deserialize, Serialize};

/// Default schema version for checkpoint state.
fn default_schema_version() -> u32 {
    1
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
///   "watermark": "date=2026-01-28/1738100400-550e8400-e29b-41d4-a716-446655440000.ndjson.gz",
///   "last_update_ts": 1738100500
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CheckpointState {
    /// Schema version for forward compatibility.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// High-watermark: last processed file path (lexicographical).
    /// e.g., "date=2026-01-28/1738100400-uuid.ndjson.gz"
    #[serde(default)]
    pub watermark: Option<String>,
    /// Unix timestamp of last checkpoint update.
    #[serde(default)]
    pub last_update_ts: i64,
}

impl CheckpointState {
    /// Create a new checkpoint state with a watermark.
    pub fn with_watermark(watermark: String) -> Self {
        Self {
            schema_version: 1,
            watermark: Some(watermark),
            last_update_ts: chrono::Utc::now().timestamp(),
        }
    }

    /// Update the watermark if the new value is greater.
    ///
    /// Returns true if the watermark was updated.
    pub fn update_watermark(&mut self, new_watermark: &str) -> bool {
        let should_update = match &self.watermark {
            None => true,
            Some(current) => new_watermark > current.as_str(),
        };

        if should_update {
            self.watermark = Some(new_watermark.to_string());
            self.last_update_ts = chrono::Utc::now().timestamp();
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_state_default() {
        let state = CheckpointState::default();
        assert_eq!(state.schema_version, 0); // Default is 0, then serde default kicks in
        assert!(state.watermark.is_none());
        assert_eq!(state.last_update_ts, 0);
    }

    #[test]
    fn test_checkpoint_state_serialization() {
        let state = CheckpointState {
            schema_version: 1,
            watermark: Some("date=2026-01-28/1738100400-uuid.ndjson.gz".to_string()),
            last_update_ts: 1738100500,
        };

        let json = serde_json::to_string_pretty(&state).unwrap();
        let restored: CheckpointState = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.schema_version, 1);
        assert_eq!(
            restored.watermark,
            Some("date=2026-01-28/1738100400-uuid.ndjson.gz".to_string())
        );
        assert_eq!(restored.last_update_ts, 1738100500);
    }

    #[test]
    fn test_checkpoint_state_backwards_compatible() {
        // Old checkpoint without watermark should deserialize with watermark = None
        let json = r#"{"schema_version":1}"#;
        let state: CheckpointState = serde_json::from_str(json).unwrap();

        assert_eq!(state.schema_version, 1);
        assert!(state.watermark.is_none());
        assert_eq!(state.last_update_ts, 0);
    }

    #[test]
    fn test_update_watermark_from_none() {
        let mut state = CheckpointState::default();
        assert!(state.update_watermark("date=2026-01-28/file1.ndjson.gz"));
        assert_eq!(
            state.watermark,
            Some("date=2026-01-28/file1.ndjson.gz".to_string())
        );
    }

    #[test]
    fn test_update_watermark_greater() {
        let mut state =
            CheckpointState::with_watermark("date=2026-01-28/file1.ndjson.gz".to_string());
        assert!(state.update_watermark("date=2026-01-28/file2.ndjson.gz"));
        assert_eq!(
            state.watermark,
            Some("date=2026-01-28/file2.ndjson.gz".to_string())
        );
    }

    #[test]
    fn test_update_watermark_not_greater() {
        let mut state =
            CheckpointState::with_watermark("date=2026-01-28/file2.ndjson.gz".to_string());
        assert!(!state.update_watermark("date=2026-01-28/file1.ndjson.gz"));
        assert_eq!(
            state.watermark,
            Some("date=2026-01-28/file2.ndjson.gz".to_string())
        );
    }

    #[test]
    fn test_update_watermark_equal() {
        let mut state =
            CheckpointState::with_watermark("date=2026-01-28/file1.ndjson.gz".to_string());
        assert!(!state.update_watermark("date=2026-01-28/file1.ndjson.gz"));
    }
}
