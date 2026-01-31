//! Checkpoint state serialization.
//!
//! Defines the checkpoint state structure that captures
//! all information needed for recovery.

use serde::{Deserialize, Serialize};

use blizzard_common::types::SourceState;

/// Default schema version for checkpoint state.
fn default_schema_version() -> u32 {
    2
}

/// Complete checkpoint state for recovery.
///
/// With atomic Txn-based checkpointing, the checkpoint is stored alongside
/// Add actions in a single Delta commit. This eliminates the need for
/// separate pending_files tracking since files and checkpoint state are
/// committed atomically.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointState {
    /// Schema version for forward compatibility.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// State of source file processing.
    pub source_state: SourceState,
    /// Last committed Delta version.
    pub delta_version: i64,
    /// High-watermark: last committed file path (lexicographical) for incoming mode.
    /// e.g., "date=2024-01-28/01926abc-def0-7123-...parquet"
    #[serde(default)]
    pub watermark: Option<String>,
}

impl Default for CheckpointState {
    fn default() -> Self {
        Self {
            schema_version: 2,
            source_state: SourceState::new(),
            delta_version: -1,
            watermark: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_state_default() {
        let state = CheckpointState::default();
        assert_eq!(state.schema_version, 2);
        assert_eq!(state.delta_version, -1);
        assert!(state.source_state.files.is_empty());
        assert!(state.watermark.is_none());
    }

    #[test]
    fn test_checkpoint_state_with_watermark() {
        let state = CheckpointState {
            schema_version: 2,
            source_state: SourceState::new(),
            delta_version: 5,
            watermark: Some(
                "date=2024-01-28/01926abc-def0-7123-4567-89abcdef0123.parquet".to_string(),
            ),
        };

        let json = serde_json::to_string(&state).unwrap();
        let restored: CheckpointState = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.watermark, state.watermark);
    }

    #[test]
    fn test_checkpoint_state_backwards_compatible() {
        // Old checkpoint without watermark should deserialize with watermark = None
        let json = r#"{"schema_version":1,"source_state":{"files":{}},"delta_version":3}"#;
        let state: CheckpointState = serde_json::from_str(json).unwrap();

        assert_eq!(state.schema_version, 1);
        assert_eq!(state.delta_version, 3);
        assert!(state.watermark.is_none());
    }
}
