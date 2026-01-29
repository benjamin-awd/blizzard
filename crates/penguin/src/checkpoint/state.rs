//! Checkpoint state serialization.
//!
//! Defines the checkpoint state structure that captures
//! all information needed for recovery.

use serde::{Deserialize, Serialize};

use blizzard_common::types::SourceState;

/// Default schema version for checkpoint state.
fn default_schema_version() -> u32 {
    1
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
}

impl Default for CheckpointState {
    fn default() -> Self {
        Self {
            schema_version: 1,
            source_state: SourceState::new(),
            delta_version: -1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_state_default() {
        let state = CheckpointState::default();
        assert_eq!(state.schema_version, 1);
        assert_eq!(state.delta_version, -1);
        assert!(state.source_state.files.is_empty());
    }
}
