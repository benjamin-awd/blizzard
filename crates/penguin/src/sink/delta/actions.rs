//! Delta Lake action creation.
//!
//! This module provides functions for creating Delta Lake actions:
//! - Add actions for adding files to the table
//! - Txn actions for embedding checkpoint state

use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use deltalake::kernel::Action;
use tracing::debug;

use blizzard_core::FinishedFile;

use crate::checkpoint::CheckpointState;
use crate::error::DeltaError;
use crate::metrics::events::{CheckpointStateSize, InternalEvent};

/// Prefix for Blizzard checkpoint app_id in Delta Txn actions.
pub const TXN_APP_ID_PREFIX: &str = "blizzard:";

/// Create a Delta Lake Add action for a finished file.
pub fn create_add_action(file: &FinishedFile) -> Action {
    use deltalake::kernel::Add;
    use std::collections::HashMap;

    debug!("Creating add action for file {:?}", file);

    let subpath = file.filename.trim_start_matches('/');

    // Convert partition_values to Option<String> as required by Delta Lake
    let partition_values: HashMap<String, Option<String>> = file
        .partition_values
        .iter()
        .map(|(k, v)| (k.clone(), Some(v.clone())))
        .collect();

    Action::Add(Add {
        path: subpath.to_string(),
        size: i64::try_from(file.size).expect("file size should fit in i64"),
        partition_values,
        modification_time: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| {
                i64::try_from(d.as_millis()).expect("modification time in millis should fit in i64")
            })
            .unwrap_or(0),
        data_change: true,
        ..Default::default()
    })
}

/// Create a Delta Lake Txn action with embedded checkpoint state.
///
/// The checkpoint state is serialized to JSON and base64-encoded in the app_id field.
/// Format: `blizzard:{base64_encoded_checkpoint_json}`
pub fn create_txn_action(
    checkpoint_state: &CheckpointState,
    version: i64,
) -> Result<Action, DeltaError> {
    use deltalake::kernel::Transaction;

    let checkpoint_json = serde_json::to_string(checkpoint_state)
        .map_err(|source| DeltaError::CheckpointJsonEncode { source })?;

    // Track checkpoint state size for memory monitoring
    CheckpointStateSize {
        bytes: checkpoint_json.len(),
    }
    .emit();

    let encoded = base64::engine::general_purpose::STANDARD.encode(&checkpoint_json);
    let app_id = format!("{TXN_APP_ID_PREFIX}{encoded}");

    Ok(Action::Txn(Transaction {
        app_id,
        version,
        last_updated: Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| {
                    i64::try_from(d.as_millis())
                        .expect("modification time in millis should fit in i64")
                })
                .unwrap_or(0),
        ),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use blizzard_core::types::SourceState;
    use blizzard_core::watermark::WatermarkState;
    use std::collections::HashMap;

    #[test]
    fn test_create_add_action() {
        let file = FinishedFile {
            filename: "test-file.parquet".to_string(),
            size: 1024,
            record_count: 100,
            bytes: None,
            partition_values: HashMap::new(),
            source_file: None,
        };

        let action = create_add_action(&file);

        match action {
            Action::Add(add) => {
                assert_eq!(add.path, "test-file.parquet");
                assert_eq!(add.size, 1024);
                assert!(add.data_change);
                assert!(add.partition_values.is_empty());
            }
            _ => panic!("Expected Add action"),
        }
    }

    #[test]
    fn test_create_add_action_strips_leading_slash() {
        let file = FinishedFile {
            filename: "/path/to/file.parquet".to_string(),
            size: 2048,
            record_count: 200,
            bytes: None,
            partition_values: HashMap::new(),
            source_file: None,
        };

        let action = create_add_action(&file);

        match action {
            Action::Add(add) => {
                assert_eq!(add.path, "path/to/file.parquet");
            }
            _ => panic!("Expected Add action"),
        }
    }

    #[test]
    fn test_create_add_action_with_partition_values() {
        let mut partition_values = HashMap::new();
        partition_values.insert("date".to_string(), "2026-01-28".to_string());

        let file = FinishedFile {
            filename: "date=2026-01-28/test-file.parquet".to_string(),
            size: 1024,
            record_count: 100,
            bytes: None,
            partition_values,
            source_file: None,
        };

        let action = create_add_action(&file);

        match action {
            Action::Add(add) => {
                assert_eq!(add.path, "date=2026-01-28/test-file.parquet");
                assert_eq!(
                    add.partition_values.get("date"),
                    Some(&Some("2026-01-28".to_string()))
                );
            }
            _ => panic!("Expected Add action"),
        }
    }

    #[test]
    fn test_create_txn_action() {
        let mut source_state = SourceState::new();
        source_state.mark_finished("file1.ndjson.gz");

        let checkpoint = CheckpointState {
            schema_version: 2,
            source_state,
            delta_version: 5,
            watermark: WatermarkState::Initial,
        };

        let action = create_txn_action(&checkpoint, 42).unwrap();

        match action {
            Action::Txn(txn) => {
                assert!(txn.app_id.starts_with(TXN_APP_ID_PREFIX));
                assert_eq!(txn.version, 42);
                assert!(txn.last_updated.is_some());
            }
            _ => panic!("Expected Txn action"),
        }
    }

    #[test]
    fn test_txn_action_roundtrip() {
        let mut source_state = SourceState::new();
        source_state.mark_finished("file1.ndjson.gz");
        source_state.mark_finished("file2.ndjson.gz");

        let original = CheckpointState {
            schema_version: 2,
            source_state,
            delta_version: 10,
            watermark: WatermarkState::active("date=2024-01-28/uuid.parquet"),
        };

        // Create Txn action
        let action = create_txn_action(&original, 1).unwrap();

        // Extract and decode
        if let Action::Txn(txn) = action {
            let encoded = txn.app_id.strip_prefix(TXN_APP_ID_PREFIX).unwrap();
            let json_bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .unwrap();
            let restored: CheckpointState = serde_json::from_slice(&json_bytes).unwrap();

            assert_eq!(restored.schema_version, original.schema_version);
            assert_eq!(restored.delta_version, original.delta_version);
            assert!(restored.source_state.is_file_finished("file2.ndjson.gz"));
            assert_eq!(restored.watermark, original.watermark);
        } else {
            panic!("Expected Txn action");
        }
    }
}
