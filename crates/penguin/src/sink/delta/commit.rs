//! Delta Lake commit logic.
//!
//! This module provides functions for committing actions to Delta Lake tables.

use std::time::Instant;

use deltalake::DeltaTable;
use deltalake::kernel::Action;
use deltalake::protocol::SaveMode;
use tracing::debug;

use crate::checkpoint::CheckpointState;
use crate::error::DeltaError;
use crate::metrics::events::{DeltaCommitCompleted, InternalEvent};

use super::actions::create_txn_action;

/// Commit actions to Delta table with optional checkpoint.
///
/// When checkpoint is provided, a Txn action is prepended to the add actions
/// and committed atomically in a single transaction.
pub async fn commit_to_delta_with_checkpoint(
    table: &mut DeltaTable,
    add_actions: Vec<Action>,
    checkpoint: Option<(&CheckpointState, i64)>,
    partition_by: &[String],
    table_name: &str,
) -> Result<i64, DeltaError> {
    use deltalake::kernel::transaction::CommitBuilder;

    let start = Instant::now();

    // Build the complete action list
    let mut all_actions = Vec::with_capacity(add_actions.len() + 1);

    // Add Txn action first if checkpoint provided
    if let Some((state, version)) = checkpoint {
        all_actions.push(create_txn_action(state, version)?);
        debug!(
            target = %table_name,
            "Including checkpoint v{} in commit ({} files)",
            version,
            add_actions.len()
        );
    }

    all_actions.extend(add_actions);

    // Convert partition_by to Option<Vec<String>> for Delta operation
    let partition_by_opt = if partition_by.is_empty() {
        None
    } else {
        Some(partition_by.to_vec())
    };

    let version = CommitBuilder::default()
        .with_actions(all_actions)
        .build(
            Some(
                table
                    .snapshot()
                    .map_err(|source| DeltaError::DeltaOperation { source })?,
            ),
            table.log_store(),
            deltalake::protocol::DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: partition_by_opt,
                predicate: None,
            },
        )
        .await
        .map_err(|source| DeltaError::DeltaOperation { source })?
        .version;

    // Reload table to get new state
    table
        .load()
        .await
        .map_err(|source| DeltaError::DeltaOperation { source })?;

    DeltaCommitCompleted {
        duration: start.elapsed(),
        target: table_name.to_string(),
    }
    .emit();

    Ok(version)
}
