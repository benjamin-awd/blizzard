//! Delta Lake commit logic.
//!
//! This module provides functions for committing actions to Delta Lake tables.

use std::time::{Duration, Instant};

use deltalake::DeltaTable;
use deltalake::kernel::Action;
use deltalake::protocol::SaveMode;
use tracing::debug;

use crate::checkpoint::CheckpointState;
use crate::error::DeltaError;
use crate::metrics::events::{DeltaCommitCompleted, InternalEvent};

use super::actions::create_txn_action;

/// Timeout for Delta commit operations (commit + table reload).
const COMMIT_TIMEOUT: Duration = Duration::from_secs(120);

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
    let partition_by_opt = (!partition_by.is_empty()).then(|| partition_by.to_vec());

    let version = tokio::time::timeout(
        COMMIT_TIMEOUT,
        CommitBuilder::default().with_actions(all_actions).build(
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
        ),
    )
    .await
    .map_err(|_| DeltaError::Timeout {
        operation: "delta commit".to_string(),
        seconds: COMMIT_TIMEOUT.as_secs(),
    })?
    .map_err(|source| DeltaError::DeltaOperation { source })?
    .version;

    // Reload table to get new state (with timeout)
    tokio::time::timeout(COMMIT_TIMEOUT, table.load())
        .await
        .map_err(|_| DeltaError::Timeout {
            operation: "table reload after commit".to_string(),
            seconds: COMMIT_TIMEOUT.as_secs(),
        })?
        .map_err(|source| DeltaError::DeltaOperation { source })?;

    DeltaCommitCompleted {
        duration: start.elapsed(),
        target: table_name.to_string(),
    }
    .emit();

    Ok(version)
}
