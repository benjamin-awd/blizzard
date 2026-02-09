//! Traits for table sink abstractions.
//!
//! This module defines focused sub-traits for table operations, grouped by concern:
//! - [`TableCommitter`]: Commit files and manage versions
//! - [`SchemaEvolution`]: Schema validation and evolution
//! - [`CheckpointRecovery`]: Checkpoint recovery and deduplication
//!
//! [`TableSink`] combines all three as a supertrait, preserving a single `dyn TableSink`
//! for code that needs the full interface.

use async_trait::async_trait;
use deltalake::arrow::datatypes::{Schema, SchemaRef};
use std::collections::HashSet;

use blizzard_core::FinishedFile;

use crate::checkpoint::CheckpointState;
use crate::error::{DeltaError, SchemaError};
use crate::schema::evolution::{EvolutionAction, SchemaEvolutionMode};

/// Commit operations: committing files and managing table/checkpoint versions.
#[async_trait]
pub trait TableCommitter: Send + Sync {
    /// Commit files with an atomic checkpoint.
    ///
    /// The checkpoint state is committed atomically with the file additions,
    /// ensuring exactly-once semantics.
    ///
    /// Returns the new version number if a commit was made.
    async fn commit_files_with_checkpoint(
        &mut self,
        files: &[FinishedFile],
        checkpoint: &CheckpointState,
    ) -> Result<Option<i64>, DeltaError>;

    /// Create a checkpoint file for the table if supported.
    ///
    /// Some table formats (like Delta Lake) support checkpoint files that
    /// summarize the table state for faster reads.
    async fn create_checkpoint(&self) -> Result<(), DeltaError>;

    /// Get the current table version.
    fn version(&self) -> i64;

    /// Get the current checkpoint version.
    fn checkpoint_version(&self) -> i64;
}

/// Schema operations: validation and evolution.
#[async_trait]
pub trait SchemaEvolution: Send + Sync {
    /// Get the cached table schema, if available.
    fn schema(&self) -> Option<&SchemaRef>;

    /// Validate an incoming schema against the table schema.
    ///
    /// Returns the evolution action to take based on the configured mode.
    fn validate_schema(
        &self,
        incoming: &Schema,
        mode: SchemaEvolutionMode,
    ) -> Result<EvolutionAction, SchemaError>;

    /// Apply a schema evolution action to the table.
    ///
    /// For `Merge` and `Overwrite` actions, this updates the table metadata
    /// with the new schema.
    async fn evolve_schema(&mut self, action: EvolutionAction) -> Result<(), DeltaError>;
}

/// Recovery and deduplication: checkpoint recovery from transaction logs.
#[async_trait]
pub trait CheckpointRecovery: Send + Sync {
    /// Recover checkpoint state from the table's transaction log.
    ///
    /// Scans the transaction log looking for embedded checkpoint state.
    /// Returns `Some((checkpoint_state, checkpoint_version))` if found.
    async fn recover_checkpoint_from_log(
        &mut self,
    ) -> Result<Option<(CheckpointState, i64)>, DeltaError>;

    /// Get all committed file paths from the table.
    ///
    /// Returns a set of paths for all files currently in the table.
    /// Used to avoid double-commits.
    fn get_committed_paths(&self) -> HashSet<String>;
}

/// Combined trait for table sinks that support all operations.
///
/// This supertrait combines [`TableCommitter`], [`SchemaEvolution`], and
/// [`CheckpointRecovery`], providing a single `dyn TableSink` for code that
/// needs the full interface.
#[async_trait]
pub trait TableSink: TableCommitter + SchemaEvolution + CheckpointRecovery {
    /// Get the table name/identifier for logging and metrics.
    fn table_name(&self) -> &str;
}
