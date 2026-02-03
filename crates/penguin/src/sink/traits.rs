//! Traits for table sink abstractions.
//!
//! This module defines the `TableSink` trait that abstracts table operations,
//! enabling dependency inversion and support for multiple table formats.

use async_trait::async_trait;
use deltalake::arrow::datatypes::{Schema, SchemaRef};
use std::collections::HashSet;

use blizzard_core::FinishedFile;

use crate::checkpoint::CheckpointState;
use crate::error::{DeltaError, SchemaError};
use crate::schema::evolution::{EvolutionAction, SchemaEvolutionMode};

/// Trait for table sinks that commit files to a table format.
///
/// This abstraction allows the pipeline to work with different table formats
/// (Delta Lake, Iceberg, Hudi, etc.) without depending on concrete implementations.
#[async_trait]
pub trait TableSink: Send + Sync {
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

    /// Get the current table version.
    fn version(&self) -> i64;

    /// Get the current checkpoint version.
    fn checkpoint_version(&self) -> i64;

    /// Get the cached table schema, if available.
    fn schema(&self) -> Option<&SchemaRef>;

    /// Get all committed file paths from the table.
    ///
    /// Returns a set of paths for all files currently in the table.
    /// Used to avoid double-commits.
    fn get_committed_paths(&self) -> HashSet<String>;

    /// Recover checkpoint state from the table's transaction log.
    ///
    /// Scans the transaction log looking for embedded checkpoint state.
    /// Returns `Some((checkpoint_state, checkpoint_version))` if found.
    async fn recover_checkpoint_from_log(
        &mut self,
    ) -> Result<Option<(CheckpointState, i64)>, DeltaError>;

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

    /// Create a checkpoint file for the table if supported.
    ///
    /// Some table formats (like Delta Lake) support checkpoint files that
    /// summarize the table state for faster reads. This method creates
    /// such a checkpoint if the format supports it.
    async fn create_checkpoint(&self) -> Result<(), DeltaError>;

    /// Get the table name/identifier for logging and metrics.
    fn table_name(&self) -> &str;
}
