//! Delta Lake sink for committing Parquet files.
//!
//! Handles creating/opening Delta Lake tables and committing
//! Parquet files with exactly-once semantics.
//!
//! # Atomic Checkpointing
//!
//! This module uses Delta Lake's `Txn` action to achieve atomic checkpointing.
//! The checkpoint state is embedded in the `Txn.app_id` field as base64-encoded JSON,
//! and committed atomically with Add actions in a single Delta commit.

mod actions;
mod commit;
mod table;

use std::collections::HashSet;

use async_trait::async_trait;
use base64::Engine;
use deltalake::DeltaTable;
use deltalake::arrow::datatypes::{Schema, SchemaRef};
use deltalake::kernel::Action;
use tracing::{debug, info, warn};

use blizzard_core::FinishedFile;
use blizzard_core::storage::StorageProvider;

use super::TableSink;
use crate::checkpoint::CheckpointState;
use crate::error::DeltaError;
use crate::schema::evolution::{EvolutionAction, SchemaEvolutionMode, validate_schema_evolution};

use actions::{TXN_APP_ID_PREFIX, create_add_action};
use commit::commit_to_delta_with_checkpoint;
use table::{arrow_schema_to_delta, ensure_handlers_registered, load_or_create_table, try_open_table};

/// Maximum number of Delta log versions to scan when recovering checkpoint state.
///
/// This limits how far back we search in the transaction log to avoid scanning
/// the entire history of long-lived tables. If no checkpoint is found within
/// this range, we start fresh (which may cause duplicate data if files were
/// already processed).
///
/// 1000 versions is generous enough to handle mixed workloads where other
/// applications write to the same Delta table, while still bounding scan time.
const CHECKPOINT_RECOVERY_SCAN_LIMIT: i64 = 1000;

/// Delta Lake sink for committing Parquet files.
pub struct DeltaSink {
    table: DeltaTable,
    last_version: i64,
    checkpoint_version: i64,
    /// Partition columns for this table.
    partition_by: Vec<String>,
    /// Cached table schema for evolution checks.
    cached_schema: Option<SchemaRef>,
    /// Table identifier for metrics labeling.
    table_name: String,
}

impl DeltaSink {
    /// Load or create a Delta Lake table.
    pub async fn new(
        storage: &StorageProvider,
        schema: &Schema,
        partition_by: Vec<String>,
        table_name: String,
    ) -> Result<Self, DeltaError> {
        ensure_handlers_registered();

        let table = load_or_create_table(storage, schema, &partition_by, &table_name).await?;
        let last_version = table.version().unwrap_or(-1);

        // Cache the schema from the created/loaded table
        let cached_schema = table.snapshot().ok().and_then(|s| {
            use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
            use std::sync::Arc;
            let arrow_schema: Schema = s.schema().as_ref().try_into_arrow().ok()?;
            Some(Arc::new(arrow_schema))
        });

        Ok(Self {
            table,
            last_version,
            checkpoint_version: 0,
            partition_by,
            cached_schema,
            table_name,
        })
    }

    /// Try to open an existing Delta Lake table without creating it.
    ///
    /// Returns an error if the table doesn't exist. Use `DeltaError::is_table_not_found()`
    /// to check if the error indicates a missing table.
    pub async fn try_open(
        storage: &StorageProvider,
        partition_by: Vec<String>,
        table_name: String,
    ) -> Result<Self, DeltaError> {
        ensure_handlers_registered();

        let table = try_open_table(storage, &table_name).await?;
        let last_version = table.version().unwrap_or(-1);

        // Cache the schema from the opened table
        let cached_schema = table.snapshot().ok().and_then(|s| {
            use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
            use std::sync::Arc;
            let arrow_schema: Schema = s.schema().as_ref().try_into_arrow().ok()?;
            Some(Arc::new(arrow_schema))
        });

        Ok(Self {
            table,
            last_version,
            checkpoint_version: 0,
            partition_by,
            cached_schema,
            table_name,
        })
    }

    /// Commit files with an atomic checkpoint.
    ///
    /// The checkpoint state is embedded in a `Txn` action and committed
    /// atomically with the Add actions for the files. This ensures that
    /// file commits and checkpoint state are always consistent.
    ///
    /// Returns the new version number if a commit was made.
    pub async fn commit_files_with_checkpoint(
        &mut self,
        files: &[FinishedFile],
        checkpoint: &CheckpointState,
    ) -> Result<Option<i64>, DeltaError> {
        // Increment checkpoint version
        self.checkpoint_version += 1;

        // Create add actions for files
        let add_actions: Vec<Action> = files.iter().map(create_add_action).collect();

        // Create checkpoint state with current delta version
        let mut checkpoint_with_version = checkpoint.clone();
        checkpoint_with_version.delta_version = self.last_version;

        // Commit with checkpoint
        let new_version = commit_to_delta_with_checkpoint(
            &mut self.table,
            add_actions,
            Some((&checkpoint_with_version, self.checkpoint_version)),
            &self.partition_by,
            &self.table_name,
        )
        .await?;

        self.last_version = new_version;
        info!(
            target = %self.table_name,
            "Committed {} files with checkpoint v{} to Delta Lake, version {}",
            files.len(),
            self.checkpoint_version,
            new_version
        );

        Ok(Some(new_version))
    }

    /// Get the current table version.
    pub fn version(&self) -> i64 {
        self.last_version
    }

    /// Get the current checkpoint version.
    pub fn checkpoint_version(&self) -> i64 {
        self.checkpoint_version
    }

    /// Get a reference to the underlying Delta table.
    pub fn table(&self) -> &DeltaTable {
        &self.table
    }

    /// Get the cached table schema, if available.
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.cached_schema.as_ref()
    }

    /// Validate an incoming schema against the table schema.
    ///
    /// Returns the evolution action to take based on the configured mode.
    pub fn validate_schema(
        &self,
        incoming: &Schema,
        mode: SchemaEvolutionMode,
    ) -> Result<EvolutionAction, crate::error::SchemaError> {
        let table_schema = match &self.cached_schema {
            Some(schema) => schema,
            None => {
                // No cached schema - accept incoming schema
                return Ok(EvolutionAction::None);
            }
        };

        validate_schema_evolution(table_schema, incoming, mode)
    }

    /// Apply a schema evolution action to the table.
    ///
    /// For `Merge` and `Overwrite` actions, this updates the table metadata
    /// with the new schema using Delta Lake's native schema evolution.
    pub async fn evolve_schema(&mut self, action: EvolutionAction) -> Result<(), DeltaError> {
        match action {
            EvolutionAction::None => {
                // No change needed
                Ok(())
            }
            EvolutionAction::Merge { new_schema } => {
                info!(
                    target = %self.table_name,
                    "Evolving schema: adding {} new fields",
                    new_schema.fields().len()
                        - self.cached_schema.as_ref().map_or(0, |s| s.fields().len())
                );
                self.apply_schema_change(&new_schema).await?;
                self.cached_schema = Some(new_schema);
                Ok(())
            }
            EvolutionAction::Overwrite { new_schema } => {
                warn!(
                    target = %self.table_name,
                    "Overwriting schema with {} fields",
                    new_schema.fields().len()
                );
                self.apply_schema_change(&new_schema).await?;
                self.cached_schema = Some(new_schema);
                Ok(())
            }
        }
    }

    /// Apply a schema change to the Delta table.
    ///
    /// Uses Delta Lake's metadata action to update the schema.
    async fn apply_schema_change(&mut self, new_schema: &Schema) -> Result<(), DeltaError> {
        use deltalake::kernel::MetadataExt;
        use deltalake::kernel::transaction::CommitBuilder;

        // Convert Arrow schema to Delta schema
        let delta_schema = arrow_schema_to_delta(new_schema)?;

        // Get current metadata and update schema
        let snapshot = self
            .table
            .snapshot()
            .map_err(|source| DeltaError::DeltaOperation { source })?;

        let current_metadata = snapshot.metadata().clone();
        let new_metadata = current_metadata
            .with_schema(&delta_schema)
            .map_err(|source| DeltaError::DeltaOperation {
                source: deltalake::DeltaTableError::Kernel { source },
            })?;

        // Commit the metadata change
        let actions = vec![Action::Metadata(new_metadata)];

        let version = CommitBuilder::default()
            .with_actions(actions)
            .build(
                Some(snapshot),
                self.table.log_store(),
                deltalake::protocol::DeltaOperation::SetTableProperties {
                    properties: std::collections::HashMap::new(),
                },
            )
            .await
            .map_err(|source| DeltaError::DeltaOperation { source })?
            .version;

        // Reload table to get new state
        self.table
            .load()
            .await
            .map_err(|source| DeltaError::DeltaOperation { source })?;

        self.last_version = version;
        info!(target = %self.table_name, "Schema evolution committed at version {}", version);

        Ok(())
    }

    /// Get all committed file paths from the Delta table snapshot.
    ///
    /// Returns a set of paths (relative to table root) for all files
    /// currently in the table. This is used to cross-check against
    /// incoming files to avoid double-commits.
    pub fn get_committed_paths(&self) -> HashSet<String> {
        match self.table.get_file_uris() {
            Ok(iter) => iter.collect(),
            Err(e) => {
                warn!(target = %self.table_name, "Failed to get committed paths: {}", e);
                HashSet::new()
            }
        }
    }

    /// Recover checkpoint state from the Delta transaction log.
    ///
    /// Scans the transaction log backwards from the latest version looking for
    /// a `Txn` action with app_id starting with "blizzard:" and decodes the
    /// embedded checkpoint state.
    ///
    /// Returns `Some((checkpoint_state, checkpoint_version))` if found.
    pub async fn recover_checkpoint_from_log(
        &mut self,
    ) -> Result<Option<(CheckpointState, i64)>, DeltaError> {
        use deltalake::logstore::{get_actions, read_commit_entry};

        // Reload table to get latest state
        self.table
            .load()
            .await
            .map_err(|source| DeltaError::DeltaOperation { source })?;

        let current_version = self.table.version().unwrap_or(-1);
        info!(
            target = %self.table_name,
            "Recovering checkpoint from Delta log, current_version={}",
            current_version
        );
        if current_version < 0 {
            debug!(target = %self.table_name, "Empty Delta table, no checkpoint to recover");
            return Ok(None);
        }

        let log_store = self.table.log_store();
        // Use object_store() (prefixed) instead of root_object_store() (unprefixed)
        let object_store = log_store.object_store(None);

        // Scan backwards through commit logs looking for our Txn action
        let start_version = (current_version - CHECKPOINT_RECOVERY_SCAN_LIMIT).max(0);

        for version in (start_version..=current_version).rev() {
            let commit_bytes = match read_commit_entry(object_store.as_ref(), version)
                .await
                .map_err(|source| DeltaError::DeltaOperation { source })?
            {
                Some(bytes) => bytes,
                None => continue,
            };

            let actions = get_actions(version, &commit_bytes)
                .map_err(|source| DeltaError::DeltaOperation { source })?;

            for action in &actions {
                if let Action::Txn(txn) = action
                    && txn.app_id.starts_with(TXN_APP_ID_PREFIX)
                {
                    let encoded = txn.app_id.strip_prefix(TXN_APP_ID_PREFIX).ok_or_else(|| {
                        DeltaError::InvalidCheckpoint {
                            message: "Missing blizzard prefix".to_string(),
                        }
                    })?;
                    let json_bytes = base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .map_err(|source| DeltaError::Base64 { source })?;
                    let state: CheckpointState = serde_json::from_slice(&json_bytes)
                        .map_err(|source| DeltaError::CheckpointJsonDecode { source })?;

                    // Update internal state
                    self.checkpoint_version = txn.version;
                    self.last_version = current_version;

                    return Ok(Some((state, txn.version)));
                }
            }
        }

        // Only warn if the table has commits beyond version 0, since a newly created
        // table won't have any checkpoint and there's nothing to re-ingest
        if current_version > 0 {
            warn!(
                target = %self.table_name,
                "No Blizzard checkpoint found in Delta log after scanning {} versions ({}..{}). \
                 Starting fresh - previously processed files may be re-ingested causing duplicates.",
                current_version - start_version + 1,
                start_version,
                current_version
            );
        } else {
            debug!(target = %self.table_name, "New Delta table (version 0), no checkpoint expected");
        }
        Ok(None)
    }
}

#[async_trait]
impl TableSink for DeltaSink {
    async fn commit_files_with_checkpoint(
        &mut self,
        files: &[FinishedFile],
        checkpoint: &CheckpointState,
    ) -> Result<Option<i64>, DeltaError> {
        DeltaSink::commit_files_with_checkpoint(self, files, checkpoint).await
    }

    fn version(&self) -> i64 {
        DeltaSink::version(self)
    }

    fn checkpoint_version(&self) -> i64 {
        DeltaSink::checkpoint_version(self)
    }

    fn schema(&self) -> Option<&SchemaRef> {
        DeltaSink::schema(self)
    }

    fn get_committed_paths(&self) -> HashSet<String> {
        DeltaSink::get_committed_paths(self)
    }

    async fn recover_checkpoint_from_log(
        &mut self,
    ) -> Result<Option<(CheckpointState, i64)>, DeltaError> {
        DeltaSink::recover_checkpoint_from_log(self).await
    }

    fn validate_schema(
        &self,
        incoming: &Schema,
        mode: SchemaEvolutionMode,
    ) -> Result<EvolutionAction, crate::error::SchemaError> {
        DeltaSink::validate_schema(self, incoming, mode)
    }

    async fn evolve_schema(&mut self, action: EvolutionAction) -> Result<(), DeltaError> {
        DeltaSink::evolve_schema(self, action).await
    }

    async fn create_checkpoint(&self) -> Result<(), DeltaError> {
        deltalake::checkpoints::create_checkpoint(self.table(), None)
            .await
            .map_err(|source| DeltaError::DeltaOperation { source })
    }

    fn table_name(&self) -> &str {
        &self.table_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_try_open_nonexistent_table() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let storage = StorageProvider::for_url_with_options(
            temp_dir.path().to_str().unwrap(),
            HashMap::new(),
        )
        .await
        .unwrap();

        let result = DeltaSink::try_open(&storage, vec![], "test".to_string()).await;
        match result {
            Ok(_) => panic!("Expected error for non-existent table"),
            Err(e) => assert!(
                e.is_table_not_found(),
                "Expected table not found error, got: {e:?}"
            ),
        }
    }

    #[tokio::test]
    async fn test_try_open_existing_table() {
        use deltalake::arrow::datatypes::{DataType, Field, Schema};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let storage = StorageProvider::for_url_with_options(
            temp_dir.path().to_str().unwrap(),
            HashMap::new(),
        )
        .await
        .unwrap();

        // First create a table
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let _sink = DeltaSink::new(&storage, &schema, vec![], "test".to_string())
            .await
            .unwrap();

        // Now try_open should succeed
        let opened_sink = DeltaSink::try_open(&storage, vec![], "test".to_string())
            .await
            .unwrap();
        assert!(opened_sink.version() >= 0);
    }

    #[test]
    fn test_is_table_not_found_delta_operation() {
        use deltalake::DeltaTableError;

        // Test with a "not found" error message
        let err = DeltaError::DeltaOperation {
            source: DeltaTableError::NotATable("Table not found".to_string()),
        };
        assert!(err.is_table_not_found());

        // Test with other error types
        let err = DeltaError::UrlParse {
            url: "invalid".to_string(),
        };
        assert!(!err.is_table_not_found());

        let err = DeltaError::InvalidCheckpoint {
            message: "bad checkpoint".to_string(),
        };
        assert!(!err.is_table_not_found());
    }
}
