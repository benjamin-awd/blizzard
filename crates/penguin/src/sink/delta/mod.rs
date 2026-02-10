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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use deltalake::DeltaTable;
use deltalake::arrow::datatypes::{Schema, SchemaRef};
use deltalake::kernel::Action;
use tracing::{debug, info, warn};

/// Timeout for schema evolution commits.
const SCHEMA_EVOLUTION_TIMEOUT: Duration = Duration::from_secs(60);

use blizzard_core::FinishedFile;
use blizzard_core::storage::StorageProvider;

use super::{CheckpointRecovery, SchemaEvolution, TableCommitter, TableSink};
use crate::checkpoint::CheckpointState;
use crate::error::DeltaError;
use crate::metrics::events::{InternalEvent, SchemaEvolved};
use crate::schema::evolution::{EvolutionAction, SchemaEvolutionMode, validate_schema_evolution};

use actions::{TXN_APP_ID_PREFIX, create_add_action};
use commit::commit_to_delta_with_checkpoint;
use table::{
    arrow_schema_to_delta, ensure_handlers_registered, load_or_create_table, try_open_table,
};

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
    /// Extract the Arrow schema from a Delta table's snapshot metadata.
    fn cached_schema_from_table(table: &DeltaTable) -> Option<SchemaRef> {
        use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
        let snapshot = table.snapshot().ok()?;
        let arrow_schema: Schema = snapshot.schema().as_ref().try_into_arrow().ok()?;
        Some(Arc::new(arrow_schema))
    }

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
        let cached_schema = Self::cached_schema_from_table(&table);

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
        let cached_schema = Self::cached_schema_from_table(&table);

        Ok(Self {
            table,
            last_version,
            checkpoint_version: 0,
            partition_by,
            cached_schema,
            table_name,
        })
    }

    /// Apply a schema change to the Delta table.
    ///
    /// Uses Delta Lake's metadata action to update the schema.
    /// Applies a timeout to prevent hanging forever on commit or reload.
    async fn apply_schema_change(
        &mut self,
        new_schema: &Schema,
        new_fields: Vec<deltalake::kernel::StructField>,
    ) -> Result<(), DeltaError> {
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

        // Commit the metadata change (with timeout to prevent hanging)
        let actions = vec![Action::Metadata(new_metadata)];

        let commit_result = tokio::time::timeout(
            SCHEMA_EVOLUTION_TIMEOUT,
            CommitBuilder::default().with_actions(actions).build(
                Some(snapshot),
                self.table.log_store(),
                deltalake::protocol::DeltaOperation::AddColumn { fields: new_fields },
            ),
        )
        .await
        .map_err(|_| DeltaError::Timeout {
            operation: "schema evolution commit".to_string(),
            seconds: SCHEMA_EVOLUTION_TIMEOUT.as_secs(),
        })?
        .map_err(|source| DeltaError::DeltaOperation { source })?;

        let version = commit_result.version;

        // Reload table to get new state (with timeout)
        tokio::time::timeout(SCHEMA_EVOLUTION_TIMEOUT, self.table.load())
            .await
            .map_err(|_| DeltaError::Timeout {
                operation: "table reload after schema evolution".to_string(),
                seconds: SCHEMA_EVOLUTION_TIMEOUT.as_secs(),
            })?
            .map_err(|source| DeltaError::DeltaOperation { source })?;

        self.last_version = version;
        info!(target = %self.table_name, "Schema evolution committed at version {version}");

        Ok(())
    }
}

#[async_trait]
impl TableCommitter for DeltaSink {
    async fn commit_files_with_checkpoint(
        &mut self,
        files: &[FinishedFile],
        checkpoint: &CheckpointState,
    ) -> Result<Option<i64>, DeltaError> {
        let next_checkpoint_version = self.checkpoint_version + 1;

        // Create add actions for files
        let add_actions: Vec<Action> = files.iter().map(create_add_action).collect();

        // Create checkpoint state with current delta version
        let mut checkpoint_with_version = checkpoint.clone();
        checkpoint_with_version.delta_version = self.last_version;

        // Commit with checkpoint
        let new_version = commit_to_delta_with_checkpoint(
            &mut self.table,
            add_actions,
            Some((&checkpoint_with_version, next_checkpoint_version)),
            &self.partition_by,
            &self.table_name,
        )
        .await?;

        // Only update state after successful commit
        self.checkpoint_version = next_checkpoint_version;
        self.last_version = new_version;
        self.cached_schema = Self::cached_schema_from_table(&self.table);
        info!(
            target = %self.table_name,
            "Committed {} files with checkpoint v{} to Delta Lake, version {}",
            files.len(),
            self.checkpoint_version,
            new_version
        );

        Ok(Some(new_version))
    }

    async fn create_checkpoint(&self) -> Result<(), DeltaError> {
        deltalake::checkpoints::create_checkpoint(&self.table, None)
            .await
            .map_err(|source| DeltaError::DeltaOperation { source })
    }

    fn version(&self) -> i64 {
        self.last_version
    }

    fn checkpoint_version(&self) -> i64 {
        self.checkpoint_version
    }
}

#[async_trait]
impl SchemaEvolution for DeltaSink {
    fn schema(&self) -> Option<&SchemaRef> {
        self.cached_schema.as_ref()
    }

    fn validate_schema(
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

    async fn evolve_schema(&mut self, action: EvolutionAction) -> Result<(), DeltaError> {
        match action {
            EvolutionAction::None => {}
            EvolutionAction::Merge { new_schema } => {
                // Compute only the newly added fields for the Delta operation log
                let existing_names: HashSet<&str> =
                    self.cached_schema.as_ref().map_or_else(HashSet::new, |s| {
                        s.fields().iter().map(|f| f.name().as_str()).collect()
                    });
                let new_fields: Vec<_> = arrow_schema_to_delta(&new_schema)?
                    .fields()
                    .filter(|f| !existing_names.contains(f.name().as_str()))
                    .cloned()
                    .collect();

                info!(
                    target = %self.table_name,
                    "Evolving schema: adding {} new fields",
                    new_fields.len(),
                );
                self.apply_schema_change(&new_schema, new_fields).await?;
                self.cached_schema = Some(new_schema);
                SchemaEvolved {
                    target: self.table_name.clone(),
                    action: "merge".to_string(),
                }
                .emit();
            }
            EvolutionAction::Overwrite { new_schema } => {
                let all_fields = arrow_schema_to_delta(&new_schema)?
                    .fields()
                    .cloned()
                    .collect();

                warn!(
                    target = %self.table_name,
                    "Overwriting schema with {} fields",
                    new_schema.fields().len()
                );
                self.apply_schema_change(&new_schema, all_fields).await?;
                self.cached_schema = Some(new_schema);
                SchemaEvolved {
                    target: self.table_name.clone(),
                    action: "overwrite".to_string(),
                }
                .emit();
            }
        }

        Ok(())
    }
}

#[async_trait]
impl CheckpointRecovery for DeltaSink {
    async fn recover_checkpoint_from_log(
        &mut self,
    ) -> Result<Option<(CheckpointState, i64)>, DeltaError> {
        use deltalake::logstore::{get_actions, read_commit_entry};
        use futures::stream::{self, StreamExt};

        /// Number of commit log entries to fetch concurrently.
        const SCAN_CONCURRENCY: usize = 16;

        // Reload table to get latest state
        self.table
            .load()
            .await
            .map_err(|source| DeltaError::DeltaOperation { source })?;
        self.cached_schema = Self::cached_schema_from_table(&self.table);

        let current_version = self.table.version().unwrap_or(-1);
        debug!(
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

        // Scan backwards through commit logs looking for our Txn action.
        // Fetch entries in parallel batches to reduce cloud storage latency.
        let start_version = (current_version - CHECKPOINT_RECOVERY_SCAN_LIMIT).max(0);
        let versions: Vec<i64> = (start_version..=current_version).rev().collect();

        for batch in versions.chunks(SCAN_CONCURRENCY) {
            let fetched: Vec<(i64, Result<Option<bytes::Bytes>, _>)> =
                stream::iter(batch.iter().copied())
                    .map(|v| {
                        let store = object_store.clone();
                        async move { (v, read_commit_entry(store.as_ref(), v).await) }
                    })
                    .buffer_unordered(SCAN_CONCURRENCY)
                    .collect()
                    .await;

            // Process in descending version order (most recent first)
            let mut fetched_sorted: Vec<_> = fetched.into_iter().collect();
            fetched_sorted.sort_by(|a, b| b.0.cmp(&a.0));

            for (version, result) in fetched_sorted {
                let commit_bytes =
                    match result.map_err(|source| DeltaError::DeltaOperation { source })? {
                        Some(bytes) => bytes,
                        None => continue,
                    };

                let actions = get_actions(version, &commit_bytes)
                    .map_err(|source| DeltaError::DeltaOperation { source })?;

                for action in &actions {
                    if let Action::Txn(txn) = action
                        && txn.app_id.starts_with(TXN_APP_ID_PREFIX)
                    {
                        let encoded =
                            txn.app_id.strip_prefix(TXN_APP_ID_PREFIX).ok_or_else(|| {
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

    fn get_committed_paths(&self) -> Result<HashSet<String>, DeltaError> {
        self.table
            .get_file_uris()
            .map(|iter| iter.collect())
            .map_err(|source| DeltaError::DeltaOperation { source })
    }
}

#[async_trait]
impl TableSink for DeltaSink {
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

        // NotATable variant should be detected
        let err = DeltaError::DeltaOperation {
            source: DeltaTableError::NotATable("no snapshot found".to_string()),
        };
        assert!(err.is_table_not_found());

        // NotInitialized variant should be detected
        let err = DeltaError::DeltaOperation {
            source: DeltaTableError::NotInitialized,
        };
        assert!(err.is_table_not_found());

        // Other DeltaTableError variants should not match
        let err = DeltaError::DeltaOperation {
            source: DeltaTableError::Generic("some error".to_string()),
        };
        assert!(!err.is_table_not_found());

        // Non-DeltaOperation variants should not match
        let err = DeltaError::UrlParse {
            url: "invalid".to_string(),
        };
        assert!(!err.is_table_not_found());

        let err = DeltaError::InvalidCheckpoint {
            message: "bad checkpoint".to_string(),
        };
        assert!(!err.is_table_not_found());
    }

    #[tokio::test]
    async fn test_checkpoint_version_unchanged_on_failed_commit() {
        use blizzard_core::FinishedFile;
        use deltalake::arrow::datatypes::{DataType, Field, Schema};
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let storage = StorageProvider::for_url_with_options(
            temp_dir.path().to_str().unwrap(),
            HashMap::new(),
        )
        .await
        .unwrap();

        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let mut sink = DeltaSink::new(&storage, &schema, vec![], "test".to_string())
            .await
            .unwrap();

        assert_eq!(sink.checkpoint_version(), 0);

        // Delete the _delta_log directory to make commits fail
        std::fs::remove_dir_all(temp_dir.path().join("_delta_log")).unwrap();

        let files = vec![FinishedFile::without_bytes(
            "test.parquet".to_string(),
            1024,
            100,
            HashMap::new(),
            None,
        )];
        let checkpoint = crate::checkpoint::CheckpointState::default();

        let result = sink.commit_files_with_checkpoint(&files, &checkpoint).await;
        assert!(result.is_err(), "commit should fail with missing log dir");

        // checkpoint_version must not have been incremented
        assert_eq!(
            sink.checkpoint_version(),
            0,
            "checkpoint_version should be unchanged after failed commit"
        );
    }
}
