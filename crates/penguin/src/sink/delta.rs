//! Delta Lake commit logic.
//!
//! Handles creating/opening Delta Lake tables and committing
//! Parquet files with exactly-once semantics.
//!
//! # Atomic Checkpointing
//!
//! This module uses Delta Lake's `Txn` action to achieve atomic checkpointing.
//! The checkpoint state is embedded in the `Txn.app_id` field as base64-encoded JSON,
//! and committed atomically with Add actions in a single Delta commit.

use base64::Engine;
use deltalake::DeltaTable;
use deltalake::arrow::datatypes::{Schema, SchemaRef};
use deltalake::kernel::Action;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use object_store::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};
use url::Url;

use blizzard_common::FinishedFile;
use blizzard_common::metrics::events::{CheckpointStateSize, DeltaCommitCompleted, InternalEvent};
use blizzard_common::storage::{BackendConfig, StorageProvider};

use crate::checkpoint::CheckpointState;
use crate::error::DeltaError;
use crate::schema::evolution::{EvolutionAction, SchemaEvolutionMode, validate_schema_evolution};

/// Prefix for Blizzard checkpoint app_id in Delta Txn actions.
const TXN_APP_ID_PREFIX: &str = "blizzard:";

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

/// Ensure Delta Lake cloud storage handlers are registered.
///
/// This is idempotent - calling multiple times is safe.
fn ensure_handlers_registered() {
    deltalake::aws::register_handlers(None);
    deltalake::gcp::register_handlers(None);
}

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

        let table = load_or_create_table(storage, schema, &partition_by).await?;
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

        let table = try_open_table(storage).await?;
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
        info!("Schema evolution committed at version {}", version);

        Ok(())
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
            "Recovering checkpoint from Delta log, current_version={}",
            current_version
        );
        if current_version < 0 {
            debug!("Empty Delta table, no checkpoint to recover");
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

        warn!(
            "No Blizzard checkpoint found in Delta log after scanning {} versions ({}..{}). \
             Starting fresh - previously processed files may be re-ingested causing duplicates.",
            current_version - start_version + 1,
            start_version,
            current_version
        );
        Ok(None)
    }
}

/// Convert an Arrow schema to a Delta schema.
fn arrow_schema_to_delta(schema: &Schema) -> Result<deltalake::kernel::StructType, DeltaError> {
    use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
    use deltalake::kernel::{DataType as DeltaType, StructField, StructType};

    let fields: Vec<StructField> = schema
        .fields()
        .iter()
        .map(|field| {
            let delta_type: DeltaType = field
                .data_type()
                .try_into_kernel()
                .map_err(|source| DeltaError::SchemaConversion { source })?;
            Ok(StructField::new(
                field.name(),
                delta_type,
                field.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>, DeltaError>>()?;

    StructType::try_new(fields).map_err(|e| DeltaError::StructType {
        message: e.to_string(),
    })
}

/// Construct the Delta table URL from a storage provider.
fn build_table_url(storage_provider: &StorageProvider) -> Result<String, DeltaError> {
    let empty_path = Path::parse("").map_err(|_| DeltaError::PathParse {
        path: "".to_string(),
    })?;

    let table_url = match storage_provider.config() {
        BackendConfig::S3(s3) => {
            format!(
                "s3://{}/{}",
                s3.bucket,
                storage_provider.qualify_path(&empty_path)
            )
        }
        BackendConfig::Gcs(gcs) => {
            format!(
                "gs://{}/{}",
                gcs.bucket,
                storage_provider.qualify_path(&empty_path)
            )
        }
        BackendConfig::Azure(azure) => {
            format!(
                "abfs://{}/{}",
                azure.container,
                storage_provider.qualify_path(&empty_path)
            )
        }
        BackendConfig::Local(local) => {
            format!("file://{}", local.path)
        }
    };

    Ok(table_url)
}

/// Try to open an existing Delta Lake table.
///
/// Returns an error if the table doesn't exist. Use `DeltaError::is_table_not_found()`
/// to check if the error indicates a missing table.
async fn try_open_table(storage_provider: &StorageProvider) -> Result<DeltaTable, DeltaError> {
    let table_url = build_table_url(storage_provider)?;

    let parsed_url = Url::parse(&table_url).map_err(|_| DeltaError::UrlParse {
        url: table_url.clone(),
    })?;

    let table = deltalake::open_table_with_storage_options(
        parsed_url,
        storage_provider.storage_options().clone(),
    )
    .await
    .map_err(|source| DeltaError::DeltaOperation { source })?;

    info!(
        "Opened existing Delta table at version {}",
        table.version().unwrap_or(-1)
    );
    Ok(table)
}

/// Load or create a Delta Lake table with the given schema.
pub async fn load_or_create_table(
    storage_provider: &StorageProvider,
    schema: &Schema,
    partition_by: &[String],
) -> Result<DeltaTable, DeltaError> {
    let table_url = build_table_url(storage_provider)?;

    // Try to open existing table
    let parsed_url = Url::parse(&table_url).map_err(|_| DeltaError::UrlParse {
        url: table_url.clone(),
    })?;
    match deltalake::open_table_with_storage_options(
        parsed_url.clone(),
        storage_provider.storage_options().clone(),
    )
    .await
    {
        Ok(table) => {
            info!(
                "Loaded existing Delta table at version {}",
                table.version().unwrap_or(-1)
            );
            Ok(table)
        }
        Err(_) => {
            // Table doesn't exist, create it
            info!("Creating new Delta table at {}", table_url);

            // Convert Arrow schema to Delta schema
            let delta_schema = arrow_schema_to_delta(schema)?;

            let mut builder = CreateBuilder::new()
                .with_location(&table_url)
                .with_columns(delta_schema.fields().cloned())
                .with_storage_options(storage_provider.storage_options().clone());

            // Add partition columns if configured
            if !partition_by.is_empty() {
                info!("Creating table with partition columns: {:?}", partition_by);
                builder = builder.with_partition_columns(partition_by);
            }

            let table = builder
                .await
                .map_err(|source| DeltaError::DeltaOperation { source })?;

            Ok(table)
        }
    }
}

/// Create a Delta Lake Add action for a finished file.
fn create_add_action(file: &FinishedFile) -> Action {
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
        size: file.size as i64,
        partition_values,
        modification_time: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0),
        data_change: true,
        ..Default::default()
    })
}

/// Create a Delta Lake Txn action with embedded checkpoint state.
///
/// The checkpoint state is serialized to JSON and base64-encoded in the app_id field.
/// Format: `blizzard:{base64_encoded_checkpoint_json}`
fn create_txn_action(
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
    let app_id = format!("{}{}", TXN_APP_ID_PREFIX, encoded);

    Ok(Action::Txn(Transaction {
        app_id,
        version,
        last_updated: Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        ),
    }))
}

/// Commit actions to Delta table with optional checkpoint.
///
/// When checkpoint is provided, a Txn action is prepended to the add actions
/// and committed atomically in a single transaction.
async fn commit_to_delta_with_checkpoint(
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
        table: table_name.to_string(),
    }
    .emit();

    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use blizzard_common::types::SourceState;

    #[test]
    fn test_create_add_action() {
        use std::collections::HashMap;

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
        use std::collections::HashMap;

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
        use std::collections::HashMap;

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
        source_state.update_records("file1.ndjson.gz", 100);

        let checkpoint = CheckpointState {
            schema_version: 1,
            source_state,
            delta_version: 5,
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
        source_state.update_records("file1.ndjson.gz", 100);
        source_state.mark_finished("file2.ndjson.gz");

        let original = CheckpointState {
            schema_version: 1,
            source_state,
            delta_version: 10,
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
        } else {
            panic!("Expected Txn action");
        }
    }

    #[tokio::test]
    async fn test_try_open_nonexistent_table() {
        use std::collections::HashMap;
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
                "Expected table not found error, got: {:?}",
                e
            ),
        }
    }

    #[tokio::test]
    async fn test_try_open_existing_table() {
        use deltalake::arrow::datatypes::{DataType, Field, Schema};
        use std::collections::HashMap;
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
        let _sink = DeltaSink::new(&storage, &schema, vec![], "test".to_string()).await.unwrap();

        // Now try_open should succeed
        let opened_sink = DeltaSink::try_open(&storage, vec![], "test".to_string()).await.unwrap();
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
