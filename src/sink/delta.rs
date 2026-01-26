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
use deltalake::arrow::datatypes::Schema;
use deltalake::kernel::Action;
use deltalake::operations::create::CreateBuilder;
use deltalake::protocol::SaveMode;
use object_store::path::Path;
use snafu::prelude::*;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, info};
use url::Url;

use super::FinishedFile;
use crate::checkpoint::CheckpointState;
use crate::emit;
use crate::error::{
    Base64DecodeSnafu, CheckpointJsonSnafu, DeltaError, DeltaLakeSnafu, SchemaConversionSnafu,
    StructTypeSnafu, UrlParseSnafu,
};
use crate::metrics::events::{CheckpointStateSize, DeltaCommitCompleted};
use crate::storage::{BackendConfig, StorageProvider, StorageProviderRef};

/// Prefix for Blizzard checkpoint app_id in Delta Txn actions.
const TXN_APP_ID_PREFIX: &str = "blizzard:";

/// Delta Lake sink for committing Parquet files.
pub struct DeltaSink {
    table: DeltaTable,
    last_version: i64,
    checkpoint_version: i64,
}

impl DeltaSink {
    /// Load or create a Delta Lake table.
    pub async fn new(storage: StorageProviderRef, schema: &Schema) -> Result<Self, DeltaError> {
        // Register Delta Lake handlers for cloud storage
        deltalake::aws::register_handlers(None);
        deltalake::gcp::register_handlers(None);

        let table = load_or_create_table(&storage, schema).await?;
        let last_version = table.version().unwrap_or(-1);

        Ok(Self {
            table,
            last_version,
            checkpoint_version: 0,
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
        self.table.load().await.context(DeltaLakeSnafu)?;

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
        // Limit search to last 100 commits to avoid scanning entire history
        let start_version = (current_version - 100).max(0);

        for version in (start_version..=current_version).rev() {
            let commit_bytes = match read_commit_entry(object_store.as_ref(), version)
                .await
                .context(DeltaLakeSnafu)?
            {
                Some(bytes) => bytes,
                None => continue,
            };

            let actions = get_actions(version, &commit_bytes).context(DeltaLakeSnafu)?;

            for action in &actions {
                if let Action::Txn(txn) = action {
                    if txn.app_id.starts_with(TXN_APP_ID_PREFIX) {
                        let encoded = txn.app_id.strip_prefix(TXN_APP_ID_PREFIX).unwrap();
                        let json_bytes = base64::engine::general_purpose::STANDARD
                            .decode(encoded)
                            .context(Base64DecodeSnafu)?;
                        let state: CheckpointState =
                            serde_json::from_slice(&json_bytes).context(CheckpointJsonSnafu)?;

                        // Update internal state
                        self.checkpoint_version = txn.version;
                        self.last_version = current_version;

                        return Ok(Some((state, txn.version)));
                    }
                }
            }
        }

        debug!(
            "No Blizzard checkpoint found in Delta transaction log (scanned versions {}..{})",
            start_version, current_version
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
                .context(SchemaConversionSnafu)?;
            Ok(StructField::new(
                field.name(),
                delta_type,
                field.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>, DeltaError>>()?;

    StructType::try_new(fields).map_err(|e| {
        StructTypeSnafu {
            message: e.to_string(),
        }
        .build()
    })
}

/// Load or create a Delta Lake table with the given schema.
pub async fn load_or_create_table(
    storage_provider: &StorageProvider,
    schema: &Schema,
) -> Result<DeltaTable, DeltaError> {
    let empty_path = &Path::parse("").unwrap();

    let table_url: String = match storage_provider.config() {
        BackendConfig::S3(s3) => {
            format!(
                "s3://{}/{}",
                s3.bucket,
                storage_provider.qualify_path(empty_path)
            )
        }
        BackendConfig::Gcs(gcs) => {
            format!(
                "gs://{}/{}",
                gcs.bucket,
                storage_provider.qualify_path(empty_path)
            )
        }
        BackendConfig::Azure(azure) => {
            format!(
                "abfs://{}/{}",
                azure.container,
                storage_provider.qualify_path(empty_path)
            )
        }
        BackendConfig::Local(local) => {
            format!("file://{}", local.path)
        }
    };

    // Try to open existing table
    let parsed_url = Url::parse(&table_url).context(UrlParseSnafu)?;
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

            let table = CreateBuilder::new()
                .with_location(&table_url)
                .with_columns(delta_schema.fields().cloned())
                .with_storage_options(storage_provider.storage_options().clone())
                .await
                .context(DeltaLakeSnafu)?;

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

    Action::Add(Add {
        path: subpath.to_string(),
        size: file.size as i64,
        partition_values: HashMap::new(),
        modification_time: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
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

    let checkpoint_json = serde_json::to_string(checkpoint_state).context(CheckpointJsonSnafu)?;

    // Track checkpoint state size for memory monitoring
    emit!(CheckpointStateSize {
        bytes: checkpoint_json.len()
    });

    let encoded = base64::engine::general_purpose::STANDARD.encode(&checkpoint_json);
    let app_id = format!("{}{}", TXN_APP_ID_PREFIX, encoded);

    Ok(Action::Txn(Transaction {
        app_id,
        version,
        last_updated: Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
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

    let version = CommitBuilder::default()
        .with_actions(all_actions)
        .build(
            Some(table.snapshot().context(DeltaLakeSnafu)?),
            table.log_store(),
            deltalake::protocol::DeltaOperation::Write {
                mode: SaveMode::Append,
                partition_by: None,
                predicate: None,
            },
        )
        .await
        .context(DeltaLakeSnafu)?
        .version;

    // Reload table to get new state
    table.load().await.context(DeltaLakeSnafu)?;

    emit!(DeltaCommitCompleted {
        duration: start.elapsed()
    });

    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::SourceState;

    #[test]
    fn test_create_add_action() {
        let file = FinishedFile {
            filename: "test-file.parquet".to_string(),
            size: 1024,
            record_count: 100,
            bytes: None,
        };

        let action = create_add_action(&file);

        match action {
            Action::Add(add) => {
                assert_eq!(add.path, "test-file.parquet");
                assert_eq!(add.size, 1024);
                assert!(add.data_change);
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
}
