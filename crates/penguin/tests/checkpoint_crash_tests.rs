//! Integration tests for checkpoint mechanism with atomic Txn-based checkpointing.
//!
//! These tests verify penguin's checkpoint embedding and recovery logic in DeltaSink.
//! Unit tests for CheckpointCoordinator and CheckpointState are in the respective modules.
//!
//! Run with: cargo test -p penguin --test checkpoint_crash_tests

use std::collections::HashMap;
use std::sync::Arc;

use blizzard_core::FinishedFile;
use blizzard_core::storage::StorageProvider;
use blizzard_core::types::SourceState;
use blizzard_core::watermark::WatermarkState;
use penguin::SchemaEvolutionMode;
use penguin::checkpoint::{CheckpointCoordinator, CheckpointState};
use penguin::schema::infer_schema_from_first_file;
use penguin::sink::DeltaSink;

/// Test: Checkpoint commit and recovery via Delta Lake Txn actions.
///
/// Verifies that:
/// - Checkpoints are embedded in Txn actions and committed atomically with data
/// - Recovery scans the log and finds the latest checkpoint
/// - Source state (including finished files) is correctly preserved
#[tokio::test]
async fn test_checkpoint_commit_and_recovery() {
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let storage = StorageProvider::for_url_with_options(table_path, HashMap::new())
        .await
        .unwrap();

    let schema = Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, true),
    ]);

    let mut delta_sink = DeltaSink::new(&storage, &schema, vec![], "test".to_string())
        .await
        .unwrap();

    // Commit first batch
    let source_state1 = SourceState::new();

    let checkpoint1 = CheckpointState {
        schema_version: 2,
        source_state: source_state1,
        delta_version: 0,
        watermark: WatermarkState::Initial,
    };

    delta_sink
        .commit_files_with_checkpoint(
            &[FinishedFile::without_bytes(
                "batch1.parquet".to_string(),
                512,
                100,
                HashMap::new(),
                None,
            )],
            &checkpoint1,
        )
        .await
        .unwrap();

    // Commit second batch - first file finished
    let mut source_state2 = SourceState::new();
    source_state2.mark_finished("file1.ndjson.gz");

    let checkpoint2 = CheckpointState {
        schema_version: 2,
        source_state: source_state2,
        delta_version: delta_sink.version(),
        watermark: WatermarkState::Initial,
    };

    delta_sink
        .commit_files_with_checkpoint(
            &[FinishedFile::without_bytes(
                "batch2.parquet".to_string(),
                1024,
                200,
                HashMap::new(),
                None,
            )],
            &checkpoint2,
        )
        .await
        .unwrap();

    // Simulate restart - create new sink and recover
    let mut new_sink = DeltaSink::new(&storage, &schema, vec![], "test".to_string())
        .await
        .unwrap();
    let recovered = new_sink.recover_checkpoint_from_log().await.unwrap();

    assert!(recovered.is_some(), "Should recover checkpoint from log");
    let (state, version) = recovered.unwrap();

    // Should recover the LATEST checkpoint (version 2)
    assert_eq!(version, 2);
    assert!(
        state.source_state.is_file_finished("file1.ndjson.gz"),
        "file1 should be marked finished"
    );
}

/// Test: CheckpointCoordinator handles concurrent updates safely.
///
/// Verifies that concurrent updates and captures don't cause data races
/// or inconsistent state due to the single consolidated lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_coordinator_access() {
    use std::collections::HashSet;

    let coordinator = Arc::new(CheckpointCoordinator::new("test".to_string()));

    // Spawn multiple tasks that update state concurrently
    let mut handles = Vec::new();

    for task_id in 0..4 {
        let coordinator = coordinator.clone();
        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let file_id = task_id * 100 + i;
                coordinator
                    .mark_file_finished(&format!("file{file_id}.ndjson.gz"))
                    .await;
                coordinator.update_delta_version(file_id as i64).await;
            }
        });
        handles.push(handle);
    }

    // Capture states while updates are happening
    let coordinator_capture = coordinator.clone();
    let capture_handle = tokio::spawn(async move {
        let mut captured_states = Vec::new();
        for _ in 0..100 {
            let state = coordinator_capture.capture_state().await;
            captured_states.push(state);
            tokio::task::yield_now().await;
        }
        captured_states
    });

    for h in handles {
        h.await.unwrap();
    }

    let captured_states = capture_handle.await.unwrap();

    // All captures should complete without panic (no data races)
    assert_eq!(captured_states.len(), 100);

    // Final state should have all 400 files
    let final_state = coordinator.capture_state().await;
    assert_eq!(final_state.source_state.files.len(), 400);

    // Verify all 400 unique file IDs are present (no lost updates)
    let expected_files: HashSet<String> =
        (0..400).map(|id| format!("file{id}.ndjson.gz")).collect();
    let actual_files: HashSet<String> = final_state.source_state.files.keys().cloned().collect();
    assert_eq!(
        actual_files, expected_files,
        "All 400 unique file IDs should be present with no lost updates"
    );

    // Verify each file is marked as finished
    for file_id in 0..400 {
        let filename = format!("file{file_id}.ndjson.gz");
        assert!(
            final_state.source_state.is_file_finished(&filename),
            "File {filename} should be marked as finished"
        );
    }

    // Verify intermediate captured states are consistent:
    // - File count should be monotonically non-decreasing over time
    // - Each captured state should be a valid snapshot (no partial updates visible)
    for (i, state) in captured_states.iter().enumerate() {
        // All files in any captured state should be marked finished
        for filename in state.source_state.files.keys() {
            assert!(
                state.source_state.is_file_finished(filename),
                "Captured state {i}: file {filename} should be finished"
            );
        }
    }

    // Verify file counts are monotonically non-decreasing (eventual consistency)
    // Note: Due to concurrent updates, we may see same count multiple times,
    // but we should never see a decrease followed by an increase to the same value
    let file_counts: Vec<usize> = captured_states
        .iter()
        .map(|s| s.source_state.files.len())
        .collect();
    for window in file_counts.windows(2) {
        assert!(
            window[1] >= window[0],
            "File count should be monotonically non-decreasing, but saw {} -> {}",
            window[0],
            window[1]
        );
    }
}

/// Test: Lazy schema inference creates Delta table with correct schema.
///
/// Verifies that:
/// - try_open fails for non-existent table
/// - Schema is correctly inferred from parquet file
/// - Table is created with the inferred schema
/// - Data can be committed and queried with correct column names
#[tokio::test]
async fn test_lazy_schema_inference_creates_correct_table() {
    use deltalake::arrow::array::{Int64Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use deltalake::arrow::record_batch::RecordBatch;
    use deltalake::parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();

    // Step 1: Verify try_open fails for non-existent table
    let storage =
        StorageProvider::for_url_with_options(table_path.to_str().unwrap(), HashMap::new())
            .await
            .unwrap();

    let try_open_result = DeltaSink::try_open(&storage, vec![], "test".to_string()).await;
    match try_open_result {
        Ok(_) => panic!("Expected error for non-existent table"),
        Err(e) => assert!(e.is_table_not_found(), "Expected table not found error"),
    }

    // Step 2: Create a parquet file with a specific schema in the table directory
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("username", DataType::Utf8, true),
        Field::new("score", DataType::Int64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )
    .unwrap();

    // Write parquet file directly to table directory
    let parquet_path = table_path.join("data.parquet");
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    std::fs::write(&parquet_path, &buffer).unwrap();

    // Step 3: Infer schema from the parquet file
    let files = vec![FinishedFile::without_bytes(
        "data.parquet".to_string(),
        buffer.len(),
        3,
        HashMap::new(),
        None,
    )];

    let inferred_schema = infer_schema_from_first_file(&storage, &files, "test")
        .await
        .unwrap();

    // Verify inferred schema matches
    assert_eq!(inferred_schema.fields().len(), 3);
    assert_eq!(inferred_schema.field(0).name(), "user_id");
    assert_eq!(inferred_schema.field(1).name(), "username");
    assert_eq!(inferred_schema.field(2).name(), "score");

    // Step 4: Create Delta table with inferred schema
    let mut delta_sink = DeltaSink::new(&storage, &inferred_schema, vec![], "test".to_string())
        .await
        .unwrap();

    // Step 5: Commit the parquet file
    let checkpoint = CheckpointState {
        schema_version: 2,
        source_state: SourceState::new(),
        delta_version: 0,
        watermark: WatermarkState::Initial,
    };

    delta_sink
        .commit_files_with_checkpoint(&files, &checkpoint)
        .await
        .unwrap();

    // Step 6: Verify table can be opened
    let reopened_sink = DeltaSink::try_open(&storage, vec![], "test".to_string())
        .await
        .unwrap();
    assert!(reopened_sink.version() >= 0);

    // Step 7: Verify we can recover checkpoint from the new table
    let mut sink_for_recovery = DeltaSink::try_open(&storage, vec![], "test".to_string())
        .await
        .unwrap();
    let recovered = sink_for_recovery
        .recover_checkpoint_from_log()
        .await
        .unwrap();
    assert!(recovered.is_some(), "Should find checkpoint in new table");
}

/// Test: Schema evolution in merge mode allows adding new nullable columns.
///
/// Verifies that:
/// - A table is created with initial schema
/// - Incoming schema with new nullable fields triggers merge evolution
/// - The merged schema includes both original and new fields
#[tokio::test]
async fn test_schema_evolution_merge_mode() {
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use penguin::schema::evolution::EvolutionAction;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let storage = StorageProvider::for_url_with_options(table_path, HashMap::new())
        .await
        .unwrap();

    // Create table with initial schema
    let initial_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let mut delta_sink = DeltaSink::new(&storage, &initial_schema, vec![], "test".to_string())
        .await
        .unwrap();

    // Verify initial schema is cached
    assert!(delta_sink.schema().is_some());
    assert_eq!(delta_sink.schema().unwrap().fields().len(), 2);

    // Incoming schema with new nullable field
    let incoming_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true), // New nullable field
    ]);

    // Validate schema evolution in merge mode
    let action = delta_sink
        .validate_schema(&incoming_schema, SchemaEvolutionMode::Merge)
        .unwrap();

    match action {
        EvolutionAction::Merge { ref new_schema } => {
            assert_eq!(new_schema.fields().len(), 3);
            assert_eq!(new_schema.field(2).name(), "email");
        }
        _ => panic!("Expected Merge action, got {action:?}"),
    }

    // Apply the evolution
    delta_sink.evolve_schema(action).await.unwrap();

    // Verify schema was updated
    assert_eq!(delta_sink.schema().unwrap().fields().len(), 3);

    // Verify we can reopen the table and see the evolved schema
    let reopened = DeltaSink::try_open(&storage, vec![], "test".to_string())
        .await
        .unwrap();
    assert_eq!(reopened.schema().unwrap().fields().len(), 3);
}

/// Test: Schema evolution in strict mode rejects any schema changes.
///
/// Verifies that:
/// - Strict mode rejects even compatible schema changes (new nullable fields)
/// - Appropriate error is returned
#[tokio::test]
async fn test_schema_evolution_strict_mode_rejects() {
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let storage = StorageProvider::for_url_with_options(table_path, HashMap::new())
        .await
        .unwrap();

    // Create table with initial schema
    let initial_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let delta_sink = DeltaSink::new(&storage, &initial_schema, vec![], "test".to_string())
        .await
        .unwrap();

    // Incoming schema with new field
    let incoming_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true), // New field
    ]);

    // Strict mode should reject
    let result = delta_sink.validate_schema(&incoming_schema, SchemaEvolutionMode::Strict);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, penguin::error::SchemaError::IncompatibleSchema { .. }),
        "Expected IncompatibleSchema error, got: {err:?}"
    );
}

/// Test: Schema evolution in overwrite mode replaces schema entirely.
///
/// Verifies that:
/// - Overwrite mode accepts completely different schemas
/// - The new schema replaces the old one
#[tokio::test]
async fn test_schema_evolution_overwrites() {
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use penguin::schema::evolution::EvolutionAction;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let storage = StorageProvider::for_url_with_options(table_path, HashMap::new())
        .await
        .unwrap();

    // Create table with initial schema
    let initial_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]);

    let mut delta_sink = DeltaSink::new(&storage, &initial_schema, vec![], "test".to_string())
        .await
        .unwrap();

    // Completely different schema
    let new_schema = Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("data", DataType::Utf8, true),
    ]);

    // Overwrite mode should accept any schema
    let action = delta_sink
        .validate_schema(&new_schema, SchemaEvolutionMode::Overwrite)
        .unwrap();

    match action {
        EvolutionAction::Overwrite { ref new_schema } => {
            assert_eq!(new_schema.fields().len(), 3);
            assert_eq!(new_schema.field(0).name(), "user_id");
        }
        _ => panic!("Expected Overwrite action, got {action:?}"),
    }

    // Apply the overwrite
    delta_sink.evolve_schema(action).await.unwrap();

    // Verify schema was replaced
    let schema = delta_sink.schema().unwrap();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "user_id");
    assert_eq!(schema.field(1).name(), "timestamp");
    assert_eq!(schema.field(2).name(), "data");
}

/// Test: Schema evolution rejects new required (non-nullable) fields in merge mode.
///
/// Verifies that merge mode correctly rejects attempts to add required fields,
/// which would break existing data.
#[tokio::test]
async fn test_schema_evolution_rejects_required_fields() {
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let storage = StorageProvider::for_url_with_options(table_path, HashMap::new())
        .await
        .unwrap();

    // Create table with initial schema
    let initial_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);

    let delta_sink = DeltaSink::new(&storage, &initial_schema, vec![], "test".to_string())
        .await
        .unwrap();

    // Incoming schema with new REQUIRED field
    let incoming_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("required_field", DataType::Utf8, false), // Non-nullable!
    ]);

    // Merge mode should reject required fields
    let result = delta_sink.validate_schema(&incoming_schema, SchemaEvolutionMode::Merge);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(
            err,
            penguin::error::SchemaError::RequiredFieldAddition { .. }
        ),
        "Expected RequiredFieldAddition error, got: {err:?}"
    );
}

/// Test: Schema evolution allows type widening (Int32 -> Int64).
///
/// Verifies that compatible type changes (widening) are allowed in merge mode
/// and that data can be written and read back correctly.
#[tokio::test]
async fn test_schema_evolution_allows_type_widening() {
    use deltalake::arrow::array::Int32Array;
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use deltalake::arrow::record_batch::RecordBatch;
    use deltalake::parquet::arrow::ArrowWriter;
    use penguin::schema::evolution::EvolutionAction;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    let storage = StorageProvider::for_url_with_options(table_path, HashMap::new())
        .await
        .unwrap();

    // Create table with Int32 field
    let initial_schema = Schema::new(vec![Field::new("value", DataType::Int32, true)]);

    let mut delta_sink = DeltaSink::new(&storage, &initial_schema, vec![], "test".to_string())
        .await
        .unwrap();

    // Incoming schema with Int64 (widened type)
    let incoming_schema = Schema::new(vec![Field::new("value", DataType::Int64, true)]);

    // Merge mode should allow type widening
    let action = delta_sink
        .validate_schema(&incoming_schema, SchemaEvolutionMode::Merge)
        .unwrap();

    // Type widening doesn't require schema change - data is compatible
    assert!(
        matches!(action, EvolutionAction::None),
        "Expected None action for compatible type widening, got {action:?}"
    );

    // Actually write Int32 data to verify the table works
    let int32_schema = Arc::new(initial_schema.clone());
    let batch = RecordBatch::try_new(
        int32_schema.clone(),
        vec![Arc::new(Int32Array::from(vec![Some(42), Some(100), None]))],
    )
    .unwrap();

    // Write parquet file
    let parquet_path = temp_dir.path().join("data.parquet");
    let mut buffer = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, int32_schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    std::fs::write(&parquet_path, &buffer).unwrap();

    // Commit the file
    let files = vec![FinishedFile::without_bytes(
        "data.parquet".to_string(),
        buffer.len(),
        3,
        HashMap::new(),
        None,
    )];

    let checkpoint = CheckpointState {
        schema_version: 2,
        source_state: SourceState::new(),
        delta_version: 0,
        watermark: WatermarkState::Initial,
    };

    delta_sink
        .commit_files_with_checkpoint(&files, &checkpoint)
        .await
        .unwrap();

    // Reopen the table and verify it can be queried
    let reopened = DeltaSink::try_open(&storage, vec![], "test".to_string())
        .await
        .unwrap();

    // Table should have Int32 schema (original)
    let schema = reopened.schema().unwrap();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "value");
    assert_eq!(schema.field(0).data_type(), &DataType::Int32);
}
