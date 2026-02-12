//! End-to-end pipeline tests: discover Parquet → commit to Delta Lake.
//!
//! These tests exercise the real data path through IncomingReader, schema
//! inference, DeltaSink, and CheckpointCoordinator.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use deltalake::arrow::array::{Int32Array, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::parquet::arrow::ArrowWriter;
use deltalake::parquet::file::properties::WriterProperties;
use tempfile::TempDir;

use blizzard_core::PartitionExtractor;
use blizzard_core::storage::StorageProvider;
use penguin::checkpoint::{CheckpointCoordinator, CheckpointState};
use penguin::incoming::{IncomingConfig, IncomingReader};
use penguin::schema::inference::infer_schema_from_files;
use penguin::sink::{CheckpointRecovery, DeltaSink, TableCommitter};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// Write a parquet file to disk and return the number of rows written.
fn write_parquet_file(path: &std::path::Path, schema: &SchemaRef, rows: &[(i32, &str)]) {
    let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
    let names: Vec<&str> = rows.iter().map(|(_, name)| *name).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap();

    let file = std::fs::File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test]
async fn test_discover_and_commit_pipeline() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let schema = test_schema();

    // Create partition directory and write 2 parquet files
    let partition_dir = table_path.join("date=2026-01-15");
    std::fs::create_dir_all(&partition_dir).unwrap();

    write_parquet_file(
        &partition_dir.join("00000001-0000-0000-0000-000000000001.parquet"),
        &schema,
        &[(1, "alice"), (2, "bob"), (3, "charlie")],
    );
    write_parquet_file(
        &partition_dir.join("00000002-0000-0000-0000-000000000002.parquet"),
        &schema,
        &[(4, "dave"), (5, "eve")],
    );

    // Create storage provider and IncomingReader
    let storage = Arc::new(
        StorageProvider::for_url_with_options(table_path.to_str().unwrap(), HashMap::new())
            .await
            .unwrap(),
    );

    let reader = IncomingReader::new(
        storage.clone(),
        "e2e-test".to_string(),
        IncomingConfig {
            partition_filter: None,
            partition_extractor: PartitionExtractor::all(),
        },
    );

    // Discover uncommitted files
    let uncommitted = reader
        .list_uncommitted_files(None, &HashSet::new(), true)
        .await
        .unwrap();

    assert_eq!(
        uncommitted.len(),
        2,
        "should discover both parquet files, found: {uncommitted:?}"
    );

    // Read parquet metadata for each file
    let mut finished_files = Vec::new();
    for incoming in &uncommitted {
        let finished = reader.read_parquet_metadata(incoming).await.unwrap();
        assert!(
            finished.record_count > 0,
            "file {} should have records",
            finished.filename
        );
        finished_files.push(finished);
    }

    let total_records: usize = finished_files.iter().map(|f| f.record_count).sum();
    assert_eq!(total_records, 5, "total records should be 5");

    // Infer schema from the first file
    let (inferred_schema, _) = infer_schema_from_files(&storage, &finished_files, "e2e-test")
        .await
        .unwrap();

    assert_eq!(inferred_schema.fields().len(), 2);
    assert_eq!(inferred_schema.field(0).name(), "id");
    assert_eq!(inferred_schema.field(1).name(), "name");

    // Create DeltaSink with inferred schema
    let mut delta_sink = DeltaSink::new(
        &storage,
        &inferred_schema,
        vec!["date".to_string()],
        "e2e-test".to_string(),
    )
    .await
    .unwrap();

    // Commit files with checkpoint
    let checkpoint_state = CheckpointState::default();
    let commit_result = delta_sink
        .commit_files_with_checkpoint(&finished_files, &checkpoint_state)
        .await
        .unwrap();

    assert!(
        commit_result.is_some(),
        "commit should return a version number"
    );

    // Verify Delta table state
    assert!(
        delta_sink.version() >= 1,
        "version should be >= 1 after commit"
    );

    let committed_paths = delta_sink.get_committed_paths().unwrap();
    assert_eq!(
        committed_paths.len(),
        2,
        "should have 2 committed paths, got: {committed_paths:?}"
    );

    // Verify both files are in committed paths.
    // On local filesystem, get_file_uris() returns full absolute paths; just
    // check that each file's relative path is a suffix of some committed path.
    for file in &finished_files {
        let found = committed_paths.iter().any(|p| p.ends_with(&file.filename));
        assert!(
            found,
            "committed paths should contain {}, but got: {:?}",
            file.filename, committed_paths
        );
    }

    // Verify checkpoint recovery from log
    let mut reopened_sink =
        DeltaSink::try_open(&storage, vec!["date".to_string()], "e2e-test".to_string())
            .await
            .unwrap();

    let recovered = reopened_sink.recover_checkpoint_from_log().await.unwrap();
    assert!(
        recovered.is_some(),
        "should recover checkpoint from Delta log"
    );

    // Verify list_uncommitted_files returns empty when committed paths are excluded.
    // On local filesystem, get_committed_paths() returns full absolute paths while
    // list_uncommitted_files returns paths relative to the table root. Extract relative
    // paths by finding and stripping the common table-root prefix.
    let canonical_table = table_path.canonicalize().unwrap();
    let table_prefix = format!(
        "{}/",
        canonical_table.to_str().unwrap().trim_start_matches('/')
    );
    let relative_committed: HashSet<String> = committed_paths
        .iter()
        .map(|p| {
            let stripped = p.trim_start_matches('/');
            stripped
                .strip_prefix(&table_prefix)
                .unwrap_or(stripped)
                .to_string()
        })
        .collect();

    let new_uncommitted = reader
        .list_uncommitted_files(None, &relative_committed, true)
        .await
        .unwrap();

    assert!(
        new_uncommitted.is_empty(),
        "should find no uncommitted files after commit, found: {new_uncommitted:?}"
    );
}

#[tokio::test]
async fn test_checkpoint_coordinator_with_delta_sink() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let schema = test_schema();

    // Write a parquet file
    let partition_dir = table_path.join("date=2026-02-01");
    std::fs::create_dir_all(&partition_dir).unwrap();

    write_parquet_file(
        &partition_dir.join("00000001-0000-0000-0000-000000000001.parquet"),
        &schema,
        &[(1, "alice"), (2, "bob")],
    );

    let storage = Arc::new(
        StorageProvider::for_url_with_options(table_path.to_str().unwrap(), HashMap::new())
            .await
            .unwrap(),
    );

    // Discover files
    let reader = IncomingReader::new(
        storage.clone(),
        "coord-test".to_string(),
        IncomingConfig {
            partition_filter: None,
            partition_extractor: PartitionExtractor::all(),
        },
    );

    let uncommitted = reader
        .list_uncommitted_files(None, &HashSet::new(), true)
        .await
        .unwrap();
    assert_eq!(uncommitted.len(), 1);

    let mut finished_files = Vec::new();
    for incoming in &uncommitted {
        finished_files.push(reader.read_parquet_metadata(incoming).await.unwrap());
    }

    // Create DeltaSink and CheckpointCoordinator
    let mut delta_sink = DeltaSink::new(
        &storage,
        &schema,
        vec!["date".to_string()],
        "coord-test".to_string(),
    )
    .await
    .unwrap();

    let coordinator = CheckpointCoordinator::new("coord-test".to_string());

    // Commit files through the coordinator
    let committed_count = coordinator
        .commit_files(&mut delta_sink, &finished_files, 10)
        .await
        .expect("commit should succeed");

    assert_eq!(committed_count, 1, "should commit 1 file");
    assert!(delta_sink.version() >= 1);

    // Verify coordinator captured version
    let captured_state = coordinator.capture_state().await;
    assert!(
        captured_state.delta_version >= 0,
        "captured state should have a valid delta version"
    );
}

/// Test that footer-only parquet metadata reading returns correct results.
///
/// Creates a parquet file with multiple row groups and verifies that
/// `read_parquet_metadata()` (which uses a suffix-range read of the last 64KB)
/// correctly extracts record count, file size, and partition values.
#[tokio::test]
async fn test_footer_only_metadata_read() {
    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path();
    let schema = test_schema();

    let partition_dir = table_path.join("date=2026-03-01");
    std::fs::create_dir_all(&partition_dir).unwrap();

    let file_path = partition_dir.join("00000001-0000-0000-0000-000000000001.parquet");

    // Write a parquet file with multiple row groups (max_row_group_size=2)
    // so we verify that row counts are summed across all row groups.
    let props = WriterProperties::builder()
        .set_max_row_group_size(2)
        .build();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
        ],
    )
    .unwrap();

    let file = std::fs::File::create(&file_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let expected_file_size: usize = std::fs::metadata(&file_path)
        .unwrap()
        .len()
        .try_into()
        .unwrap();

    // Set up IncomingReader and read metadata via footer-only path
    let storage = Arc::new(
        StorageProvider::for_url_with_options(table_path.to_str().unwrap(), HashMap::new())
            .await
            .unwrap(),
    );

    let reader = IncomingReader::new(
        storage.clone(),
        "footer-test".to_string(),
        IncomingConfig {
            partition_filter: None,
            partition_extractor: PartitionExtractor::all(),
        },
    );

    let incoming = penguin::incoming::IncomingFile {
        path: "date=2026-03-01/00000001-0000-0000-0000-000000000001.parquet".to_string(),
        size: 0, // size is unknown at listing time
    };

    let finished = reader.read_parquet_metadata(&incoming).await.unwrap();

    // Verify record count (5 rows across 3 row groups: 2+2+1)
    assert_eq!(
        finished.record_count, 5,
        "should count all rows across row groups"
    );

    // Verify file size comes from suffix-range response metadata, not from IncomingFile.size
    assert_eq!(
        finished.size, expected_file_size,
        "file size should match actual file on disk"
    );

    // Verify partition values were extracted from the path
    assert_eq!(
        finished.partition_values.get("date"),
        Some(&"2026-03-01".to_string()),
        "should extract partition values from path"
    );

    // Verify schema inference also works via footer-only path
    let (inferred, _) = infer_schema_from_files(&storage, &[finished], "footer-test")
        .await
        .unwrap();

    assert_eq!(inferred.fields().len(), 2);
    assert_eq!(inferred.field(0).name(), "id");
    assert_eq!(inferred.field(1).name(), "name");
}
