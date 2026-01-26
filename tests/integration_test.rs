//! Integration tests for blizzard

use deltalake::arrow::array::{Int64Array, StringArray};
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::arrow::record_batch::RecordBatch;
use std::sync::Arc;

mod config_tests {
    use super::*;

    #[test]
    fn test_config_yaml_parsing() {
        let yaml = r#"
source:
  path: "s3://bucket/input/*.ndjson.gz"
  compression: gzip
  batch_size: 4096

sink:
  path: "s3://bucket/output/table"
  file_size_mb: 64
  compression: zstd

schema:
  fields:
    - name: id
      type: string
    - name: timestamp
      type: timestamp
    - name: value
      type: float64
      nullable: true
    - name: count
      type: int64
"#;
        let config: blizzard::config::Config = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.source.path, "s3://bucket/input/*.ndjson.gz");
        assert_eq!(config.source.batch_size, 4096);
        assert_eq!(config.sink.file_size_mb, 64);
        assert_eq!(config.schema.fields.len(), 4);

        // Test schema conversion
        let arrow_schema = config.to_arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 4);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
source:
  path: "/input/*.ndjson.gz"

sink:
  path: "/output/table"

schema:
  fields:
    - name: data
      type: string
"#;
        let config: blizzard::config::Config = serde_yaml::from_str(yaml).unwrap();

        // Check defaults
        assert_eq!(config.source.batch_size, 8192);
        assert_eq!(config.sink.file_size_mb, 128);
    }

    #[test]
    fn test_field_types() {
        use blizzard::config::FieldType;

        let types = vec![
            ("string", FieldType::String, DataType::Utf8),
            ("int32", FieldType::Int32, DataType::Int32),
            ("int64", FieldType::Int64, DataType::Int64),
            ("float32", FieldType::Float32, DataType::Float32),
            ("float64", FieldType::Float64, DataType::Float64),
            ("boolean", FieldType::Boolean, DataType::Boolean),
        ];

        for (name, _field_type, expected_arrow) in types {
            let yaml = format!(
                r#"
source:
  path: "/input"
sink:
  path: "/output"
schema:
  fields:
    - name: test_field
      type: {}
"#,
                name
            );
            let config: blizzard::config::Config = serde_yaml::from_str(&yaml).unwrap();
            let schema = config.to_arrow_schema();
            assert_eq!(
                schema.field(0).data_type(),
                &expected_arrow,
                "Failed for type: {}",
                name
            );
        }
    }
}

mod storage_tests {
    use blizzard::storage::BackendConfig;

    #[test]
    fn test_s3_url_parsing() {
        let config = BackendConfig::parse_url("s3://mybucket/path/to/data", false).unwrap();
        match config {
            BackendConfig::S3(s3) => {
                assert_eq!(s3.bucket, "mybucket");
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_gcs_url_parsing() {
        let config = BackendConfig::parse_url("gs://mybucket/path/to/data", false).unwrap();
        match config {
            BackendConfig::Gcs(gcs) => {
                assert_eq!(gcs.bucket, "mybucket");
            }
            _ => panic!("Expected GCS config"),
        }
    }

    #[test]
    fn test_local_url_parsing() {
        let config = BackendConfig::parse_url("/local/path/to/data", false).unwrap();
        match config {
            BackendConfig::Local(local) => {
                assert_eq!(local.path, "/local/path/to/data");
            }
            _ => panic!("Expected Local config"),
        }
    }

    #[test]
    fn test_file_url_parsing() {
        let config = BackendConfig::parse_url("file:///local/path/to/data", false).unwrap();
        match config {
            BackendConfig::Local(local) => {
                assert_eq!(local.path, "/local/path/to/data");
            }
            _ => panic!("Expected Local config"),
        }
    }

    #[test]
    fn test_azure_url_parsing() {
        let config = BackendConfig::parse_url(
            "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/path/to/data",
            false,
        )
        .unwrap();
        match config {
            BackendConfig::Azure(azure) => {
                assert_eq!(azure.account, "mystorageaccount");
                assert_eq!(azure.container, "mycontainer");
            }
            _ => panic!("Expected Azure config"),
        }
    }

    #[test]
    fn test_invalid_url() {
        let result = BackendConfig::parse_url("invalid://url", false);
        assert!(result.is_err());
    }
}

mod checkpoint_tests {
    use blizzard::source::SourceState;

    #[test]
    fn test_source_state_tracking() {
        let mut state = SourceState::new();

        // Track file progress
        state.update_records("file1.ndjson.gz", 100);
        assert_eq!(state.records_to_skip("file1.ndjson.gz"), 100);
        assert!(!state.is_file_finished("file1.ndjson.gz"));

        // Update progress
        state.update_records("file1.ndjson.gz", 250);
        assert_eq!(state.records_to_skip("file1.ndjson.gz"), 250);

        // Mark as finished
        state.mark_finished("file1.ndjson.gz");
        assert!(state.is_file_finished("file1.ndjson.gz"));
        assert_eq!(state.records_to_skip("file1.ndjson.gz"), 0); // Finished files skip 0
    }

    #[test]
    fn test_pending_files_filtering() {
        let mut state = SourceState::new();
        state.mark_finished("file1.ndjson.gz");
        state.update_records("file2.ndjson.gz", 50);

        let all_files = vec![
            "file1.ndjson.gz".to_string(),
            "file2.ndjson.gz".to_string(),
            "file3.ndjson.gz".to_string(),
        ];

        let pending = state.pending_files(&all_files);

        // file1 is finished, so only file2 and file3 should be pending
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"file2.ndjson.gz"));
        assert!(pending.contains(&"file3.ndjson.gz"));
        assert!(!pending.contains(&"file1.ndjson.gz"));
    }
}

mod parquet_tests {
    use super::*;
    use blizzard::config::ParquetCompression;
    use blizzard::sink::parquet::{ParquetWriter, ParquetWriterConfig};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let ids: Vec<String> = (0..num_rows).map(|i| format!("id_{}", i)).collect();
        let values: Vec<i64> = (0..num_rows).map(|i| i as i64 * 10).collect();

        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_parquet_writer_basic() {
        let schema = test_schema();
        let config = ParquetWriterConfig::default();
        let mut writer = ParquetWriter::new(schema, config);

        let batch = create_test_batch(100);
        writer.write_batch(&batch).unwrap();

        assert!(writer.current_file_size() > 0);
    }

    #[test]
    fn test_parquet_writer_multiple_batches() {
        let schema = test_schema();
        let config = ParquetWriterConfig::default();
        let mut writer = ParquetWriter::new(schema, config);

        for _ in 0..5 {
            let batch = create_test_batch(100);
            writer.write_batch(&batch).unwrap();
        }

        assert!(writer.current_file_size() > 0);
    }

    #[test]
    fn test_parquet_writer_config() {
        let config = ParquetWriterConfig::default()
            .with_file_size_mb(64)
            .with_compression(ParquetCompression::Zstd);

        assert_eq!(config.target_file_size, 64 * 1024 * 1024);
    }
}

mod sink_tests {
    use blizzard::sink::FinishedFile;

    #[test]
    fn test_finished_file() {
        let file = FinishedFile {
            filename: "data-001.parquet".to_string(),
            size: 1024 * 1024,
            record_count: 10000,
            bytes: None,
        };

        assert_eq!(file.filename, "data-001.parquet");
        assert_eq!(file.size, 1024 * 1024);
        assert_eq!(file.record_count, 10000);
    }
}

mod dlq_tests {
    use blizzard::config::ErrorHandlingConfig;
    use blizzard::dlq::{DeadLetterQueue, FailedFile};
    use blizzard::metrics::events::FailureStage;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_dlq_end_to_end() {
        let temp_dir = TempDir::new().unwrap();
        let dlq_path = temp_dir.path().to_str().unwrap().to_string();

        let config = ErrorHandlingConfig {
            max_failures: 0,
            dlq_path: Some(dlq_path.clone()),
            dlq_storage_options: HashMap::new(),
        };

        // Create DLQ
        let dlq = DeadLetterQueue::from_config(&config)
            .await
            .expect("Failed to create DLQ")
            .expect("DLQ should be Some when path is configured");
        let dlq = Arc::new(dlq);

        // Simulate various failure scenarios
        dlq.record_failure(
            "s3://bucket/data/file1.ndjson.gz",
            "Connection timeout after 30s",
            FailureStage::Download,
        )
        .await;

        dlq.record_failure(
            "s3://bucket/data/file2.ndjson.gz",
            "invalid gzip header",
            FailureStage::Decompress,
        )
        .await;

        dlq.record_failure(
            "s3://bucket/data/file3.ndjson.gz",
            "JSON parse error at line 42: unexpected token",
            FailureStage::Parse,
        )
        .await;

        dlq.record_failure(
            "s3://bucket/data/file4.ndjson.gz",
            "S3 PutObject failed: Access Denied",
            FailureStage::Upload,
        )
        .await;

        // Finalize and verify stats
        let stats = dlq.finalize().await.expect("Failed to finalize DLQ");
        assert_eq!(stats.download, 1);
        assert_eq!(stats.decompress, 1);
        assert_eq!(stats.parse, 1);
        assert_eq!(stats.upload, 1);
        assert_eq!(stats.total(), 4);

        // Verify file was written
        let entries: Vec<_> = std::fs::read_dir(&dlq_path)
            .expect("Failed to read DLQ directory")
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "ndjson")
                    .unwrap_or(false)
            })
            .collect();

        assert_eq!(entries.len(), 1, "Should have exactly one NDJSON file");

        // Read and parse the NDJSON file
        let content = std::fs::read_to_string(entries[0].path()).expect("Failed to read DLQ file");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 4, "Should have 4 failure records");

        // Parse each line and verify structure
        let mut stages_seen = Vec::new();
        for line in lines {
            let record: FailedFile =
                serde_json::from_str(line).expect("Each line should be valid JSON");
            assert!(!record.path.is_empty());
            assert!(!record.error.is_empty());
            assert_eq!(record.retry_count, 0);
            stages_seen.push(record.stage);
        }

        // Verify all stages are represented
        assert!(
            stages_seen
                .iter()
                .any(|s| matches!(s, FailureStage::Download))
        );
        assert!(
            stages_seen
                .iter()
                .any(|s| matches!(s, FailureStage::Decompress))
        );
        assert!(stages_seen.iter().any(|s| matches!(s, FailureStage::Parse)));
        assert!(
            stages_seen
                .iter()
                .any(|s| matches!(s, FailureStage::Upload))
        );
    }

    #[tokio::test]
    async fn test_dlq_not_created_without_path() {
        let config = ErrorHandlingConfig {
            max_failures: 10,
            dlq_path: None,
            dlq_storage_options: HashMap::new(),
        };

        let dlq = DeadLetterQueue::from_config(&config)
            .await
            .expect("Should not error");
        assert!(dlq.is_none(), "DLQ should be None when no path configured");
    }

    #[tokio::test]
    async fn test_dlq_handles_special_characters_in_errors() {
        let temp_dir = TempDir::new().unwrap();
        let dlq_path = temp_dir.path().to_str().unwrap().to_string();

        let config = ErrorHandlingConfig {
            max_failures: 0,
            dlq_path: Some(dlq_path.clone()),
            dlq_storage_options: HashMap::new(),
        };

        let dlq = DeadLetterQueue::from_config(&config)
            .await
            .unwrap()
            .unwrap();

        // Test with special characters that might break JSON
        dlq.record_failure(
            "file with \"quotes\" and \\ backslashes.ndjson.gz",
            "Error with\nnewlines\tand\ttabs",
            FailureStage::Parse,
        )
        .await;

        dlq.finalize().await.unwrap();

        // Read and verify it's valid JSON
        let entries: Vec<_> = std::fs::read_dir(&dlq_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "ndjson")
                    .unwrap_or(false)
            })
            .collect();

        let content = std::fs::read_to_string(entries[0].path()).unwrap();
        let record: FailedFile = serde_json::from_str(content.trim()).unwrap();

        assert!(record.path.contains("quotes"));
        assert!(record.error.contains("newlines"));
    }

    #[tokio::test]
    async fn test_error_handling_config_defaults() {
        let config = ErrorHandlingConfig::default();
        assert_eq!(config.max_failures, 0);
        assert!(config.dlq_path.is_none());
        assert!(config.dlq_storage_options.is_empty());
    }

    #[tokio::test]
    async fn test_error_handling_config_yaml_parsing() {
        let yaml = r#"
source:
  path: "/input/*.ndjson.gz"

sink:
  path: "/output/table"

schema:
  fields:
    - name: id
      type: string

error_handling:
  max_failures: 100
  dlq_path: "/var/log/blizzard/dlq"
"#;
        let config: blizzard::config::Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.error_handling.max_failures, 100);
        assert_eq!(
            config.error_handling.dlq_path,
            Some("/var/log/blizzard/dlq".to_string())
        );
    }
}

mod delta_atomic_checkpoint_tests {
    use blizzard::checkpoint::CheckpointState;
    use blizzard::sink::FinishedFile;
    use blizzard::sink::delta::DeltaSink;
    use blizzard::source::SourceState;
    use blizzard::storage::StorageProvider;
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ])
    }

    #[tokio::test]
    async fn test_commit_files_with_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(table_path, HashMap::new())
                .await
                .unwrap(),
        );

        let schema = test_schema();
        let mut delta_sink = DeltaSink::new(storage, &schema).await.unwrap();

        // Create checkpoint state
        let mut source_state = SourceState::new();
        source_state.update_records("file1.ndjson.gz", 100);

        let checkpoint = CheckpointState {
            schema_version: 1,
            source_state,
            delta_version: 0,
        };

        // Create test files
        let files = vec![FinishedFile {
            filename: "part-00000.parquet".to_string(),
            size: 1024,
            record_count: 100,
            bytes: None,
        }];

        // Commit with checkpoint
        let result = delta_sink
            .commit_files_with_checkpoint(&files, &checkpoint)
            .await;
        assert!(result.is_ok(), "Commit should succeed");
        assert!(result.unwrap().is_some(), "Should return new version");
        assert!(delta_sink.version() > 0);
        assert_eq!(delta_sink.checkpoint_version(), 1);
    }

    #[tokio::test]
    async fn test_recover_checkpoint_from_log() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(table_path, HashMap::new())
                .await
                .unwrap(),
        );

        let schema = test_schema();
        let mut delta_sink = DeltaSink::new(storage.clone(), &schema).await.unwrap();

        // Create and commit checkpoint
        let mut source_state = SourceState::new();
        source_state.update_records("file1.ndjson.gz", 500);
        source_state.mark_finished("file2.ndjson.gz");

        let checkpoint = CheckpointState {
            schema_version: 1,
            source_state: source_state.clone(),
            delta_version: 0,
        };

        let files = vec![FinishedFile {
            filename: "part-00000.parquet".to_string(),
            size: 1024,
            record_count: 500,
            bytes: None,
        }];

        delta_sink
            .commit_files_with_checkpoint(&files, &checkpoint)
            .await
            .unwrap();

        // Now create a new delta sink (simulating restart) and recover
        let mut new_delta_sink = DeltaSink::new(storage, &schema).await.unwrap();
        let recovered = new_delta_sink.recover_checkpoint_from_log().await.unwrap();

        assert!(recovered.is_some(), "Should recover checkpoint");
        let (recovered_state, recovered_version) = recovered.unwrap();

        assert_eq!(recovered_version, 1);
        assert_eq!(recovered_state.schema_version, 1);
        assert!(
            recovered_state
                .source_state
                .is_file_finished("file2.ndjson.gz")
        );
    }

    #[tokio::test]
    async fn test_no_checkpoint_in_empty_table() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        let storage = Arc::new(
            StorageProvider::for_url_with_options(table_path, HashMap::new())
                .await
                .unwrap(),
        );

        let schema = test_schema();
        let mut delta_sink = DeltaSink::new(storage, &schema).await.unwrap();

        let recovered = delta_sink.recover_checkpoint_from_log().await.unwrap();
        assert!(recovered.is_none(), "Empty table should have no checkpoint");
    }
}
