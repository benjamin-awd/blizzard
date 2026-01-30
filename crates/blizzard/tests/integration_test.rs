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
pipelines:
  events:
    source:
      path: "s3://bucket/input/*.ndjson.gz"
      compression: gzip
      batch_size: 4096
    sink:
      table_uri: "s3://bucket/output/table"
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
        let (_, pipeline) = config.pipelines().next().unwrap();

        assert_eq!(pipeline.source.path, "s3://bucket/input/*.ndjson.gz");
        assert_eq!(pipeline.source.batch_size, 4096);
        assert_eq!(pipeline.sink.file_size_mb, 64);
        assert_eq!(pipeline.schema.fields.len(), 4);

        // Test schema conversion
        let arrow_schema = pipeline.schema.to_arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 4);
        assert_eq!(arrow_schema.field(0).name(), "id");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: "/input/*.ndjson.gz"
    sink:
      table_uri: "/output/table"
    schema:
      fields:
        - name: data
          type: string
"#;
        let config: blizzard::config::Config = serde_yaml::from_str(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();

        // Check defaults
        assert_eq!(pipeline.source.batch_size, 8192);
        assert_eq!(pipeline.sink.file_size_mb, 128);
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
pipelines:
  test:
    source:
      path: "/input"
    sink:
      table_uri: "/output"
    schema:
      fields:
        - name: test_field
          type: {}
"#,
                name
            );
            let config: blizzard::config::Config = serde_yaml::from_str(&yaml).unwrap();
            let (_, pipeline) = config.pipelines().next().unwrap();
            let schema = pipeline.schema.to_arrow_schema();
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

        let pending = state.filter_pending_files(all_files);

        // file1 is finished, so only file2 and file3 should be pending
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"file2.ndjson.gz".to_string()));
        assert!(pending.contains(&"file3.ndjson.gz".to_string()));
        assert!(!pending.contains(&"file1.ndjson.gz".to_string()));
    }
}

mod parquet_tests {
    use super::*;
    use blizzard::config::ParquetCompression;
    use blizzard::sink::{ParquetWriter, ParquetWriterConfig};

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
        let mut writer = ParquetWriter::new(schema, config, "test".to_string()).unwrap();

        let batch = create_test_batch(100);
        writer.write_batch(&batch).unwrap();

        assert!(writer.current_file_size() > 0);
    }

    #[test]
    fn test_parquet_writer_multiple_batches() {
        let schema = test_schema();
        let config = ParquetWriterConfig::default();
        let mut writer = ParquetWriter::new(schema, config, "test".to_string()).unwrap();

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
            partition_values: std::collections::HashMap::new(),
            source_file: None,
        };

        assert_eq!(file.filename, "data-001.parquet");
        assert_eq!(file.size, 1024 * 1024);
        assert_eq!(file.record_count, 10000);
    }
}

mod dlq_tests {
    use blizzard::config::ErrorHandlingConfig;
    use blizzard::dlq::DeadLetterQueue;
    use blizzard_common::metrics::events::FailureStage;
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

        // Finalize
        dlq.finalize().await.expect("Failed to finalize DLQ");

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
            let record: serde_json::Value =
                serde_json::from_str(line).expect("Each line should be valid JSON");
            assert!(record.get("path").and_then(|v| v.as_str()).is_some());
            assert!(record.get("error").and_then(|v| v.as_str()).is_some());
            assert_eq!(record.get("retry_count").and_then(|v| v.as_u64()), Some(0));
            if let Some(stage) = record.get("stage").and_then(|v| v.as_str()) {
                stages_seen.push(stage.to_string());
            }
        }

        // Verify all stages are represented
        assert!(stages_seen.iter().any(|s| s == "download"));
        assert!(stages_seen.iter().any(|s| s == "decompress"));
        assert!(stages_seen.iter().any(|s| s == "parse"));
        assert!(stages_seen.iter().any(|s| s == "upload"));
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
        let record: serde_json::Value = serde_json::from_str(content.trim()).unwrap();

        assert!(record["path"].as_str().unwrap().contains("quotes"));
        assert!(record["error"].as_str().unwrap().contains("newlines"));
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
pipelines:
  events:
    source:
      path: "/input/*.ndjson.gz"
    sink:
      table_uri: "/output/table"
    schema:
      fields:
        - name: id
          type: string
    error_handling:
      max_failures: 100
      dlq_path: "/var/log/blizzard/dlq"
"#;
        let config: blizzard::config::Config = serde_yaml::from_str(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();
        assert_eq!(pipeline.error_handling.max_failures, 100);
        assert_eq!(
            pipeline.error_handling.dlq_path,
            Some("/var/log/blizzard/dlq".to_string())
        );
    }
}

mod polling_tests {
    use blizzard::config::Config;

    #[test]
    fn test_polling_config_defaults() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: "/input/*.ndjson.gz"
    sink:
      table_uri: "/output/table"
    schema:
      fields:
        - name: id
          type: string
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();

        // Check default poll interval
        assert_eq!(
            pipeline.source.poll_interval_secs, 60,
            "default poll interval should be 60s"
        );
    }

    #[test]
    fn test_polling_config_yaml_parsing() {
        let yaml = r#"
pipelines:
  events:
    source:
      path: "s3://bucket/input/*.ndjson.gz"
      poll_interval_secs: 30
    sink:
      table_uri: "s3://bucket/output/table"
    schema:
      fields:
        - name: id
          type: string
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();

        assert_eq!(
            pipeline.source.poll_interval_secs, 30,
            "poll interval should be 30s"
        );
    }
}

mod shutdown_tests {
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    /// Test that shutdown token clones share cancellation state.
    /// This verifies the fix for the bug where a separate local token
    /// was created in the process() method instead of using the shared one.
    #[tokio::test]
    async fn test_shutdown_propagation() {
        let shutdown = CancellationToken::new();
        let shutdown_for_processor = shutdown.clone();
        let shutdown_for_downloader = shutdown_for_processor.clone();

        // Spawn a mock "processing" task
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_for_downloader.cancelled() => {
                    "shutdown_received"
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    "timeout"
                }
            }
        });

        // Simulate signal handler cancelling the original token
        shutdown.cancel();

        // The processing task should receive the cancellation
        let result = tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("task should complete quickly")
            .expect("task should not panic");

        assert_eq!(result, "shutdown_received");
    }

    /// Test that creating a new token (the bug) does NOT propagate cancellation
    #[tokio::test]
    async fn test_separate_token_does_not_receive_cancellation() {
        let shutdown = CancellationToken::new();
        let separate_token = CancellationToken::new(); // Bug: new token instead of clone

        // Spawn a mock "processing" task with the WRONG token
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = separate_token.cancelled() => {
                    "shutdown_received"
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
                    "timeout"
                }
            }
        });

        // Cancel the original token
        shutdown.cancel();

        // The task will NOT receive cancellation because it has a different token
        let result = handle.await.expect("task should not panic");

        assert_eq!(
            result, "timeout",
            "separate token should NOT receive cancellation from original"
        );
    }
}
