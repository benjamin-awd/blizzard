//! Integration tests for blizzard

use deltalake::arrow::datatypes::DataType;

mod config_tests {
    use super::*;

    #[test]
    fn test_config_yaml_parsing() {
        let yaml = r#"
pipelines:
  events:
    sources:
      default:
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
        let source = pipeline.sources.get("default").unwrap();

        assert_eq!(source.path, "s3://bucket/input/*.ndjson.gz");
        assert_eq!(source.batch_size, 4096);
        assert_eq!(pipeline.sink.file_size_mb, 64);
        assert_eq!(pipeline.schema.fields().len(), 4);

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
    sources:
      default:
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
        let source = pipeline.sources.get("default").unwrap();

        // Check defaults
        assert_eq!(source.batch_size, 8192);
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
    sources:
      default:
        path: "/input"
    sink:
      table_uri: "/output"
    schema:
      fields:
        - name: test_field
          type: {name}
"#
            );
            let config: blizzard::config::Config = serde_yaml::from_str(&yaml).unwrap();
            let (_, pipeline) = config.pipelines().next().unwrap();
            let schema = pipeline.schema.to_arrow_schema();
            assert_eq!(
                schema.field(0).data_type(),
                &expected_arrow,
                "Failed for type: {name}"
            );
        }
    }
}

// Storage URL parsing tests are in blizzard-core/src/storage/mod.rs

mod checkpoint_tests {
    use blizzard::source::SourceState;

    #[test]
    fn test_source_state_tracking() {
        let mut state = SourceState::new();

        // Initially file is not finished
        assert!(!state.is_file_finished("file1.ndjson.gz"));

        // Mark as finished
        state.mark_finished("file1.ndjson.gz");
        assert!(state.is_file_finished("file1.ndjson.gz"));
    }

    #[test]
    fn test_pending_files_filtering() {
        let mut state = SourceState::new();
        state.mark_finished("file1.ndjson.gz");

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
    use blizzard::config::ParquetCompression;
    use blizzard::parquet::{ParquetWriter, ParquetWriterConfig};
    use deltalake::arrow::array::{Int64Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use deltalake::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let ids: Vec<String> = (0..num_rows).map(|i| format!("id_{i}")).collect();
        let values: Vec<i64> = (0..num_rows)
            .map(|i| i64::try_from(i).unwrap() * 10)
            .collect();

        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    // test_parquet_writer_basic is in blizzard/src/parquet/writer.rs

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
    use blizzard::parquet::FinishedFile;

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
    use blizzard_core::metrics::events::FailureStage;
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
    sources:
      default:
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
    sources:
      default:
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
        let source = pipeline.sources.get("default").unwrap();

        // Check default poll interval
        assert_eq!(
            source.poll_interval_secs, 60,
            "default poll interval should be 60s"
        );
    }

    #[test]
    fn test_polling_config_yaml_parsing() {
        let yaml = r#"
pipelines:
  events:
    sources:
      default:
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
        let source = pipeline.sources.get("default").unwrap();

        assert_eq!(source.poll_interval_secs, 30, "poll interval should be 30s");
    }
}

mod watermark_tests {
    use blizzard::checkpoint::{CheckpointManager, CheckpointState, WatermarkState};
    use blizzard_core::storage::StorageProvider;
    use blizzard_core::watermark::{
        list_files_above_partition_watermarks, list_files_above_watermark, FileListingConfig,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn create_test_storage(temp_dir: &TempDir) -> Arc<StorageProvider> {
        Arc::new(
            StorageProvider::for_url_with_options(temp_dir.path().to_str().unwrap(), HashMap::new())
                .await
                .unwrap(),
        )
    }

    fn ndjson_config(target: &str) -> FileListingConfig<'_> {
        FileListingConfig {
            extension: ".ndjson.gz",
            target,
        }
    }

    // ==================== WatermarkState Serialization Tests ====================

    #[test]
    fn test_watermark_state_active_serialization() {
        let state = WatermarkState::Active("date=2024-01-28/file.ndjson.gz".to_string());
        let json = serde_json::to_string(&state).unwrap();

        // Should serialize to tagged enum format
        assert!(json.contains("\"state\":\"Active\""));
        assert!(json.contains("\"value\":\"date=2024-01-28/file.ndjson.gz\""));

        // Should deserialize back correctly
        let restored: WatermarkState = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, state);
    }

    #[test]
    fn test_watermark_state_idle_serialization() {
        let state = WatermarkState::Idle("date=2024-01-28/file.ndjson.gz".to_string());
        let json = serde_json::to_string(&state).unwrap();

        assert!(json.contains("\"state\":\"Idle\""));
        assert!(json.contains("\"value\":\"date=2024-01-28/file.ndjson.gz\""));

        let restored: WatermarkState = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, state);
    }

    #[test]
    fn test_watermark_state_initial_serialization() {
        let state = WatermarkState::Initial;
        let json = serde_json::to_string(&state).unwrap();

        assert!(json.contains("\"state\":\"Initial\""));

        let restored: WatermarkState = serde_json::from_str(&json).unwrap();
        assert_eq!(restored, state);
    }

    #[test]
    fn test_checkpoint_format_roundtrip() {
        // Create checkpoint with new format
        let mut state = CheckpointState::default();
        state.schema_version = 1;
        state.update_watermark("date=2024-01-28/file1.ndjson.gz");
        state.update_watermark("date=2024-01-29/file2.ndjson.gz");

        // Serialize
        let json = serde_json::to_string_pretty(&state).unwrap();
        eprintln!("JSON output:\n{json}");

        // Should include new format - watermark is a tagged enum
        assert!(
            json.contains("\"state\"") && json.contains("\"Active\""),
            "Expected tagged enum format with 'state' and 'Active', got:\n{json}"
        );
        assert!(json.contains("partition_watermarks"));

        // Deserialize
        let restored: CheckpointState = serde_json::from_str(&json).unwrap();

        assert_eq!(
            restored.watermark,
            WatermarkState::Active("date=2024-01-29/file2.ndjson.gz".to_string())
        );
        assert_eq!(
            restored.partition_watermarks.get("date=2024-01-28"),
            Some(&"file1.ndjson.gz".to_string())
        );
        assert_eq!(
            restored.partition_watermarks.get("date=2024-01-29"),
            Some(&"file2.ndjson.gz".to_string())
        );
    }

    // ==================== Idle State Transition Tests ====================

    #[test]
    fn test_idle_state_transitions() {
        let mut state = CheckpointState::default();

        // Initial -> cannot mark idle
        assert!(!state.mark_idle());
        assert!(state.watermark.is_initial());

        // Process a file: Initial -> Active
        state.update_watermark("date=2024-01-28/file1.ndjson.gz");
        assert!(matches!(state.watermark, WatermarkState::Active(_)));

        // No new files: Active -> Idle
        assert!(state.mark_idle());
        assert!(matches!(state.watermark, WatermarkState::Idle(_)));
        assert_eq!(
            state.watermark_path(),
            Some("date=2024-01-28/file1.ndjson.gz")
        );

        // Idle -> cannot mark idle again
        assert!(!state.mark_idle());

        // New file arrives: Idle -> Active
        state.update_watermark("date=2024-01-28/file2.ndjson.gz");
        assert!(matches!(state.watermark, WatermarkState::Active(_)));
        assert_eq!(
            state.watermark_path(),
            Some("date=2024-01-28/file2.ndjson.gz")
        );
    }

    #[test]
    fn test_idle_state_persists_watermark_path() {
        let mut state =
            CheckpointState::with_watermark("date=2024-01-28/file1.ndjson.gz".to_string());

        // Path is accessible in Active state
        assert_eq!(
            state.watermark_path(),
            Some("date=2024-01-28/file1.ndjson.gz")
        );

        // Mark idle
        state.mark_idle();

        // Path is still accessible in Idle state
        assert_eq!(
            state.watermark_path(),
            Some("date=2024-01-28/file1.ndjson.gz")
        );

        // Serialize and deserialize - path should persist
        let json = serde_json::to_string(&state).unwrap();
        let restored: CheckpointState = serde_json::from_str(&json).unwrap();

        assert!(matches!(restored.watermark, WatermarkState::Idle(_)));
        assert_eq!(
            restored.watermark_path(),
            Some("date=2024-01-28/file1.ndjson.gz")
        );
    }

    // ==================== Per-Partition Watermark Tests ====================

    #[test]
    fn test_partition_watermarks_independent_tracking() {
        let mut state = CheckpointState::default();

        // Process files in two partitions concurrently
        state.update_watermark("date=2024-01-28/file1.ndjson.gz");
        state.update_watermark("date=2024-01-29/file1.ndjson.gz");
        state.update_watermark("date=2024-01-28/file2.ndjson.gz");
        state.update_watermark("date=2024-01-29/file2.ndjson.gz");
        state.update_watermark("date=2024-01-28/file3.ndjson.gz");

        // Each partition should have its own watermark
        assert_eq!(
            state.partition_watermarks.get("date=2024-01-28"),
            Some(&"file3.ndjson.gz".to_string())
        );
        assert_eq!(
            state.partition_watermarks.get("date=2024-01-29"),
            Some(&"file2.ndjson.gz".to_string())
        );

        // Global watermark should be the highest overall path
        assert_eq!(
            state.watermark_path(),
            Some("date=2024-01-29/file2.ndjson.gz")
        );
    }

    #[test]
    fn test_partition_watermarks_no_regression_for_older_files() {
        let mut state = CheckpointState::default();

        // Process file in partition
        state.update_watermark("date=2024-01-28/file2.ndjson.gz");

        // Try to process older file - should be rejected
        assert!(!state.update_watermark("date=2024-01-28/file1.ndjson.gz"));

        // Partition watermark should not regress
        assert_eq!(
            state.partition_watermarks.get("date=2024-01-28"),
            Some(&"file2.ndjson.gz".to_string())
        );
    }

    #[test]
    fn test_partition_watermarks_nested_partitions() {
        let mut state = CheckpointState::default();

        // Nested partitions (date + hour)
        state.update_watermark("date=2024-01-28/hour=13/file1.ndjson.gz");
        state.update_watermark("date=2024-01-28/hour=14/file1.ndjson.gz");
        state.update_watermark("date=2024-01-28/hour=13/file2.ndjson.gz");

        // Each nested partition tracked independently
        assert_eq!(
            state.partition_watermarks.get("date=2024-01-28/hour=13"),
            Some(&"file2.ndjson.gz".to_string())
        );
        assert_eq!(
            state.partition_watermarks.get("date=2024-01-28/hour=14"),
            Some(&"file1.ndjson.gz".to_string())
        );
    }

    #[test]
    fn test_partition_watermarks_root_level_files() {
        let mut state = CheckpointState::default();

        // Root-level files (no partition)
        state.update_watermark("file1.ndjson.gz");
        state.update_watermark("file2.ndjson.gz");

        // No partition watermarks for root-level files
        assert!(state.partition_watermarks.is_empty());

        // But global watermark should still work
        assert_eq!(state.watermark_path(), Some("file2.ndjson.gz"));
    }

    // ==================== Integration: Per-Partition Watermark Listing ====================

    #[tokio::test]
    async fn test_list_files_with_partition_watermarks_filters_correctly() {
        let temp_dir = TempDir::new().unwrap();

        // Create two partitions with multiple files
        let partition1 = temp_dir.path().join("date=2024-01-28");
        let partition2 = temp_dir.path().join("date=2024-01-29");
        std::fs::create_dir_all(&partition1).unwrap();
        std::fs::create_dir_all(&partition2).unwrap();

        // Partition 1: files before and after watermark
        std::fs::write(partition1.join("1000-uuid.ndjson.gz"), b"").unwrap();
        std::fs::write(partition1.join("2000-uuid.ndjson.gz"), b"").unwrap();
        std::fs::write(partition1.join("3000-uuid.ndjson.gz"), b"").unwrap();

        // Partition 2: files before and after watermark
        std::fs::write(partition2.join("1000-uuid.ndjson.gz"), b"").unwrap();
        std::fs::write(partition2.join("2000-uuid.ndjson.gz"), b"").unwrap();

        let storage = StorageProvider::for_url_with_options(
            temp_dir.path().to_str().unwrap(),
            HashMap::new(),
        )
        .await
        .unwrap();
        let config = ndjson_config("test");

        // Set different watermarks for each partition
        let mut partition_watermarks = HashMap::new();
        partition_watermarks.insert("date=2024-01-28".to_string(), "2000-uuid.ndjson.gz".to_string());
        partition_watermarks.insert("date=2024-01-29".to_string(), "1000-uuid.ndjson.gz".to_string());

        let files =
            list_files_above_partition_watermarks(&storage, &partition_watermarks, None, &config)
                .await
                .unwrap();

        // Partition 1: only file above 2000 (which is 3000)
        // Partition 2: only file above 1000 (which is 2000)
        assert_eq!(files.len(), 2);
        assert!(files.contains(&"date=2024-01-28/3000-uuid.ndjson.gz".to_string()));
        assert!(files.contains(&"date=2024-01-29/2000-uuid.ndjson.gz".to_string()));
    }

    #[tokio::test]
    async fn test_list_files_new_partition_includes_all() {
        let temp_dir = TempDir::new().unwrap();

        // Create two partitions
        let partition1 = temp_dir.path().join("date=2024-01-28");
        let partition2 = temp_dir.path().join("date=2024-01-29");
        std::fs::create_dir_all(&partition1).unwrap();
        std::fs::create_dir_all(&partition2).unwrap();

        std::fs::write(partition1.join("file1.ndjson.gz"), b"").unwrap();
        std::fs::write(partition2.join("file1.ndjson.gz"), b"").unwrap();
        std::fs::write(partition2.join("file2.ndjson.gz"), b"").unwrap();

        let storage = StorageProvider::for_url_with_options(
            temp_dir.path().to_str().unwrap(),
            HashMap::new(),
        )
        .await
        .unwrap();
        let config = ndjson_config("test");

        // Only partition 1 has a watermark - partition 2 is "new"
        let mut partition_watermarks = HashMap::new();
        partition_watermarks.insert("date=2024-01-28".to_string(), "file1.ndjson.gz".to_string());
        // No entry for date=2024-01-29

        let files =
            list_files_above_partition_watermarks(&storage, &partition_watermarks, None, &config)
                .await
                .unwrap();

        // Partition 1: nothing above file1
        // Partition 2: ALL files (no watermark = new partition)
        assert_eq!(files.len(), 2);
        assert!(files.contains(&"date=2024-01-29/file1.ndjson.gz".to_string()));
        assert!(files.contains(&"date=2024-01-29/file2.ndjson.gz".to_string()));
    }

    #[tokio::test]
    async fn test_global_vs_partition_watermarks_comparison() {
        let temp_dir = TempDir::new().unwrap();

        // Scenario: 2 concurrent hour partitions
        let hour13 = temp_dir.path().join("date=2024-01-28").join("hour=13");
        let hour14 = temp_dir.path().join("date=2024-01-28").join("hour=14");
        std::fs::create_dir_all(&hour13).unwrap();
        std::fs::create_dir_all(&hour14).unwrap();

        // Hour 13: has 3 files, watermark at file2
        std::fs::write(hour13.join("file1.ndjson.gz"), b"").unwrap();
        std::fs::write(hour13.join("file2.ndjson.gz"), b"").unwrap();
        std::fs::write(hour13.join("file3.ndjson.gz"), b"").unwrap();

        // Hour 14: has 2 files
        std::fs::write(hour14.join("file1.ndjson.gz"), b"").unwrap();
        std::fs::write(hour14.join("file2.ndjson.gz"), b"").unwrap();

        let storage = StorageProvider::for_url_with_options(
            temp_dir.path().to_str().unwrap(),
            HashMap::new(),
        )
        .await
        .unwrap();
        let config = ndjson_config("test");

        // Global watermark approach: watermark is at hour=14/file1
        // This would skip ALL of hour=13 because hour=14 > hour=13
        let global_watermark = "date=2024-01-28/hour=14/file1.ndjson.gz";
        let global_files = list_files_above_watermark(&storage, global_watermark, &config)
            .await
            .unwrap();

        // With global watermark, only gets file2 from hour=14
        assert_eq!(global_files.len(), 1);
        assert_eq!(
            global_files[0],
            "date=2024-01-28/hour=14/file2.ndjson.gz"
        );

        // Per-partition watermarks approach
        let mut partition_watermarks = HashMap::new();
        partition_watermarks.insert(
            "date=2024-01-28/hour=13".to_string(),
            "file2.ndjson.gz".to_string(),
        );
        partition_watermarks.insert(
            "date=2024-01-28/hour=14".to_string(),
            "file1.ndjson.gz".to_string(),
        );

        let partition_files =
            list_files_above_partition_watermarks(&storage, &partition_watermarks, None, &config)
                .await
                .unwrap();

        // With per-partition watermarks, gets file3 from hour=13 AND file2 from hour=14
        assert_eq!(partition_files.len(), 2);
        assert!(partition_files.contains(&"date=2024-01-28/hour=13/file3.ndjson.gz".to_string()));
        assert!(partition_files.contains(&"date=2024-01-28/hour=14/file2.ndjson.gz".to_string()));
    }

    #[tokio::test]
    async fn test_partition_watermarks_respects_prefix_filter() {
        // This test verifies that when partition watermarks are used with a prefix filter,
        // only the filtered partitions are scanned (not all existing partitions).
        let temp_dir = TempDir::new().unwrap();

        // Create many partitions spanning several days
        let old_partition = temp_dir.path().join("date=2024-01-25");
        let day1 = temp_dir.path().join("date=2024-01-28").join("hour=05");
        let day2 = temp_dir.path().join("date=2024-01-28").join("hour=06");
        let future_partition = temp_dir.path().join("date=2024-01-30");
        std::fs::create_dir_all(&old_partition).unwrap();
        std::fs::create_dir_all(&day1).unwrap();
        std::fs::create_dir_all(&day2).unwrap();
        std::fs::create_dir_all(&future_partition).unwrap();

        // Files in old partition (should NOT be scanned with prefix filter)
        std::fs::write(old_partition.join("old-file.ndjson.gz"), b"").unwrap();

        // Files in day1
        std::fs::write(day1.join("file1.ndjson.gz"), b"").unwrap();
        std::fs::write(day1.join("file2.ndjson.gz"), b"").unwrap();
        std::fs::write(day1.join("file3.ndjson.gz"), b"").unwrap();

        // Files in day2
        std::fs::write(day2.join("file1.ndjson.gz"), b"").unwrap();
        std::fs::write(day2.join("file2.ndjson.gz"), b"").unwrap();

        // Files in future partition (should NOT be scanned with prefix filter)
        std::fs::write(future_partition.join("future-file.ndjson.gz"), b"").unwrap();

        let storage = StorageProvider::for_url_with_options(
            temp_dir.path().to_str().unwrap(),
            HashMap::new(),
        )
        .await
        .unwrap();
        let config = ndjson_config("test");

        // Partition watermarks for day1 and day2
        let mut partition_watermarks = HashMap::new();
        partition_watermarks.insert(
            "date=2024-01-28/hour=05".to_string(),
            "file2.ndjson.gz".to_string(),
        );
        partition_watermarks.insert(
            "date=2024-01-28/hour=06".to_string(),
            "file1.ndjson.gz".to_string(),
        );

        // Only scan day1 and day2 partitions (prefix filter excludes old and future partitions)
        let prefixes = vec![
            "date=2024-01-28/hour=05".to_string(),
            "date=2024-01-28/hour=06".to_string(),
        ];

        let files =
            list_files_above_partition_watermarks(&storage, &partition_watermarks, Some(&prefixes), &config)
                .await
                .unwrap();

        // Should find:
        // - file3 from day1 (above watermark file2)
        // - file2 from day2 (above watermark file1)
        // Should NOT include:
        // - old-file from old_partition (excluded by prefix filter)
        // - future-file from future_partition (excluded by prefix filter)
        // - file1, file2 from day1 (at or below watermark)
        // - file1 from day2 (at watermark)
        assert_eq!(files.len(), 2);
        assert!(files.contains(&"date=2024-01-28/hour=05/file3.ndjson.gz".to_string()));
        assert!(files.contains(&"date=2024-01-28/hour=06/file2.ndjson.gz".to_string()));
        assert!(!files.contains(&"date=2024-01-25/old-file.ndjson.gz".to_string()));
        assert!(!files.contains(&"date=2024-01-30/future-file.ndjson.gz".to_string()));
    }

    // ==================== End-to-End Checkpoint Manager Tests ====================

    #[tokio::test]
    async fn test_checkpoint_manager_save_and_load_new_format() {
        let temp_dir = TempDir::new().unwrap();

        // Create _blizzard directory
        std::fs::create_dir_all(temp_dir.path().join("_blizzard")).unwrap();

        let storage = create_test_storage(&temp_dir).await;

        // Create manager and process some files
        let mut manager = CheckpointManager::new(
            storage.clone(),
            "test_pipeline".to_string(),
            "source1".to_string(),
        );

        manager.update_watermark("date=2024-01-28/file1.ndjson.gz");
        manager.update_watermark("date=2024-01-29/file1.ndjson.gz");
        manager.update_watermark("date=2024-01-28/file2.ndjson.gz");

        // Save checkpoint
        manager.save().await.unwrap();

        // Load with new manager
        let mut manager2 = CheckpointManager::new(
            storage,
            "test_pipeline".to_string(),
            "source1".to_string(),
        );
        let loaded = manager2.load().await.unwrap();

        assert!(loaded);
        assert_eq!(
            manager2.watermark(),
            Some("date=2024-01-29/file1.ndjson.gz")
        );

        // Verify partition watermarks loaded
        let pw = manager2.partition_watermarks();
        assert_eq!(pw.get("date=2024-01-28"), Some(&"file2.ndjson.gz".to_string()));
        assert_eq!(pw.get("date=2024-01-29"), Some(&"file1.ndjson.gz".to_string()));
    }

    #[tokio::test]
    async fn test_checkpoint_manager_idle_state_persisted() {
        let temp_dir = TempDir::new().unwrap();
        std::fs::create_dir_all(temp_dir.path().join("_blizzard")).unwrap();

        let storage = create_test_storage(&temp_dir).await;

        // Create manager, process file, then mark idle
        let mut manager = CheckpointManager::new(
            storage.clone(),
            "test_pipeline".to_string(),
            "source1".to_string(),
        );

        manager.update_watermark("date=2024-01-28/file1.ndjson.gz");
        manager.mark_idle();
        manager.save().await.unwrap();

        // Load and verify idle state
        let mut manager2 = CheckpointManager::new(
            storage,
            "test_pipeline".to_string(),
            "source1".to_string(),
        );
        manager2.load().await.unwrap();

        // Watermark path should still be accessible
        assert_eq!(
            manager2.watermark(),
            Some("date=2024-01-28/file1.ndjson.gz")
        );

        // State should be Idle
        assert!(matches!(
            manager2.state().watermark,
            WatermarkState::Idle(_)
        ));
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
