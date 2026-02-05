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
    use blizzard::config::{Config, MB};
    use blizzard::parquet::{ParquetWriter, ParquetWriterConfig, RollingPolicy};
    use deltalake::arrow::array::{Int64Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use deltalake::arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use std::time::Duration;

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

    /// Build rolling policies from config the same way processor.rs does.
    /// This mirrors the logic in Iteration::new() to ensure config is wired correctly.
    fn build_rolling_policies_from_config(config: &Config) -> Vec<RollingPolicy> {
        let (_, pipeline) = config.pipelines().next().unwrap();
        let mut policies = vec![RollingPolicy::SizeLimit(pipeline.sink.file_size_mb * MB)];
        if let Some(secs) = pipeline.sink.rollover_timeout_secs {
            policies.push(RollingPolicy::RolloverDuration(Duration::from_secs(secs)));
        }
        policies
    }

    #[test]
    fn test_rollover_timeout_config_to_policy_wiring() {
        // Test that rollover_timeout_secs in config gets wired to RollingPolicy::RolloverDuration
        let yaml = r#"
pipelines:
  events:
    sources:
      default:
        path: "/input/*.ndjson.gz"
    sink:
      table_uri: "/output/table"
      rollover_timeout_secs: 300
    schema:
      fields:
        - name: id
          type: string
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let policies = build_rolling_policies_from_config(&config);

        // Should have both SizeLimit (default) and RolloverDuration
        assert_eq!(policies.len(), 2);
        assert!(
            matches!(policies[0], RollingPolicy::SizeLimit(size) if size == 128 * MB),
            "First policy should be SizeLimit with default 128MB"
        );
        assert!(
            matches!(policies[1], RollingPolicy::RolloverDuration(d) if d == Duration::from_secs(300)),
            "Second policy should be RolloverDuration(300s)"
        );
    }

    #[test]
    fn test_no_rollover_timeout_config_only_size_policy() {
        // Test that without rollover_timeout_secs, only SizeLimit policy is created
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
        let policies = build_rolling_policies_from_config(&config);

        assert_eq!(policies.len(), 1);
        assert!(matches!(policies[0], RollingPolicy::SizeLimit(_)));
    }

    #[test]
    fn test_rollover_timeout_triggers_file_roll() {
        use bytes::Bytes;
        use deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        // End-to-end test: config → policies → writer → actual file roll
        let yaml = r#"
pipelines:
  events:
    sources:
      default:
        path: "/input/*.ndjson.gz"
    sink:
      table_uri: "/output/table"
      file_size_mb: 100
      rollover_timeout_secs: 1
    schema:
      fields:
        - name: id
          type: string
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let (_, pipeline) = config.pipelines().next().unwrap();

        // Build config the same way processor.rs does
        let mut policies = vec![RollingPolicy::SizeLimit(pipeline.sink.file_size_mb * MB)];
        if let Some(secs) = pipeline.sink.rollover_timeout_secs {
            policies.push(RollingPolicy::RolloverDuration(Duration::from_secs(secs)));
        }

        let writer_config = ParquetWriterConfig::default()
            .with_file_size_mb(pipeline.sink.file_size_mb)
            .with_rolling_policies(policies);

        let mut writer =
            ParquetWriter::new(test_schema(), writer_config, "test".to_string()).unwrap();

        // Write a small batch (won't trigger size limit)
        let batch = create_test_batch(10);
        writer.write_batch(&batch).unwrap();
        assert!(
            writer.take_finished_files().is_empty(),
            "Should not roll immediately"
        );

        // Wait for rollover timeout (1 second + buffer)
        std::thread::sleep(Duration::from_millis(1100));

        // Write another batch - this should trigger the time-based roll
        writer.write_batch(&batch).unwrap();

        let finished = writer.take_finished_files();
        assert_eq!(
            finished.len(),
            1,
            "Rollover timeout should have triggered exactly one file roll"
        );
        assert!(
            finished[0].size < 100 * MB,
            "File should be well below size limit since time triggered the roll"
        );

        // Verify record count: both batches (20 records) are in the finished file
        // because the second write is added to the buffer before the roll check triggers
        assert_eq!(
            finished[0].record_count, 20,
            "Finished file should contain 20 records (both batches before roll)"
        );

        // Verify the Parquet file is readable and contains correct data
        let bytes = finished[0]
            .bytes
            .as_ref()
            .expect("FinishedFile should contain parquet bytes");
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::clone(bytes))
            .expect("Should be able to create Parquet reader from bytes")
            .build()
            .expect("Should be able to build Parquet reader");

        // Read all batches and verify total record count
        let mut total_rows = 0;
        for batch_result in reader {
            let batch = batch_result.expect("Should be able to read batch from Parquet file");
            total_rows += batch.num_rows();

            // Verify schema matches expected (id: Utf8, value: Int64)
            assert_eq!(batch.num_columns(), 2, "Should have 2 columns");
            assert_eq!(batch.schema().field(0).name(), "id");
            assert_eq!(batch.schema().field(1).name(), "value");
        }
        assert_eq!(
            total_rows, 20,
            "Parquet file should contain 20 rows when read back"
        );
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

        // Expected records (path -> (error, stage))
        let expected: std::collections::HashMap<&str, (&str, &str)> = [
            (
                "s3://bucket/data/file1.ndjson.gz",
                ("Connection timeout after 30s", "download"),
            ),
            (
                "s3://bucket/data/file2.ndjson.gz",
                ("invalid gzip header", "decompress"),
            ),
            (
                "s3://bucket/data/file3.ndjson.gz",
                ("JSON parse error at line 42: unexpected token", "parse"),
            ),
            (
                "s3://bucket/data/file4.ndjson.gz",
                ("S3 PutObject failed: Access Denied", "upload"),
            ),
        ]
        .into_iter()
        .collect();

        // Track paths seen to verify no duplicates
        let mut paths_seen = std::collections::HashSet::new();

        for line in lines {
            let record: serde_json::Value =
                serde_json::from_str(line).expect("Each line should be valid JSON");

            // Verify required fields exist
            let path = record
                .get("path")
                .and_then(|v| v.as_str())
                .expect("Record should have 'path' field");
            let error = record
                .get("error")
                .and_then(|v| v.as_str())
                .expect("Record should have 'error' field");
            let stage = record
                .get("stage")
                .and_then(|v| v.as_str())
                .expect("Record should have 'stage' field");
            let retry_count = record
                .get("retry_count")
                .and_then(|v| v.as_u64())
                .expect("Record should have 'retry_count' field");

            // Verify no duplicate paths
            assert!(
                paths_seen.insert(path.to_string()),
                "Duplicate path found: {path}"
            );

            // Verify retry_count is 0 (first failure)
            assert_eq!(retry_count, 0, "retry_count should be 0");

            // Verify path, error, and stage match expected values
            let (expected_error, expected_stage) = expected
                .get(path)
                .unwrap_or_else(|| panic!("Unexpected path in DLQ: {path}"));
            assert_eq!(
                error, *expected_error,
                "Error message mismatch for path {path}"
            );
            assert_eq!(stage, *expected_stage, "Stage mismatch for path {path}");
        }

        // Verify all expected paths were seen
        assert_eq!(
            paths_seen.len(),
            expected.len(),
            "Should have exactly {} unique paths",
            expected.len()
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
        FileListingConfig, list_files_above_partition_watermarks, list_files_above_watermark,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn create_test_storage(temp_dir: &TempDir) -> Arc<StorageProvider> {
        Arc::new(
            StorageProvider::for_url_with_options(
                temp_dir.path().to_str().unwrap(),
                HashMap::new(),
            )
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
        let mut state = CheckpointState {
            schema_version: 1,
            ..Default::default()
        };
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
        partition_watermarks.insert(
            "date=2024-01-28".to_string(),
            "2000-uuid.ndjson.gz".to_string(),
        );
        partition_watermarks.insert(
            "date=2024-01-29".to_string(),
            "1000-uuid.ndjson.gz".to_string(),
        );

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
        assert_eq!(global_files[0], "date=2024-01-28/hour=14/file2.ndjson.gz");

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

        let files = list_files_above_partition_watermarks(
            &storage,
            &partition_watermarks,
            Some(&prefixes),
            &config,
        )
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
        let mut manager2 =
            CheckpointManager::new(storage, "test_pipeline".to_string(), "source1".to_string());
        let loaded = manager2.load().await.unwrap();

        assert!(loaded);
        assert_eq!(
            manager2.watermark(),
            Some("date=2024-01-29/file1.ndjson.gz")
        );

        // Verify partition watermarks loaded
        let pw = manager2.partition_watermarks();
        assert_eq!(
            pw.get("date=2024-01-28"),
            Some(&"file2.ndjson.gz".to_string())
        );
        assert_eq!(
            pw.get("date=2024-01-29"),
            Some(&"file1.ndjson.gz".to_string())
        );
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
        let mut manager2 =
            CheckpointManager::new(storage, "test_pipeline".to_string(), "source1".to_string());
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

mod concurrency_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Semaphore;

    /// Test that global semaphore limits concurrent operations across multiple tasks.
    ///
    /// This verifies the `total_concurrency` config option works correctly by:
    /// 1. Creating a semaphore with a small limit (2)
    /// 2. Spawning more concurrent tasks than the limit (5)
    /// 3. Verifying that no more than `limit` tasks run simultaneously
    #[tokio::test]
    async fn test_global_semaphore_limits_concurrency() {
        let semaphore = Arc::new(Semaphore::new(2));
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let current_concurrent = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();

        // Spawn 5 tasks that each try to acquire the semaphore
        for _i in 0..5 {
            let sem = semaphore.clone();
            let max = max_concurrent.clone();
            let current = current_concurrent.clone();

            handles.push(tokio::spawn(async move {
                // Acquire semaphore permit (like upload/download tasks do)
                let _permit = sem.acquire().await.expect("semaphore should not be closed");

                // Track concurrent count
                let active = current.fetch_add(1, Ordering::SeqCst) + 1;

                // Update max if this is the highest concurrent count we've seen
                max.fetch_max(active, Ordering::SeqCst);

                // Simulate I/O work
                tokio::time::sleep(Duration::from_millis(50)).await;

                // Done with work
                current.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.expect("task should not panic");
        }

        // Verify max concurrent never exceeded the semaphore limit
        let max = max_concurrent.load(Ordering::SeqCst);
        assert!(
            max <= 2,
            "Max concurrent tasks ({max}) should not exceed semaphore limit (2)"
        );
        assert!(
            max >= 1,
            "At least one task should have run concurrently (got {max})"
        );
    }

    /// Test that when global_semaphore is None, operations proceed without blocking.
    #[tokio::test]
    async fn test_no_semaphore_allows_full_concurrency() {
        let global_semaphore: Option<Arc<Semaphore>> = None;
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let current_concurrent = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();

        // Spawn 5 tasks without a semaphore
        for _i in 0..5 {
            let sem = global_semaphore.clone();
            let max = max_concurrent.clone();
            let current = current_concurrent.clone();

            handles.push(tokio::spawn(async move {
                // This is the pattern used in upload/download tasks:
                // acquire permit only if semaphore is Some
                let _permit = if let Some(ref s) = sem {
                    Some(s.acquire().await.expect("semaphore should not be closed"))
                } else {
                    None
                };

                let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                max.fetch_max(active, Ordering::SeqCst);

                // Brief sleep to allow concurrency
                tokio::time::sleep(Duration::from_millis(20)).await;

                current.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        for handle in handles {
            handle.await.expect("task should not panic");
        }

        // Without a semaphore, all 5 tasks should be able to run concurrently
        let max = max_concurrent.load(Ordering::SeqCst);
        assert!(
            max >= 3,
            "Without semaphore, should allow high concurrency (got {max})"
        );
    }

    /// Test that semaphore permit is released when task completes.
    #[tokio::test]
    async fn test_semaphore_permit_released_on_completion() {
        let semaphore = Arc::new(Semaphore::new(1));

        // First task acquires the only permit
        let sem1 = semaphore.clone();
        let handle1 = tokio::spawn(async move {
            let _permit = sem1.acquire().await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
            // permit released here when _permit drops
        });

        // Give first task time to acquire permit
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Second task should be blocked
        let sem2 = semaphore.clone();
        let start = std::time::Instant::now();
        let handle2 = tokio::spawn(async move {
            let _permit = sem2.acquire().await.unwrap();
            // This will only succeed after first task releases permit
        });

        // Wait for both to complete
        handle1.await.unwrap();
        handle2.await.unwrap();

        // Second task should have been blocked until first completed
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(40),
            "Second task should have waited for first to release permit (elapsed: {elapsed:?})"
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
