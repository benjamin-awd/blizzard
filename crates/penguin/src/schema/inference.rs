//! Schema inference from parquet files.
//!
//! This module provides utilities to infer Arrow schema from parquet files,
//! allowing Penguin to create Delta tables with the correct schema instead
//! of using a placeholder.
//!
//! Inferred schemas are automatically coerced to be Delta Lake compatible
//! (e.g., timestamp precision is converted to microseconds).

use bytes::Bytes;
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::parquet::arrow::parquet_to_arrow_schema;
use deltalake::parquet::file::reader::{FileReader, SerializedFileReader};
use tracing::{debug, warn};

use blizzard_common::{FinishedFile, StorageProvider};

use crate::error::SchemaError;
use crate::schema::evolution::coerce_schema;

/// Maximum number of files to try when inferring schema.
///
/// If the first file is corrupted or inaccessible, we try subsequent files
/// up to this limit before giving up.
const MAX_SCHEMA_INFERENCE_ATTEMPTS: usize = 3;

/// Infer Arrow schema from parquet file bytes.
///
/// This reads only the parquet footer (metadata), not the actual data,
/// making it efficient for schema inference.
///
/// The inferred schema is automatically coerced to be Delta Lake compatible:
/// - Timestamp precision is converted to microseconds (ns/ms -> μs)
/// - Nested types (List, Struct) are recursively coerced
pub fn infer_schema_from_parquet_bytes(bytes: &Bytes) -> Result<SchemaRef, SchemaError> {
    // Create a serialized file reader from the bytes
    let reader = SerializedFileReader::new(bytes.clone())
        .map_err(|source| SchemaError::ParquetFooter { source })?;

    // Get the parquet metadata
    let metadata = reader.metadata();

    // Convert parquet schema to Arrow schema
    let schema = parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
        .map_err(|source| SchemaError::ArrowConversion { source })?;

    // Coerce schema to be Delta Lake compatible (e.g., timestamp precision)
    Ok(coerce_schema(&schema))
}

/// Extract UUID from a filename like "date=2024-01-01/uuid.parquet" -> "uuid"
fn extract_uuid(filename: &str) -> &str {
    filename
        .split('/')
        .next_back()
        .unwrap_or(filename)
        .trim_end_matches(".parquet")
}

/// Infer schema from the first available parquet file.
///
/// Tries up to 3 files in case some are corrupted or inaccessible.
/// Returns the schema from the first file that can be successfully read.
///
/// Note: Files are read from `_staging/pending/{uuid}.parquet` (where blizzard
/// writes them) rather than the target path in `file.filename`.
pub async fn infer_schema_from_first_file(
    storage: &StorageProvider,
    files: &[FinishedFile],
    table: &str,
) -> Result<SchemaRef, SchemaError> {
    if files.is_empty() {
        return Err(SchemaError::NoFilesAvailable);
    }

    let max_attempts = std::cmp::min(MAX_SCHEMA_INFERENCE_ATTEMPTS, files.len());
    let mut last_error = None;

    for file in files.iter().take(max_attempts) {
        // Files are in staging directory, not at the target path yet
        let uuid = extract_uuid(&file.filename);
        let staging_path = format!("_staging/pending/{}.parquet", uuid);

        debug!(target = %table, "Attempting to infer schema from staging file: {}", staging_path);

        match storage.get(staging_path.as_str()).await {
            Ok(bytes) => match infer_schema_from_parquet_bytes(&bytes) {
                Ok(schema) => {
                    debug!(
                        target = %table,
                        "Successfully inferred schema with {} fields from {}",
                        schema.fields().len(),
                        staging_path
                    );
                    return Ok(schema);
                }
                Err(e) => {
                    warn!(
                        target = %table,
                        "Failed to parse parquet schema from {}: {}",
                        staging_path, e
                    );
                    last_error = Some(e);
                }
            },
            Err(e) => {
                warn!(target = %table, "Failed to read file {}: {}", staging_path, e);
                last_error = Some(SchemaError::StorageRead { source: e });
            }
        }
    }

    // Return the last error we encountered
    Err(last_error.unwrap_or(SchemaError::NoFilesAvailable))
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Int32Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use deltalake::arrow::record_batch::RecordBatch;
    use deltalake::parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    fn create_test_parquet_bytes() -> Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        Bytes::from(buffer)
    }

    #[test]
    fn test_infer_schema_from_parquet_bytes() {
        let bytes = create_test_parquet_bytes();
        let schema = infer_schema_from_parquet_bytes(&bytes).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_infer_schema_invalid_bytes() {
        let bytes = Bytes::from_static(b"not a parquet file");
        let result = infer_schema_from_parquet_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_infer_schema_empty_bytes() {
        let bytes = Bytes::new();
        let result = infer_schema_from_parquet_bytes(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_infer_schema_preserves_nullability() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("required_field", DataType::Int32, false),
            Field::new("nullable_field", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![Some(2)])),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let bytes = Bytes::from(buffer);
        let inferred = infer_schema_from_parquet_bytes(&bytes).unwrap();

        assert!(!inferred.field(0).is_nullable());
        assert!(inferred.field(1).is_nullable());
    }

    #[test]
    fn test_infer_schema_coerces_timestamp_to_microseconds() {
        use deltalake::arrow::array::TimestampNanosecondArray;
        use deltalake::arrow::datatypes::TimeUnit;

        // Create a schema with nanosecond timestamp (not Delta-compatible)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(TimestampNanosecondArray::from(vec![Some(
                    1234567890123456789i64,
                )])),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let bytes = Bytes::from(buffer);
        let inferred = infer_schema_from_parquet_bytes(&bytes).unwrap();

        // Timestamp should be coerced to microseconds for Delta Lake compatibility
        assert_eq!(inferred.fields().len(), 2);
        assert_eq!(
            inferred.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[tokio::test]
    async fn test_infer_schema_from_first_file_with_storage() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path();

        // Create staging directory and write parquet file there (as blizzard does)
        let staging_dir = table_path.join("_staging/pending");
        std::fs::create_dir_all(&staging_dir).unwrap();

        let parquet_bytes = create_test_parquet_bytes();
        let parquet_path = staging_dir.join("test-uuid.parquet");
        std::fs::write(&parquet_path, &parquet_bytes).unwrap();

        // Create storage provider
        let storage =
            StorageProvider::for_url_with_options(table_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap();

        // Create a FinishedFile with target path (schema inference reads from staging)
        let files = vec![FinishedFile::without_bytes(
            "date=2024-01-01/test-uuid.parquet".to_string(),
            parquet_bytes.len(),
            3,
            HashMap::new(),
            None,
        )];

        let schema = infer_schema_from_first_file(&storage, &files, "test")
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
    }

    #[tokio::test]
    async fn test_infer_schema_from_first_file_empty_list() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let storage = StorageProvider::for_url_with_options(
            temp_dir.path().to_str().unwrap(),
            HashMap::new(),
        )
        .await
        .unwrap();

        let result = infer_schema_from_first_file(&storage, &[], "test").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SchemaError::NoFilesAvailable));
    }

    #[tokio::test]
    async fn test_infer_schema_tries_multiple_files() {
        use std::collections::HashMap;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path();

        // Create staging directory
        let staging_dir = table_path.join("_staging/pending");
        std::fs::create_dir_all(&staging_dir).unwrap();

        // Write a valid parquet file as the second file (in staging)
        let parquet_bytes = create_test_parquet_bytes();
        let parquet_path = staging_dir.join("valid-uuid.parquet");
        std::fs::write(&parquet_path, &parquet_bytes).unwrap();

        // Create storage provider
        let storage =
            StorageProvider::for_url_with_options(table_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap();

        // First file doesn't exist in staging, second file is valid
        let files = vec![
            FinishedFile::without_bytes(
                "date=2024-01-01/nonexistent-uuid.parquet".to_string(),
                100,
                1,
                HashMap::new(),
                None,
            ),
            FinishedFile::without_bytes(
                "date=2024-01-01/valid-uuid.parquet".to_string(),
                parquet_bytes.len(),
                3,
                HashMap::new(),
                None,
            ),
        ];

        // Should succeed by trying the second file
        let schema = infer_schema_from_first_file(&storage, &files, "test")
            .await
            .unwrap();

        assert_eq!(schema.fields().len(), 2);
    }
}
