//! Schema inference from NDJSON files.
//!
//! This module uses Arrow's built-in JSON schema inference to automatically detect
//! field names and types from NDJSON files.

use std::io::{BufRead, Cursor};
use std::sync::Arc;

use bytes::Bytes;
use deltalake::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use deltalake::arrow::json::reader::infer_json_schema;
use tracing::{debug, info, warn};

use blizzard_core::{StorageProviderRef, storage::list_ndjson_files_with_prefixes};

use super::compression::CompressionCodecExt;
use crate::config::CompressionFormat;
use crate::error::InferenceError;

/// Number of records to sample for schema inference.
const SAMPLE_SIZE: usize = 1000;

/// Maximum number of files to try for schema inference.
const MAX_FILE_ATTEMPTS: usize = 3;

/// Infer schema from the first available NDJSON file in storage.
///
/// Tries up to 3 files in case some are corrupted or inaccessible.
/// Returns the schema inferred from the first file that can be successfully read.
pub async fn infer_schema_from_source(
    storage: &StorageProviderRef,
    compression: CompressionFormat,
    prefixes: Option<&[String]>,
    pipeline: &str,
) -> Result<SchemaRef, InferenceError> {
    // List available files
    let files = list_ndjson_files_with_prefixes(storage, prefixes, pipeline)
        .await
        .map_err(|e| InferenceError::ReadFile { source: e })?;

    if files.is_empty() {
        return Err(InferenceError::NoFilesFound);
    }

    let max_attempts = std::cmp::min(MAX_FILE_ATTEMPTS, files.len());
    let mut last_error = None;

    for path in files.iter().take(max_attempts) {
        debug!(target = %pipeline, "Attempting to infer schema from file: {path}");

        match storage.get(path.as_str()).await {
            Ok(bytes) => match infer_schema_from_bytes(&bytes, compression) {
                Ok(schema) => {
                    info!(
                        target = %pipeline,
                        "Inferred schema with {} fields from {}: {:?}",
                        schema.fields().len(),
                        path,
                        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
                    );
                    return Ok(schema);
                }
                Err(e) => {
                    warn!(target = %pipeline, "Failed to infer schema from {path}: {e}");
                    last_error = Some(e);
                }
            },
            Err(e) => {
                warn!(target = %pipeline, "Failed to read file {path}: {e}");
                last_error = Some(InferenceError::ReadFile { source: e });
            }
        }
    }

    Err(last_error.unwrap_or(InferenceError::NoFilesFound))
}

/// Infer schema from compressed NDJSON bytes.
///
/// Decompresses the data, uses Arrow's built-in schema inference,
/// and coerces timestamps to microseconds for Delta Lake compatibility.
pub fn infer_schema_from_bytes(
    bytes: &Bytes,
    compression: CompressionFormat,
) -> Result<SchemaRef, InferenceError> {
    // Decompress into memory
    let decompressed = decompress(bytes, compression)?;

    // Use Arrow's built-in schema inference
    let reader: Box<dyn BufRead> = Box::new(Cursor::new(decompressed));
    let (schema, records_read) =
        infer_json_schema(reader, Some(SAMPLE_SIZE)).map_err(|e| InferenceError::JsonParse {
            message: e.to_string(),
        })?;

    if records_read == 0 {
        return Err(InferenceError::NoValidRecords);
    }

    debug!("Arrow inferred schema from {records_read} records");

    // Coerce schema for Delta Lake compatibility (timestamps to microseconds)
    Ok(coerce_schema(&schema))
}

/// Decompress bytes based on compression format.
fn decompress(bytes: &Bytes, compression: CompressionFormat) -> Result<Vec<u8>, InferenceError> {
    let codec = compression.codec();
    codec
        .decompress_bytes(bytes)
        .map_err(|e| InferenceError::Decompression { message: e.message })
}

/// Coerce schema to be Delta Lake compatible.
///
/// Delta Lake requires timestamp precision to be microseconds.
fn coerce_schema(schema: &Schema) -> SchemaRef {
    let fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|f| coerce_field(f.clone()))
        .collect();

    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Coerce a field to be Delta Lake compatible.
fn coerce_field(field: FieldRef) -> FieldRef {
    match field.data_type() {
        // Coerce timestamp precision to microseconds
        DataType::Timestamp(TimeUnit::Nanosecond | TimeUnit::Millisecond, tz) => {
            Arc::new(Field::new(
                field.name(),
                DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                field.is_nullable(),
            ))
        }
        // Recursively coerce List inner types
        DataType::List(inner) => {
            let coerced_inner = coerce_field(inner.clone());
            if Arc::ptr_eq(&coerced_inner, inner) {
                field
            } else {
                Arc::new(Field::new(
                    field.name(),
                    DataType::List(coerced_inner),
                    field.is_nullable(),
                ))
            }
        }
        // Recursively coerce Struct field types
        DataType::Struct(fields) => {
            let coerced_fields: Vec<FieldRef> =
                fields.iter().map(|f| coerce_field(f.clone())).collect();

            // Check if any field changed
            let any_changed = coerced_fields
                .iter()
                .zip(fields.iter())
                .any(|(c, o)| !Arc::ptr_eq(c, o));

            if any_changed {
                Arc::new(Field::new(
                    field.name(),
                    DataType::Struct(coerced_fields.into()),
                    field.is_nullable(),
                ))
            } else {
                field
            }
        }
        // All other types pass through unchanged
        _ => field,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn make_ndjson(records: &[&str]) -> Bytes {
        Bytes::from(records.join("\n"))
    }

    fn make_gzip_ndjson(records: &[&str]) -> Bytes {
        let json = records.join("\n");
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(json.as_bytes()).unwrap();
        Bytes::from(encoder.finish().unwrap())
    }

    #[test]
    fn test_infer_basic_types() {
        let bytes = make_ndjson(&[
            r#"{"id": 1, "name": "Alice", "active": true, "score": 95.5}"#,
            r#"{"id": 2, "name": "Bob", "active": false, "score": 87.0}"#,
        ]);

        let schema = infer_schema_from_bytes(&bytes, CompressionFormat::None).unwrap();

        assert_eq!(schema.fields().len(), 4);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("active").is_ok());
        assert!(schema.field_with_name("score").is_ok());
    }

    #[test]
    fn test_infer_nested_object() {
        let bytes = make_ndjson(&[r#"{"id": 1, "meta": {"key": "value", "count": 42}}"#]);

        let schema = infer_schema_from_bytes(&bytes, CompressionFormat::None).unwrap();

        let meta_field = schema.field_with_name("meta").unwrap();
        match meta_field.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
            }
            other => panic!("Expected Struct type, got {other:?}"),
        }
    }

    #[test]
    fn test_infer_array_type() {
        let bytes = make_ndjson(&[r#"{"tags": ["a", "b", "c"]}"#, r#"{"tags": ["d"]}"#]);

        let schema = infer_schema_from_bytes(&bytes, CompressionFormat::None).unwrap();

        let tags_field = schema.field_with_name("tags").unwrap();
        assert!(matches!(tags_field.data_type(), DataType::List(_)));
    }

    #[test]
    fn test_infer_with_gzip_compression() {
        let bytes = make_gzip_ndjson(&[
            r#"{"id": 1, "name": "Alice"}"#,
            r#"{"id": 2, "name": "Bob"}"#,
        ]);

        let schema = infer_schema_from_bytes(&bytes, CompressionFormat::Gzip).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
    }

    #[test]
    fn test_infer_empty_file_error() {
        let bytes = Bytes::new();
        let result = infer_schema_from_bytes(&bytes, CompressionFormat::None);
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_timestamp_to_microseconds() {
        use deltalake::arrow::datatypes::TimeUnit;

        let schema = Schema::new(vec![
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]);

        let coerced = coerce_schema(&schema);

        // All timestamps should be coerced to microseconds
        assert_eq!(
            coerced.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            coerced.field(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            coerced.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_coerce_nested_timestamp() {
        use deltalake::arrow::datatypes::TimeUnit;

        let schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ))]
                .into(),
            ),
            true,
        )]);

        let coerced = coerce_schema(&schema);

        match coerced.field(0).data_type() {
            DataType::Struct(fields) => {
                assert_eq!(
                    fields[0].data_type(),
                    &DataType::Timestamp(TimeUnit::Microsecond, None)
                );
            }
            other => panic!("Expected Struct, got {other:?}"),
        }
    }
}
