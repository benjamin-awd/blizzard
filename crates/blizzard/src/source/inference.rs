//! Schema inference from NDJSON files.
//!
//! This module uses Arrow's built-in JSON schema inference to automatically detect
//! field names and types from NDJSON files.
//!
//! When Arrow's inference fails due to type conflicts (e.g., the same field appearing
//! as both an object and a string), a fallback inference strategy is used that treats
//! conflicting fields as `Utf8` strings.

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Cursor};
use std::sync::Arc;

use bytes::Bytes;
use deltalake::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::json::reader::infer_json_schema;
use serde_json::Value;
use tracing::{debug, info, warn};

use blizzard_core::emit;
use blizzard_core::metrics::events::SchemaTypeConflicts;
use blizzard_core::{StorageProviderRef, storage::list_ndjson_files_with_prefixes};

use super::compression::CompressionCodecExt;
use crate::config::CompressionFormat;
use crate::error::InferenceError;

/// Number of records to sample for schema inference.
const SAMPLE_SIZE: usize = 1000;

/// Maximum number of files to try for schema inference.
const MAX_FILE_ATTEMPTS: usize = 3;

/// Simplified JSON type for conflict detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum JsonType {
    Null,
    Boolean,
    Integer,
    Float,
    String,
    Array,
    Object,
}

impl JsonType {
    /// Determine the JsonType from a serde_json Value.
    fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => JsonType::Null,
            Value::Bool(_) => JsonType::Boolean,
            Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    JsonType::Integer
                } else {
                    JsonType::Float
                }
            }
            Value::String(_) => JsonType::String,
            Value::Array(_) => JsonType::Array,
            Value::Object(_) => JsonType::Object,
        }
    }
}

/// Tracks observed types for each field during custom inference.
#[derive(Debug, Default)]
struct FieldTypeTracker {
    /// Maps field path (e.g., "user.address.city") to observed types.
    field_types: HashMap<String, HashSet<JsonType>>,
    /// Tracks nested object structures for fields observed as objects.
    nested_trackers: HashMap<String, FieldTypeTracker>,
    /// Tracks array element types for fields observed as arrays.
    array_element_types: HashMap<String, HashSet<JsonType>>,
    /// Tracks nested array element structures (for arrays of objects).
    array_nested_trackers: HashMap<String, FieldTypeTracker>,
}

impl FieldTypeTracker {
    /// Track all fields in a JSON object.
    fn track_object(&mut self, obj: &serde_json::Map<String, Value>) {
        for (key, value) in obj {
            let json_type = JsonType::from_value(value);
            self.field_types
                .entry(key.clone())
                .or_default()
                .insert(json_type);

            match value {
                Value::Object(nested) => {
                    self.nested_trackers
                        .entry(key.clone())
                        .or_default()
                        .track_object(nested);
                }
                Value::Array(arr) => {
                    self.track_array_elements(key, arr);
                }
                _ => {}
            }
        }
    }

    /// Track element types within an array field.
    fn track_array_elements(&mut self, key: &str, arr: &[Value]) {
        for elem in arr {
            let elem_type = JsonType::from_value(elem);
            self.array_element_types
                .entry(key.to_string())
                .or_default()
                .insert(elem_type);

            if let Value::Object(nested) = elem {
                self.array_nested_trackers
                    .entry(key.to_string())
                    .or_default()
                    .track_object(nested);
            }
        }
    }

    /// Build an Arrow schema from tracked types.
    /// Returns the schema and a list of field paths that had type conflicts.
    fn build_schema(&self) -> (Schema, Vec<String>) {
        let mut conflicts = Vec::new();
        let schema = self.build_schema_inner("", &mut conflicts);
        (schema, conflicts)
    }

    /// Build schema, collecting conflicts into the provided vec.
    fn build_schema_inner(&self, prefix: &str, conflicts: &mut Vec<String>) -> Schema {
        let mut fields = Vec::new();

        for (name, types) in &self.field_types {
            let full_path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}.{name}")
            };

            let data_type = self.infer_data_type(name, types, &full_path, conflicts);
            fields.push(Field::new(name, data_type, true));
        }

        // Sort fields for deterministic schema ordering
        fields.sort_by(|a, b| a.name().cmp(b.name()));
        Schema::new(fields)
    }

    /// Infer Arrow DataType from observed JSON types.
    fn infer_data_type(
        &self,
        field_name: &str,
        types: &HashSet<JsonType>,
        full_path: &str,
        conflicts: &mut Vec<String>,
    ) -> DataType {
        // Remove Null from consideration for type inference
        let non_null_types: HashSet<_> = types
            .iter()
            .filter(|t| **t != JsonType::Null)
            .copied()
            .collect();

        if non_null_types.is_empty() {
            return DataType::Utf8;
        }

        if non_null_types.len() == 1 {
            let single_type = non_null_types.iter().next().unwrap();
            return self.map_single_type(field_name, *single_type, full_path, conflicts);
        }

        // Check for numeric widening (Int + Float -> Float64)
        if non_null_types.len() == 2
            && non_null_types.contains(&JsonType::Integer)
            && non_null_types.contains(&JsonType::Float)
        {
            return DataType::Float64;
        }

        // Multiple incompatible types - use Utf8
        conflicts.push(full_path.to_string());
        DataType::Utf8
    }

    /// Map a single JsonType to Arrow DataType.
    fn map_single_type(
        &self,
        field_name: &str,
        json_type: JsonType,
        full_path: &str,
        conflicts: &mut Vec<String>,
    ) -> DataType {
        match json_type {
            JsonType::Null => DataType::Utf8,
            JsonType::Boolean => DataType::Boolean,
            JsonType::Integer => DataType::Int64,
            JsonType::Float => DataType::Float64,
            JsonType::String => DataType::Utf8,
            JsonType::Object => {
                if let Some(nested) = self.nested_trackers.get(field_name) {
                    let nested_schema = nested.build_schema_inner(full_path, conflicts);
                    DataType::Struct(nested_schema.fields)
                } else {
                    DataType::Utf8
                }
            }
            JsonType::Array => self.build_array_type(field_name, full_path, conflicts),
        }
    }

    /// Build Arrow List type from tracked array element types.
    fn build_array_type(
        &self,
        field_name: &str,
        full_path: &str,
        conflicts: &mut Vec<String>,
    ) -> DataType {
        let elem_types = self.array_element_types.get(field_name);
        let array_item_path = format!("{full_path}[]");

        let inner_type = match elem_types {
            None => DataType::Utf8,
            Some(types) => {
                let non_null_types: HashSet<_> = types
                    .iter()
                    .filter(|t| **t != JsonType::Null)
                    .copied()
                    .collect();

                if non_null_types.is_empty() {
                    DataType::Utf8
                } else if non_null_types.len() == 1 {
                    let single_type = non_null_types.iter().next().unwrap();
                    match single_type {
                        JsonType::Object => {
                            if let Some(nested) = self.array_nested_trackers.get(field_name) {
                                let nested_schema =
                                    nested.build_schema_inner(&array_item_path, conflicts);
                                DataType::Struct(nested_schema.fields)
                            } else {
                                DataType::Utf8
                            }
                        }
                        JsonType::Boolean => DataType::Boolean,
                        JsonType::Integer => DataType::Int64,
                        JsonType::Float => DataType::Float64,
                        JsonType::String => DataType::Utf8,
                        JsonType::Array => DataType::Utf8, // Nested arrays become strings
                        JsonType::Null => DataType::Utf8,
                    }
                } else if non_null_types.len() == 2
                    && non_null_types.contains(&JsonType::Integer)
                    && non_null_types.contains(&JsonType::Float)
                {
                    DataType::Float64
                } else {
                    // Array element type conflict
                    conflicts.push(array_item_path);
                    DataType::Utf8
                }
            }
        };

        DataType::List(Arc::new(Field::new("item", inner_type, true)))
    }
}

/// Perform custom schema inference with type conflict handling.
///
/// This is used as a fallback when Arrow's built-in inference fails due to
/// incompatible types in the same field across different records.
fn infer_with_conflict_handling(data: &[u8], pipeline: &str) -> Result<SchemaRef, InferenceError> {
    let mut tracker = FieldTypeTracker::default();
    let mut records_read = 0;

    for line in data.split(|&b| b == b'\n').take(SAMPLE_SIZE) {
        if line.is_empty() {
            continue;
        }

        let value: Value = serde_json::from_slice(line).map_err(|e| InferenceError::JsonParse {
            message: e.to_string(),
        })?;

        if let Value::Object(obj) = value {
            tracker.track_object(&obj);
            records_read += 1;
        }
    }

    if records_read == 0 {
        return Err(InferenceError::NoValidRecords);
    }

    let (schema, conflicts) = tracker.build_schema();

    if !conflicts.is_empty() {
        info!(
            target = %pipeline,
            "Resolved {} type conflicts as Utf8: {:?}",
            conflicts.len(),
            conflicts
        );
        emit!(SchemaTypeConflicts {
            count: conflicts.len(),
            target: pipeline.to_string(),
        });
    }

    debug!(
        "Custom inference built schema from {records_read} records with {} fields",
        schema.fields().len()
    );

    Ok(coerce_schema(&schema))
}

/// Infer schema from the first available NDJSON file in storage.
///
/// Tries up to 3 files in case some are corrupted or inaccessible.
/// Returns the schema inferred from the first file that can be successfully read.
///
/// If `coerce_conflicts_to_utf8` is true and Arrow's inference fails due to type conflicts,
/// falls back to custom inference that treats conflicting fields as `Utf8`.
pub async fn infer_schema_from_source(
    storage: &StorageProviderRef,
    compression: CompressionFormat,
    prefixes: Option<&[String]>,
    pipeline: &str,
    coerce_conflicts_to_utf8: bool,
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
            Ok(bytes) => {
                match infer_schema_from_bytes(
                    &bytes,
                    compression,
                    pipeline,
                    coerce_conflicts_to_utf8,
                ) {
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
                }
            }
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
///
/// If `coerce_conflicts_to_utf8` is true and Arrow's inference fails due to type conflicts
/// (e.g., the same field appearing as both an object and a string), falls back to
/// custom inference that treats conflicting fields as `Utf8`.
pub fn infer_schema_from_bytes(
    bytes: &Bytes,
    compression: CompressionFormat,
    pipeline: &str,
    coerce_conflicts_to_utf8: bool,
) -> Result<SchemaRef, InferenceError> {
    // Decompress into memory
    let decompressed = decompress(bytes, compression)?;

    // Try Arrow's built-in schema inference first
    let reader: Box<dyn BufRead> = Box::new(Cursor::new(&decompressed));
    match infer_json_schema(reader, Some(SAMPLE_SIZE)) {
        Ok((schema, records_read)) => {
            if records_read == 0 {
                return Err(InferenceError::NoValidRecords);
            }
            debug!("Arrow inferred schema from {records_read} records");
            Ok(coerce_schema(&schema))
        }
        Err(e) => {
            // Check if this is a type conflict error that we can handle
            if coerce_conflicts_to_utf8 && is_type_conflict_error(&e) {
                warn!(
                    target = %pipeline,
                    "Arrow inference hit type conflict, using fallback: {e}"
                );
                infer_with_conflict_handling(&decompressed, pipeline)
            } else {
                Err(InferenceError::JsonParse {
                    message: e.to_string(),
                })
            }
        }
    }
}

/// Check if an ArrowError is a type conflict that can be handled by fallback inference.
///
/// Arrow's `JsonError` variant is a plain `String` with no structured error codes,
/// so we must match on error message text. This is fragile but necessary given Arrow's
/// current API.
///
/// The following messages indicate recoverable type conflicts:
/// - "Incompatible type found during schema inference" - different types for the same field
/// - "Expected scalar or scalar array JSON type" - object where scalar expected
/// - "Expected object json type" - scalar where object expected
///
/// # Stability
///
/// If Arrow changes these messages, the `test_arrow_error_message_*` tests will fail.
/// Update the patterns here accordingly.
fn is_type_conflict_error(err: &ArrowError) -> bool {
    match err {
        ArrowError::JsonError(msg) => {
            msg.contains("Incompatible type found during schema inference")
                || msg.contains("Expected scalar or scalar array JSON type")
                || msg.contains("Expected object json type")
        }
        _ => false,
    }
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

    const TEST_PIPELINE: &str = "test";

    #[test]
    fn test_infer_basic_types() {
        let bytes = make_ndjson(&[
            r#"{"id": 1, "name": "Alice", "active": true, "score": 95.5}"#,
            r#"{"id": 2, "name": "Bob", "active": false, "score": 87.0}"#,
        ]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

        assert_eq!(schema.fields().len(), 4);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("active").is_ok());
        assert!(schema.field_with_name("score").is_ok());
    }

    #[test]
    fn test_infer_nested_object() {
        let bytes = make_ndjson(&[r#"{"id": 1, "meta": {"key": "value", "count": 42}}"#]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

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

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

        let tags_field = schema.field_with_name("tags").unwrap();
        assert!(matches!(tags_field.data_type(), DataType::List(_)));
    }

    #[test]
    fn test_infer_with_gzip_compression() {
        let bytes = make_gzip_ndjson(&[
            r#"{"id": 1, "name": "Alice"}"#,
            r#"{"id": 2, "name": "Bob"}"#,
        ]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::Gzip, TEST_PIPELINE, true).unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
    }

    #[test]
    fn test_infer_empty_file_error() {
        let bytes = Bytes::new();
        let result = infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true);
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

    // ========================================================================
    // Type conflict handling tests
    // ========================================================================

    #[test]
    fn test_conflict_object_vs_string() {
        // Same field is an object in one record and a string in another
        let bytes = make_ndjson(&[
            r#"{"data": {"nested": "value"}}"#,
            r#"{"data": "just a string"}"#,
        ]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

        let data_field = schema.field_with_name("data").unwrap();
        assert_eq!(
            data_field.data_type(),
            &DataType::Utf8,
            "Conflicting object/string field should become Utf8"
        );
    }

    #[test]
    fn test_conflict_number_vs_string() {
        // Same field is a number in one record and a string in another
        let bytes = make_ndjson(&[r#"{"value": 123}"#, r#"{"value": "not a number"}"#]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

        let value_field = schema.field_with_name("value").unwrap();
        assert_eq!(
            value_field.data_type(),
            &DataType::Utf8,
            "Conflicting number/string field should become Utf8"
        );
    }

    #[test]
    fn test_int_float_widening() {
        // Same field is an integer in one record and a float in another
        // This should widen to Float64, not be treated as a conflict
        let bytes = make_ndjson(&[r#"{"amount": 100}"#, r#"{"amount": 99.99}"#]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

        let amount_field = schema.field_with_name("amount").unwrap();
        assert_eq!(
            amount_field.data_type(),
            &DataType::Float64,
            "Int + Float should widen to Float64"
        );
    }

    #[test]
    fn test_array_with_scalar_handled_by_arrow() {
        // Arrow can handle array vs scalar by inferring List type
        // This test verifies Arrow's behavior is preserved
        let bytes = make_ndjson(&[r#"{"items": ["a", "b", "c"]}"#, r#"{"items": "single"}"#]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

        let items_field = schema.field_with_name("items").unwrap();
        // Arrow infers List type - this is Arrow's behavior
        assert!(
            matches!(items_field.data_type(), DataType::List(_)),
            "Arrow handles array/scalar by inferring List"
        );
    }

    #[test]
    fn test_array_vs_number_handled_by_arrow() {
        // Arrow also handles array vs number by inferring List
        let bytes = make_ndjson(&[r#"{"data": [1, 2, 3]}"#, r#"{"data": 42}"#]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

        let data_field = schema.field_with_name("data").unwrap();
        // Arrow handles this by inferring List type
        assert!(
            matches!(data_field.data_type(), DataType::List(_)),
            "Arrow handles array/number by inferring List"
        );
    }

    #[test]
    fn test_nested_fields_preserved_on_conflict() {
        // When a field conflicts, sibling and other nested fields should stay typed
        let bytes = make_ndjson(&[
            r#"{"id": 1, "meta": {"key": "val"}, "conflict": {"a": 1}}"#,
            r#"{"id": 2, "meta": {"key": "other"}, "conflict": "string"}"#,
        ]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

        // Non-conflicting fields should retain their types
        let id_field = schema.field_with_name("id").unwrap();
        assert_eq!(id_field.data_type(), &DataType::Int64);

        let meta_field = schema.field_with_name("meta").unwrap();
        assert!(
            matches!(meta_field.data_type(), DataType::Struct(_)),
            "Non-conflicting nested object should stay Struct"
        );

        // Conflicting field becomes Utf8
        let conflict_field = schema.field_with_name("conflict").unwrap();
        assert_eq!(conflict_field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_conflict_with_gzip_compression() {
        // Type conflicts should be handled correctly with compressed data
        let bytes = make_gzip_ndjson(&[
            r#"{"data": {"nested": true}}"#,
            r#"{"data": "string value"}"#,
        ]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::Gzip, TEST_PIPELINE, true).unwrap();

        let data_field = schema.field_with_name("data").unwrap();
        assert_eq!(
            data_field.data_type(),
            &DataType::Utf8,
            "Conflicting field in compressed data should become Utf8"
        );
    }

    #[test]
    fn test_no_conflict_normal_inference() {
        // When there are no conflicts, normal Arrow inference should work
        let bytes = make_ndjson(&[
            r#"{"id": 1, "name": "Alice", "active": true}"#,
            r#"{"id": 2, "name": "Bob", "active": false}"#,
            r#"{"id": 3, "name": "Charlie", "active": true}"#,
        ]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("active").unwrap().data_type(),
            &DataType::Boolean
        );
    }

    #[test]
    fn test_conflict_bool_vs_string() {
        // Boolean vs string conflict
        let bytes = make_ndjson(&[r#"{"flag": true}"#, r#"{"flag": "yes"}"#]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

        let flag_field = schema.field_with_name("flag").unwrap();
        assert_eq!(
            flag_field.data_type(),
            &DataType::Utf8,
            "Conflicting bool/string field should become Utf8"
        );
    }

    #[test]
    fn test_null_handling_in_conflict_resolution() {
        // Null values should not cause conflicts
        let bytes = make_ndjson(&[
            r#"{"value": null}"#,
            r#"{"value": 42}"#,
            r#"{"value": null}"#,
        ]);

        let schema =
            infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, true).unwrap();

        let value_field = schema.field_with_name("value").unwrap();
        assert_eq!(
            value_field.data_type(),
            &DataType::Int64,
            "Null + Int should resolve to Int64"
        );
    }

    #[test]
    fn test_coerce_conflicts_to_utf8_disabled_returns_error() {
        // When coerce_conflicts_to_utf8 is false, type conflicts should return an error
        let bytes = make_ndjson(&[
            r#"{"data": {"nested": "value"}}"#,
            r#"{"data": "just a string"}"#,
        ]);

        let result = infer_schema_from_bytes(&bytes, CompressionFormat::None, TEST_PIPELINE, false);

        assert!(
            result.is_err(),
            "Should return error when coerce_conflicts_to_utf8 is false"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Expected scalar"),
            "Error should mention type conflict: {err}"
        );
    }

    // ========================================================================
    // Arrow error message stability tests
    // ========================================================================
    //
    // These tests verify that Arrow's error messages haven't changed.
    // If these fail after an Arrow upgrade, update `is_type_conflict_error`.

    #[test]
    fn test_arrow_error_message_object_then_scalar() {
        // Object first, then scalar -> "Expected scalar or scalar array JSON type"
        let data = r#"{"field": {"nested": 1}}
{"field": "string"}"#;

        let reader = Cursor::new(data.as_bytes());
        let result = infer_json_schema(reader, None);

        let err = result.expect_err("Should fail with type conflict");
        let msg = err.to_string();

        assert!(
            msg.contains("Expected scalar or scalar array JSON type"),
            "Arrow error message changed! Update is_type_conflict_error(). Got: {msg}"
        );
    }

    #[test]
    fn test_arrow_error_message_scalar_then_object() {
        // Scalar first, then object -> "Expected object json type"
        let data = r#"{"field": 123}
{"field": {"unexpected": "object"}}"#;

        let reader = Cursor::new(data.as_bytes());
        let result = infer_json_schema(reader, None);

        let err = result.expect_err("Should fail with type conflict");
        let msg = err.to_string();

        assert!(
            msg.contains("Expected object json type"),
            "Arrow error message changed! Update is_type_conflict_error(). Got: {msg}"
        );
    }

    #[test]
    fn test_is_type_conflict_error_detection() {
        // Verify our detection function works for both error types
        let incompatible_err =
            ArrowError::JsonError("Incompatible type found during schema inference: X vs Y".into());
        let expected_scalar_err = ArrowError::JsonError(
            "Expected scalar or scalar array JSON type, found: Object".into(),
        );
        let expected_object_err =
            ArrowError::JsonError("Expected object json type, found: Scalar".into());
        let other_json_err = ArrowError::JsonError("Some other JSON error".into());
        let other_err = ArrowError::ParseError("Not a JSON error".into());

        assert!(
            is_type_conflict_error(&incompatible_err),
            "Should detect incompatible type error"
        );
        assert!(
            is_type_conflict_error(&expected_scalar_err),
            "Should detect expected scalar type error"
        );
        assert!(
            is_type_conflict_error(&expected_object_err),
            "Should detect expected object type error"
        );
        assert!(
            !is_type_conflict_error(&other_json_err),
            "Should not match other JSON errors"
        );
        assert!(
            !is_type_conflict_error(&other_err),
            "Should not match non-JSON errors"
        );
    }

    #[test]
    fn test_nested_conflict_reports_full_path() {
        // Verify that conflicts in nested fields report the full path
        let mut tracker = FieldTypeTracker::default();

        // Record 1: nested.value is a number
        let record1: Value =
            serde_json::from_str(r#"{"nested": {"value": 123, "stable": "ok"}}"#).unwrap();
        if let Value::Object(obj) = record1 {
            tracker.track_object(&obj);
        }

        // Record 2: nested.value is a string (conflict!)
        let record2: Value =
            serde_json::from_str(r#"{"nested": {"value": "string", "stable": "ok"}}"#).unwrap();
        if let Value::Object(obj) = record2 {
            tracker.track_object(&obj);
        }

        let (_schema, conflicts) = tracker.build_schema();

        assert!(
            conflicts.iter().any(|c| c == "nested.value"),
            "Should report nested conflict with full path. Got: {conflicts:?}"
        );
        assert!(
            !conflicts.iter().any(|c| c == "nested.stable"),
            "Should not report non-conflicting nested field"
        );
    }

    #[test]
    fn test_array_nested_conflict_reports_full_path() {
        // Verify that conflicts in array element fields report the full path
        let mut tracker = FieldTypeTracker::default();

        // Record 1: items[].count is a number
        let record1: Value =
            serde_json::from_str(r#"{"items": [{"count": 1}, {"count": 2}]}"#).unwrap();
        if let Value::Object(obj) = record1 {
            tracker.track_object(&obj);
        }

        // Record 2: items[].count is a string (conflict!)
        let record2: Value = serde_json::from_str(r#"{"items": [{"count": "many"}]}"#).unwrap();
        if let Value::Object(obj) = record2 {
            tracker.track_object(&obj);
        }

        let (_schema, conflicts) = tracker.build_schema();

        assert!(
            conflicts.iter().any(|c| c == "items[].count"),
            "Should report array nested conflict with full path. Got: {conflicts:?}"
        );
    }

    #[test]
    fn test_deeply_nested_conflict_reports_full_path() {
        // Verify conflicts 3+ levels deep report the full path
        let mut tracker = FieldTypeTracker::default();

        let record1: Value =
            serde_json::from_str(r#"{"a": {"b": {"c": {"deep_field": 123}}}}"#).unwrap();
        if let Value::Object(obj) = record1 {
            tracker.track_object(&obj);
        }

        let record2: Value =
            serde_json::from_str(r#"{"a": {"b": {"c": {"deep_field": "string"}}}}"#).unwrap();
        if let Value::Object(obj) = record2 {
            tracker.track_object(&obj);
        }

        let (_schema, conflicts) = tracker.build_schema();

        assert!(
            conflicts.iter().any(|c| c == "a.b.c.deep_field"),
            "Should report deeply nested conflict with full path. Got: {conflicts:?}"
        );
    }

    #[test]
    fn test_multiple_conflicts_at_different_levels() {
        let mut tracker = FieldTypeTracker::default();

        // Record with various fields
        let record1: Value =
            serde_json::from_str(r#"{"top": 1, "nested": {"mid": true, "deep": {"bottom": 100}}}"#)
                .unwrap();
        if let Value::Object(obj) = record1 {
            tracker.track_object(&obj);
        }

        // Conflicts at top, mid, and deep levels
        let record2: Value = serde_json::from_str(
            r#"{"top": "string", "nested": {"mid": "string", "deep": {"bottom": "string"}}}"#,
        )
        .unwrap();
        if let Value::Object(obj) = record2 {
            tracker.track_object(&obj);
        }

        let (_schema, conflicts) = tracker.build_schema();

        assert!(
            conflicts.iter().any(|c| c == "top"),
            "Should report top-level conflict. Got: {conflicts:?}"
        );
        assert!(
            conflicts.iter().any(|c| c == "nested.mid"),
            "Should report mid-level conflict. Got: {conflicts:?}"
        );
        assert!(
            conflicts.iter().any(|c| c == "nested.deep.bottom"),
            "Should report deep-level conflict. Got: {conflicts:?}"
        );
        assert_eq!(conflicts.len(), 3, "Should have exactly 3 conflicts");
    }

    #[test]
    fn test_array_element_type_conflict() {
        // Array with mixed primitive types (not nested objects)
        let mut tracker = FieldTypeTracker::default();

        let record1: Value = serde_json::from_str(r#"{"tags": [1, 2, 3]}"#).unwrap();
        if let Value::Object(obj) = record1 {
            tracker.track_object(&obj);
        }

        let record2: Value = serde_json::from_str(r#"{"tags": ["a", "b"]}"#).unwrap();
        if let Value::Object(obj) = record2 {
            tracker.track_object(&obj);
        }

        let (_schema, conflicts) = tracker.build_schema();

        assert!(
            conflicts.iter().any(|c| c == "tags[]"),
            "Should report array element type conflict. Got: {conflicts:?}"
        );
    }

    #[test]
    fn test_nested_array_in_nested_object() {
        // nested.items[].value has a conflict
        let mut tracker = FieldTypeTracker::default();

        let record1: Value =
            serde_json::from_str(r#"{"outer": {"items": [{"value": 1}, {"value": 2}]}}"#).unwrap();
        if let Value::Object(obj) = record1 {
            tracker.track_object(&obj);
        }

        let record2: Value =
            serde_json::from_str(r#"{"outer": {"items": [{"value": "text"}]}}"#).unwrap();
        if let Value::Object(obj) = record2 {
            tracker.track_object(&obj);
        }

        let (_schema, conflicts) = tracker.build_schema();

        assert!(
            conflicts.iter().any(|c| c == "outer.items[].value"),
            "Should report nested array object field conflict. Got: {conflicts:?}"
        );
    }

    #[test]
    fn test_no_false_positives_for_compatible_types() {
        // Int + Float should widen to Float64, not be a conflict
        let mut tracker = FieldTypeTracker::default();

        let record1: Value = serde_json::from_str(r#"{"num": 42, "nested": {"val": 1}}"#).unwrap();
        if let Value::Object(obj) = record1 {
            tracker.track_object(&obj);
        }

        let record2: Value =
            serde_json::from_str(r#"{"num": 3.14, "nested": {"val": 2.5}}"#).unwrap();
        if let Value::Object(obj) = record2 {
            tracker.track_object(&obj);
        }

        let (_schema, conflicts) = tracker.build_schema();

        assert!(
            conflicts.is_empty(),
            "Int+Float widening should not be reported as conflict. Got: {conflicts:?}"
        );
    }
}
