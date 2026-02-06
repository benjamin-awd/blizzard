//! NDJSON.gz async reader.
//!
//! Reads gzip-compressed newline-delimited JSON files and converts them
//! to Arrow RecordBatches using a user-provided schema.
//!
//! Supports two modes:
//! - **Standard mode**: Uses Arrow's optimized JSON reader directly
//! - **Coercing mode**: Pre-processes JSON to stringify objects in Utf8 fields,
//!   then uses Arrow's reader. This handles cases where the same field sometimes
//!   contains an object and sometimes a string.

use std::io::{BufRead, BufReader, Cursor};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::{DataType, SchemaRef};
use deltalake::arrow::json::ReaderBuilder;
use serde_json::Value;
use tracing::debug;

use crate::config::CompressionFormat;
use crate::error::{DecoderBuildSnafu, JsonDecodeSnafu, ReaderError};
use blizzard_core::emit;
use blizzard_core::metrics::events::{BytesRead, FileDecompressionCompleted};

use super::traits::FileReader;

/// Configuration for the NDJSON reader.
#[derive(Debug, Clone)]
pub struct NdjsonReaderConfig {
    /// Number of records per batch.
    pub batch_size: usize,
    /// Compression format of input files.
    pub compression: CompressionFormat,
    /// When true, uses serde_json parsing and coerces objects/arrays to JSON
    /// strings for fields typed as `Utf8` in the schema.
    ///
    /// This is slower than the standard Arrow reader but handles cases where
    /// the same field sometimes contains an object and sometimes a string.
    pub coerce_objects_to_strings: bool,
}

impl NdjsonReaderConfig {
    /// Create a new reader configuration.
    pub fn new(batch_size: usize, compression: CompressionFormat) -> Self {
        Self {
            batch_size,
            compression,
            coerce_objects_to_strings: false,
        }
    }

    /// Create a new reader configuration with object-to-string coercion enabled.
    pub fn with_coercion(batch_size: usize, compression: CompressionFormat) -> Self {
        Self {
            batch_size,
            compression,
            coerce_objects_to_strings: true,
        }
    }
}

/// A reader for NDJSON.gz files that yields Arrow RecordBatches.
pub struct NdjsonReader {
    schema: SchemaRef,
    config: NdjsonReaderConfig,
    /// Pipeline identifier for metrics labeling.
    pipeline: String,
}

impl NdjsonReader {
    /// Create a new NDJSON reader with the given schema and configuration.
    pub fn new(schema: SchemaRef, config: NdjsonReaderConfig, pipeline: String) -> Self {
        Self {
            schema,
            config,
            pipeline,
        }
    }

    /// Read compressed data and stream parsed batches via callback.
    ///
    /// Returns the total number of records read.
    fn read_internal_streaming(
        &self,
        compressed: &Bytes,
        path: &str,
        on_batch: &mut dyn FnMut(RecordBatch) -> ControlFlow<()>,
    ) -> Result<usize, ReaderError> {
        // Emit bytes read metric
        emit!(BytesRead {
            bytes: compressed.len() as u64,
            target: self.pipeline.clone(),
        });

        let start = Instant::now();
        let compressed_len = compressed.len();

        // Create a streaming reader using the compression codec.
        let codec = self.config.compression.codec();
        let reader =
            codec
                .create_reader(compressed)
                .map_err(|e| ReaderError::ZstdDecompression {
                    path: path.to_string(),
                    source: std::io::Error::other(e.message),
                })?;

        // Wrap in BufReader for efficient reading
        let buf_reader = BufReader::new(reader);

        let (batch_count, total_records) = if self.config.coerce_objects_to_strings {
            self.read_with_coercion(buf_reader, path, on_batch)?
        } else {
            self.read_with_arrow(buf_reader, path, on_batch)?
        };

        emit!(FileDecompressionCompleted {
            duration: start.elapsed(),
            target: self.pipeline.clone(),
        });

        debug!(
            "Streamed and parsed {} bytes -> {} batches ({} records) from {}",
            compressed_len, batch_count, total_records, path
        );

        Ok(total_records)
    }

    /// Read using Arrow's optimized JSON reader (standard mode).
    ///
    /// Invokes callback per batch. Returns (batch_count, total_records).
    fn read_with_arrow<R: BufRead>(
        &self,
        reader: R,
        path: &str,
        on_batch: &mut dyn FnMut(RecordBatch) -> ControlFlow<()>,
    ) -> Result<(usize, usize), ReaderError> {
        let json_reader = ReaderBuilder::new(Arc::clone(&self.schema))
            .with_batch_size(self.config.batch_size)
            .with_strict_mode(false)
            .build(reader)
            .map_err(|e| {
                DecoderBuildSnafu {
                    message: e.to_string(),
                }
                .build()
            })?;

        let mut batch_count = 0;
        let mut total_records = 0;

        for batch_result in json_reader {
            let batch = batch_result.map_err(|e| {
                JsonDecodeSnafu {
                    path: path.to_string(),
                    message: e.to_string(),
                }
                .build()
            })?;

            total_records += batch.num_rows();
            batch_count += 1;

            if on_batch(batch).is_break() {
                break;
            }
        }

        Ok((batch_count, total_records))
    }

    /// Read using serde_json preprocessing with object-to-string coercion.
    ///
    /// This mode:
    /// 1. Parses each JSON line with serde_json
    /// 2. Coerces objects/arrays to JSON strings for Utf8 fields
    /// 3. Re-serializes and passes to Arrow's reader
    ///
    /// This is slower than direct Arrow reading but handles type conflicts.
    /// Still collects lines for coercion preprocessing, then streams Arrow batches via callback.
    fn read_with_coercion<R: BufRead>(
        &self,
        reader: R,
        path: &str,
        on_batch: &mut dyn FnMut(RecordBatch) -> ControlFlow<()>,
    ) -> Result<(usize, usize), ReaderError> {
        let mut coerced_lines: Vec<String> = Vec::new();

        for (line_num, line_result) in reader.lines().enumerate() {
            let line = line_result.map_err(|e| ReaderError::ZstdDecompression {
                path: path.to_string(),
                source: e,
            })?;

            if line.is_empty() {
                continue;
            }

            let mut value: Value = serde_json::from_str(&line).map_err(|e| {
                JsonDecodeSnafu {
                    path: path.to_string(),
                    message: format!("line {}: {}", line_num + 1, e),
                }
                .build()
            })?;

            // Coerce objects/arrays to strings for Utf8 fields
            if let Value::Object(ref mut obj) = value {
                coerce_object_fields_to_strings(obj, &self.schema);
            }

            // Re-serialize the coerced value
            let coerced_line = serde_json::to_string(&value).map_err(|e| {
                JsonDecodeSnafu {
                    path: path.to_string(),
                    message: format!("line {}: failed to re-serialize: {}", line_num + 1, e),
                }
                .build()
            })?;

            coerced_lines.push(coerced_line);
        }

        // Join lines and pass to Arrow's reader, streaming batches via callback
        let coerced_ndjson = coerced_lines.join("\n");
        let cursor = Cursor::new(coerced_ndjson.as_bytes());

        self.read_with_arrow(cursor, path, on_batch)
    }
}

impl FileReader for NdjsonReader {
    fn read_batches(
        &self,
        data: Bytes,
        path: &str,
        on_batch: &mut dyn FnMut(RecordBatch) -> ControlFlow<()>,
    ) -> Result<usize, ReaderError> {
        self.read_internal_streaming(&data, path, on_batch)
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

// ============================================================================
// Object-to-string coercion helpers
// ============================================================================

/// Coerce object/array fields to JSON strings for Utf8 schema fields.
///
/// Walks through the JSON object and for any field that is typed as `Utf8`
/// in the schema but contains an object or array, serializes it to a JSON string.
fn coerce_object_fields_to_strings(obj: &mut serde_json::Map<String, Value>, schema: &SchemaRef) {
    for field in schema.fields() {
        if let Some(value) = obj.get_mut(field.name()) {
            coerce_value_for_field(value, field.data_type());
        }
    }
}

/// Coerce a single value based on the expected data type.
fn coerce_value_for_field(value: &mut Value, data_type: &DataType) {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => {
            // If the value is not already a string (or null), stringify it
            match value {
                Value::String(_) | Value::Null => {}
                Value::Number(n) => *value = Value::String(n.to_string()),
                Value::Bool(b) => *value = Value::String(b.to_string()),
                Value::Object(_) | Value::Array(_) => *value = Value::String(value.to_string()),
            }
        }
        DataType::Struct(fields) => {
            // Recurse into struct fields
            if let Value::Object(obj) = value {
                for field in fields.iter() {
                    if let Some(nested_value) = obj.get_mut(field.name()) {
                        coerce_value_for_field(nested_value, field.data_type());
                    }
                }
            }
        }
        DataType::List(field) | DataType::LargeList(field) => {
            // Recurse into list elements
            if let Value::Array(arr) = value {
                for elem in arr.iter_mut() {
                    coerce_value_for_field(elem, field.data_type());
                }
            }
        }
        _ => {
            // Other types don't need coercion
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::datatypes::{Field, Schema};
    use serde_json::json;

    fn make_schema(fields: Vec<(&str, DataType)>) -> SchemaRef {
        let arrow_fields: Vec<Field> = fields
            .into_iter()
            .map(|(name, dt)| Field::new(name, dt, true))
            .collect();
        Arc::new(Schema::new(arrow_fields))
    }

    /// Helper: collect all batches from read_batches into a Vec.
    fn collect_batches(
        reader: &NdjsonReader,
        bytes: Bytes,
        path: &str,
    ) -> Result<(Vec<RecordBatch>, usize), ReaderError> {
        let mut batches = Vec::new();
        let total = reader.read_batches(bytes, path, &mut |batch| {
            batches.push(batch);
            ControlFlow::Continue(())
        })?;
        Ok((batches, total))
    }

    #[test]
    fn test_coerce_object_to_string() {
        let schema = make_schema(vec![("data", DataType::Utf8)]);
        let mut obj = serde_json::Map::new();
        obj.insert("data".to_string(), json!({"nested": "value", "count": 42}));

        coerce_object_fields_to_strings(&mut obj, &schema);

        let result = obj.get("data").unwrap();
        assert!(result.is_string(), "Object should be coerced to string");
        let s = result.as_str().unwrap();
        assert!(
            s.contains("nested"),
            "Stringified object should contain field names"
        );
        assert!(
            s.contains("value"),
            "Stringified object should contain field values"
        );
    }

    #[test]
    fn test_coerce_array_to_string() {
        let schema = make_schema(vec![("tags", DataType::Utf8)]);
        let mut obj = serde_json::Map::new();
        obj.insert("tags".to_string(), json!(["a", "b", "c"]));

        coerce_object_fields_to_strings(&mut obj, &schema);

        let result = obj.get("tags").unwrap();
        assert!(result.is_string(), "Array should be coerced to string");
        let s = result.as_str().unwrap();
        assert_eq!(s, r#"["a","b","c"]"#);
    }

    #[test]
    fn test_coerce_preserves_strings() {
        let schema = make_schema(vec![("name", DataType::Utf8)]);
        let mut obj = serde_json::Map::new();
        obj.insert("name".to_string(), json!("Alice"));

        coerce_object_fields_to_strings(&mut obj, &schema);

        let result = obj.get("name").unwrap();
        assert_eq!(result.as_str().unwrap(), "Alice");
    }

    #[test]
    fn test_coerce_number_to_string() {
        let schema = make_schema(vec![("value", DataType::Utf8)]);
        let mut obj = serde_json::Map::new();
        obj.insert("value".to_string(), json!(0.00025));

        coerce_object_fields_to_strings(&mut obj, &schema);

        let result = obj.get("value").unwrap();
        assert!(result.is_string(), "Number should be coerced to string");
        assert_eq!(result.as_str().unwrap(), "0.00025");
    }

    #[test]
    fn test_coerce_bool_to_string() {
        let schema = make_schema(vec![("flag", DataType::Utf8)]);
        let mut obj = serde_json::Map::new();
        obj.insert("flag".to_string(), json!(true));

        coerce_object_fields_to_strings(&mut obj, &schema);

        let result = obj.get("flag").unwrap();
        assert!(result.is_string(), "Boolean should be coerced to string");
        assert_eq!(result.as_str().unwrap(), "true");
    }

    #[test]
    fn test_coerce_integer_to_string() {
        let schema = make_schema(vec![("count", DataType::Utf8)]);
        let mut obj = serde_json::Map::new();
        obj.insert("count".to_string(), json!(42));

        coerce_object_fields_to_strings(&mut obj, &schema);

        let result = obj.get("count").unwrap();
        assert!(result.is_string(), "Integer should be coerced to string");
        assert_eq!(result.as_str().unwrap(), "42");
    }

    #[test]
    fn test_coerce_preserves_non_utf8_types() {
        let schema = make_schema(vec![
            ("id", DataType::Int64),
            ("score", DataType::Float64),
            ("active", DataType::Boolean),
        ]);
        let mut obj = serde_json::Map::new();
        obj.insert("id".to_string(), json!(42));
        obj.insert("score".to_string(), json!(95.5));
        obj.insert("active".to_string(), json!(true));

        coerce_object_fields_to_strings(&mut obj, &schema);

        assert_eq!(obj.get("id").unwrap().as_i64().unwrap(), 42);
        assert_eq!(obj.get("score").unwrap().as_f64().unwrap(), 95.5);
        assert!(obj.get("active").unwrap().as_bool().unwrap());
    }

    #[test]
    fn test_coerce_nested_struct_field() {
        // Schema has a struct with a Utf8 field inside
        let struct_fields = deltalake::arrow::datatypes::Fields::from(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("metadata", DataType::Utf8, true), // This should be coerced
        ]);
        let schema = make_schema(vec![("user", DataType::Struct(struct_fields))]);

        let mut obj = serde_json::Map::new();
        obj.insert(
            "user".to_string(),
            json!({
                "name": "Alice",
                "metadata": {"role": "admin", "level": 5}  // Object in Utf8 field
            }),
        );

        coerce_object_fields_to_strings(&mut obj, &schema);

        let user = obj.get("user").unwrap().as_object().unwrap();
        assert_eq!(user.get("name").unwrap().as_str().unwrap(), "Alice");
        // metadata should be stringified
        let metadata = user.get("metadata").unwrap();
        assert!(
            metadata.is_string(),
            "Nested object should be coerced to string"
        );
    }

    #[test]
    fn test_coerce_list_elements() {
        // Schema has a list of Utf8
        let schema = make_schema(vec![(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        )]);

        let mut obj = serde_json::Map::new();
        obj.insert(
            "items".to_string(),
            json!([
                "string",
                {"nested": "object"},  // Object in Utf8 list element
                "another"
            ]),
        );

        coerce_object_fields_to_strings(&mut obj, &schema);

        let items = obj.get("items").unwrap().as_array().unwrap();
        assert_eq!(items[0].as_str().unwrap(), "string");
        assert!(items[1].is_string(), "Object in list should be coerced");
        assert!(items[1].as_str().unwrap().contains("nested"));
        assert_eq!(items[2].as_str().unwrap(), "another");
    }

    #[test]
    fn test_reader_with_coercion_basic() {
        use crate::config::CompressionFormat;

        let schema = make_schema(vec![("id", DataType::Int64), ("data", DataType::Utf8)]);

        let ndjson = r#"{"id": 1, "data": {"nested": "value"}}
{"id": 2, "data": "plain string"}"#;

        let config = NdjsonReaderConfig::with_coercion(1000, CompressionFormat::None);
        let reader = NdjsonReader::new(schema, config, "test".to_string());

        let bytes = Bytes::from(ndjson);
        let (batches, total_records) = collect_batches(&reader, bytes, "test.ndjson").unwrap();

        assert_eq!(total_records, 2);
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        // Check that the data column contains strings
        let data_col = batch
            .column_by_name("data")
            .unwrap()
            .as_any()
            .downcast_ref::<deltalake::arrow::array::StringArray>()
            .unwrap();

        // First row: object was stringified
        let first = data_col.value(0);
        assert!(
            first.contains("nested"),
            "Object should be stringified: {first}"
        );

        // Second row: string preserved
        assert_eq!(data_col.value(1), "plain string");
    }

    #[test]
    fn test_reader_without_coercion_fails_on_type_mismatch() {
        use crate::config::CompressionFormat;

        let schema = make_schema(vec![("id", DataType::Int64), ("data", DataType::Utf8)]);

        // First record has object, second has string - Arrow reader will fail
        let ndjson = r#"{"id": 1, "data": {"nested": "value"}}
{"id": 2, "data": "plain string"}"#;

        let config = NdjsonReaderConfig::new(1000, CompressionFormat::None);
        let reader = NdjsonReader::new(schema, config, "test".to_string());

        let bytes = Bytes::from(ndjson);
        let result = collect_batches(&reader, bytes, "test.ndjson");

        // Should fail because Arrow can't read object into Utf8
        assert!(result.is_err(), "Should fail without coercion");
    }
}
