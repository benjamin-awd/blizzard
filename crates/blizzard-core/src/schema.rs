//! Schema coercion utilities for Delta Lake compatibility.
//!
//! Delta Lake requires specific type constraints (e.g., microsecond timestamp
//! precision). This module provides shared coercion logic used by both blizzard
//! (NDJSON schema inference) and penguin (parquet schema inference).

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};

/// Result of coercing a field to Delta Lake compatible types.
///
/// This struct explicitly tracks whether coercion was needed, avoiding
/// the need for fragile pointer equality checks.
struct CoercedField {
    /// The field after coercion (may be the same as input if unchanged).
    field: FieldRef,
    /// Whether the field was actually modified during coercion.
    changed: bool,
}

impl CoercedField {
    /// Create a result indicating the field was unchanged.
    fn unchanged(field: FieldRef) -> Self {
        Self {
            field,
            changed: false,
        }
    }

    /// Create a result indicating the field was changed.
    fn changed(field: FieldRef) -> Self {
        Self {
            field,
            changed: true,
        }
    }
}

/// Coerce an Arrow field to be Delta Lake compatible.
///
/// Converts incompatible types into compatible ones. Specifically:
///
/// - Timestamp precision is coerced to microseconds (Delta Lake requirement)
/// - Nested types (List, LargeList, Struct) are recursively processed
pub fn coerce_field(field: FieldRef) -> FieldRef {
    coerce_field_inner(field).field
}

/// Inner implementation that tracks whether coercion occurred.
fn coerce_field_inner(field: FieldRef) -> CoercedField {
    match field.data_type() {
        // Coerce timestamp precision to microseconds
        DataType::Timestamp(TimeUnit::Nanosecond | TimeUnit::Millisecond, tz) => {
            CoercedField::changed(Arc::new(Field::new(
                field.name(),
                DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                field.is_nullable(),
            )))
        }
        // Recursively coerce List inner types
        DataType::List(inner_field) => {
            let coerced_inner = coerce_field_inner(inner_field.clone());
            if coerced_inner.changed {
                CoercedField::changed(Arc::new(Field::new(
                    field.name(),
                    DataType::List(coerced_inner.field),
                    field.is_nullable(),
                )))
            } else {
                CoercedField::unchanged(field)
            }
        }
        // Recursively coerce LargeList inner types
        DataType::LargeList(inner_field) => {
            let coerced_inner = coerce_field_inner(inner_field.clone());
            if coerced_inner.changed {
                CoercedField::changed(Arc::new(Field::new(
                    field.name(),
                    DataType::LargeList(coerced_inner.field),
                    field.is_nullable(),
                )))
            } else {
                CoercedField::unchanged(field)
            }
        }
        // Recursively coerce Struct field types
        DataType::Struct(fields) => {
            let coerced: Vec<CoercedField> = fields
                .iter()
                .map(|f| coerce_field_inner(f.clone()))
                .collect();

            let any_changed = coerced.iter().any(|c| c.changed);

            if any_changed {
                let coerced_fields: Vec<FieldRef> = coerced.into_iter().map(|c| c.field).collect();
                CoercedField::changed(Arc::new(Field::new(
                    field.name(),
                    DataType::Struct(coerced_fields.into()),
                    field.is_nullable(),
                )))
            } else {
                CoercedField::unchanged(field)
            }
        }
        // All other types pass through unchanged
        _ => CoercedField::unchanged(field),
    }
}

/// Coerce an entire schema to be Delta Lake compatible.
///
/// Applies [`coerce_field`] to all fields in the schema, converting
/// incompatible types (like nanosecond timestamps) to Delta-compatible types.
pub fn coerce_schema(schema: &Schema) -> SchemaRef {
    let coerced_fields: Vec<FieldRef> = schema
        .fields()
        .iter()
        .map(|f| coerce_field(f.clone()))
        .collect();

    Arc::new(Schema::new_with_metadata(
        coerced_fields,
        schema.metadata().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coerce_field_timestamp_nanosecond() {
        let field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ));

        let coerced = coerce_field(field);

        assert_eq!(
            coerced.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(coerced.name(), "ts");
        assert!(coerced.is_nullable());
    }

    #[test]
    fn test_coerce_field_timestamp_millisecond() {
        let field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ));

        let coerced = coerce_field(field);

        assert_eq!(
            coerced.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert!(!coerced.is_nullable());
    }

    #[test]
    fn test_coerce_field_timestamp_microsecond_unchanged() {
        let field = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));

        let coerced = coerce_field(field.clone());

        // Should return the same Arc (no change needed)
        assert!(Arc::ptr_eq(&field, &coerced));
    }

    #[test]
    fn test_coerce_field_non_timestamp_unchanged() {
        let field = Arc::new(Field::new("id", DataType::Int64, false));

        let coerced = coerce_field(field.clone());

        assert!(Arc::ptr_eq(&field, &coerced));
    }

    #[test]
    fn test_coerce_field_list_with_timestamp() {
        let field = Arc::new(Field::new(
            "timestamps",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ))),
            true,
        ));

        let coerced = coerce_field(field);

        match coerced.data_type() {
            DataType::List(inner) => {
                assert_eq!(
                    inner.data_type(),
                    &DataType::Timestamp(TimeUnit::Microsecond, None)
                );
            }
            other => panic!("Expected List type, got {other:?}"),
        }
    }

    #[test]
    fn test_coerce_field_large_list_with_timestamp() {
        let field = Arc::new(Field::new(
            "timestamps",
            DataType::LargeList(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ))),
            true,
        ));

        let coerced = coerce_field(field);

        match coerced.data_type() {
            DataType::LargeList(inner) => {
                assert_eq!(
                    inner.data_type(),
                    &DataType::Timestamp(TimeUnit::Microsecond, None)
                );
            }
            other => panic!("Expected LargeList type, got {other:?}"),
        }
    }

    #[test]
    fn test_coerce_field_struct_with_timestamp() {
        let field = Arc::new(Field::new(
            "meta",
            DataType::Struct(
                vec![
                    Arc::new(Field::new(
                        "created_at",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    )),
                    Arc::new(Field::new("id", DataType::Int32, true)),
                ]
                .into(),
            ),
            true,
        ));

        let coerced = coerce_field(field);

        match coerced.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(
                    fields[0].data_type(),
                    &DataType::Timestamp(TimeUnit::Microsecond, None)
                );
                assert_eq!(fields[1].data_type(), &DataType::Int32);
            }
            other => panic!("Expected Struct type, got {other:?}"),
        }
    }

    #[test]
    fn test_coerce_schema() {
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
            Field::new("id", DataType::Int64, false),
        ]);

        let coerced = coerce_schema(&schema);

        assert_eq!(coerced.fields().len(), 4);
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
        assert_eq!(coerced.field(3).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_coerce_schema_preserves_metadata() {
        use std::collections::HashMap;

        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), "value".to_string());

        let schema = Schema::new_with_metadata(
            vec![Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )],
            metadata.clone(),
        );

        let coerced = coerce_schema(&schema);

        assert_eq!(coerced.metadata(), &metadata);
    }
}
