//! Schema evolution support for Delta Lake tables.
//!
//! This module provides utilities for comparing schemas and handling schema
//! evolution when incoming parquet files have different schemas than the
//! existing Delta table.
//!
//! Based on delta-rs patterns from `crates/core/src/operations/write/mod.rs`.

use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub use blizzard_core::schema::{coerce_field, coerce_schema};

use crate::error::SchemaError;

/// Schema evolution mode determining how schema changes are handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaEvolutionMode {
    /// Reject any schema changes.
    Strict,
    /// Allow adding new nullable columns (default).
    #[default]
    Merge,
    /// Replace schema entirely (requires explicit opt-in).
    Overwrite,
}

/// Result of comparing two schemas.
#[derive(Debug, Clone)]
pub struct SchemaComparison {
    /// Fields present in incoming schema but not in table schema.
    pub new_fields: Vec<Field>,
    /// Fields present in table schema but not in incoming schema.
    pub missing_fields: Vec<Field>,
    /// Type changes: (field_name, table_type, incoming_type).
    pub type_changes: Vec<(String, DataType, DataType)>,
}

impl SchemaComparison {
    /// Check if the schemas are identical.
    pub fn is_identical(&self) -> bool {
        self.new_fields.is_empty() && self.missing_fields.is_empty() && self.type_changes.is_empty()
    }

    /// Check if there are any new required (non-nullable) fields.
    pub fn has_new_required_fields(&self) -> bool {
        self.first_new_required_field().is_some()
    }

    /// Returns the first new required (non-nullable) field, if any.
    pub fn first_new_required_field(&self) -> Option<&Field> {
        self.new_fields.iter().find(|f| !f.is_nullable())
    }

    /// Whether the schemas are compatible for merge mode.
    pub fn is_compatible(&self) -> bool {
        self.type_changes.is_empty() && !self.has_new_required_fields()
    }
}

/// Compare a table schema against an incoming schema.
///
/// Identifies:
/// - New fields (in incoming but not in table)
/// - Missing fields (in table but not in incoming)
/// - Type changes (same name, different type)
///
/// The comparison considers type widening as compatible:
/// - Int8 -> Int16 -> Int32 -> Int64
/// - Float32 -> Float64
/// - Date32 -> Date64
/// - Timestamp precision coercion to Microsecond (Delta Lake requirement)
pub fn compare_schemas(table: &Schema, incoming: &Schema) -> SchemaComparison {
    let mut new_fields = Vec::new();
    let mut missing_fields = Vec::new();
    let mut type_changes = Vec::new();

    // Find new fields and type changes (incoming vs table)
    for field in incoming.fields() {
        if let Some((_, table_field)) = table.column_with_name(field.name()) {
            if !are_types_compatible(table_field.data_type(), field.data_type()) {
                type_changes.push((
                    field.name().clone(),
                    table_field.data_type().clone(),
                    field.data_type().clone(),
                ));
            }
        } else {
            new_fields.push(field.as_ref().clone());
        }
    }

    // Find missing fields (in table but not in incoming)
    for field in table.fields() {
        if incoming.column_with_name(field.name()).is_none() {
            missing_fields.push(field.as_ref().clone());
        }
    }

    SchemaComparison {
        new_fields,
        missing_fields,
        type_changes,
    }
}

/// Check if two data types are compatible for schema evolution.
///
/// Returns true if types are structurally equivalent or represent valid widening/coercion:
/// - Struct: name-based field matching with recursive compatibility
/// - Timestamp: coercion to microsecond precision (same timezone, Delta Lake requirement)
/// - List/LargeList: recursive inner type check (ignores field names like "element" vs "item")
/// - Scalar widening: Int8→Int16→Int32→Int64, UInt8→…→UInt64, Float32→Float64, Date32→Date64
/// - All other types: structural equality via [`DataType::equals_datatype`]
fn are_types_compatible(a: &DataType, b: &DataType) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        // Struct fields are matched by name (not position) since JSON field
        // ordering is not guaranteed and different writers may emit fields in
        // different orders. Recurse into each field's data type.
        (DataType::Struct(a_fields), DataType::Struct(b_fields)) => {
            a_fields.len() == b_fields.len()
                && a_fields.iter().all(|a_field| {
                    b_fields.find(a_field.name()).is_some_and(|(_, b_field)| {
                        are_types_compatible(a_field.data_type(), b_field.data_type())
                    })
                })
        }
        // Timestamp: allow coercion to microsecond precision (Delta Lake requirement).
        (DataType::Timestamp(from_unit, from_tz), DataType::Timestamp(to_unit, to_tz)) => {
            from_tz == to_tz
                && matches!(
                    (from_unit, to_unit),
                    (TimeUnit::Nanosecond, TimeUnit::Microsecond)
                        | (TimeUnit::Millisecond, TimeUnit::Microsecond)
                )
        }
        // List/LargeList: recursive inner type check.
        // Ignores inner field names ("element" vs "item") since Arrow and Parquet differ.
        (DataType::List(a_field), DataType::List(b_field))
        | (DataType::LargeList(a_field), DataType::LargeList(b_field)) => {
            are_types_compatible(a_field.data_type(), b_field.data_type())
        }
        // Scalar widening or structural equality for everything else.
        _ => {
            a.equals_datatype(b)
                || matches!(
                    (a, b),
                    // Integer widening
                    (DataType::Int8, DataType::Int16 | DataType::Int32 | DataType::Int64)
                        | (DataType::Int16, DataType::Int32 | DataType::Int64)
                        | (DataType::Int32, DataType::Int64)
                        // Unsigned integer widening
                        | (
                            DataType::UInt8,
                            DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                        )
                        | (DataType::UInt16, DataType::UInt32 | DataType::UInt64)
                        | (DataType::UInt32, DataType::UInt64)
                        // Float widening
                        | (DataType::Float32, DataType::Float64)
                        // Date widening
                        | (DataType::Date32, DataType::Date64)
                )
        }
    }
}

/// Describes an evolution action to be taken on the table schema.
#[derive(Debug, Clone)]
pub enum EvolutionAction {
    /// No change needed - schemas are compatible.
    None,
    /// Merge new fields into the existing schema.
    Merge { new_schema: SchemaRef },
    /// Overwrite the schema entirely (dangerous).
    Overwrite { new_schema: SchemaRef },
}

/// Validate an incoming schema against a table schema based on the evolution mode.
///
/// Returns the appropriate evolution action or an error if the schema is incompatible.
pub fn validate_schema_evolution(
    table_schema: &Schema,
    incoming_schema: &Schema,
    mode: SchemaEvolutionMode,
) -> Result<EvolutionAction, SchemaError> {
    let comparison = compare_schemas(table_schema, incoming_schema);

    // If schemas are identical, no action needed
    if comparison.is_identical() {
        return Ok(EvolutionAction::None);
    }

    match mode {
        SchemaEvolutionMode::Strict => {
            // Any difference is an error in strict mode
            let details = format_incompatibility(&comparison);
            Err(SchemaError::IncompatibleSchema { details })
        }
        SchemaEvolutionMode::Merge => {
            if !comparison.type_changes.is_empty() {
                let (field, from, to) = &comparison.type_changes[0];
                return Err(SchemaError::TypeChangeNotAllowed {
                    field: field.clone(),
                    from: format!("{from:?}"),
                    to: format!("{to:?}"),
                });
            }
            if let Some(field) = comparison.first_new_required_field() {
                return Err(SchemaError::RequiredFieldAddition {
                    field_name: field.name().clone(),
                });
            }

            if comparison.new_fields.is_empty() {
                Ok(EvolutionAction::None)
            } else {
                let fields: Vec<Arc<Field>> = table_schema
                    .fields()
                    .iter()
                    .cloned()
                    .chain(comparison.new_fields.iter().map(|f| Arc::new(f.clone())))
                    .collect();
                Ok(EvolutionAction::Merge {
                    new_schema: coerce_schema(&Schema::new(fields)),
                })
            }
        }
        SchemaEvolutionMode::Overwrite => {
            // Overwrite mode always accepts the incoming schema
            Ok(EvolutionAction::Overwrite {
                new_schema: coerce_schema(incoming_schema),
            })
        }
    }
}

/// Format a schema comparison as a human-readable incompatibility message.
fn format_incompatibility(comparison: &SchemaComparison) -> String {
    let mut parts = Vec::new();

    if !comparison.new_fields.is_empty() {
        let names: Vec<_> = comparison.new_fields.iter().map(|f| f.name()).collect();
        let required: Vec<_> = comparison
            .new_fields
            .iter()
            .filter(|f| !f.is_nullable())
            .map(|f| f.name())
            .collect();
        if !required.is_empty() {
            parts.push(format!("new required fields: {required:?}"));
        } else {
            parts.push(format!("new fields: {names:?}"));
        }
    }

    if !comparison.missing_fields.is_empty() {
        let names: Vec<_> = comparison.missing_fields.iter().map(|f| f.name()).collect();
        parts.push(format!("missing fields: {names:?}"));
    }

    if !comparison.type_changes.is_empty() {
        let changes: Vec<_> = comparison
            .type_changes
            .iter()
            .map(|(name, from, to)| format!("{name}: {from:?} -> {to:?}"))
            .collect();
        parts.push(format!("type changes: {}", changes.join(", ")));
    }

    parts.join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    fn make_schema(fields: Vec<(&str, DataType, bool)>) -> Schema {
        Schema::new(
            fields
                .into_iter()
                .map(|(name, dtype, nullable)| Field::new(name, dtype, nullable))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn test_compare_identical_schemas() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);

        let comparison = compare_schemas(&schema, &schema);

        assert!(comparison.is_identical());
        assert!(comparison.is_compatible());
        assert!(comparison.new_fields.is_empty());
        assert!(comparison.missing_fields.is_empty());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_new_nullable_field() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            ("email", DataType::Utf8, true), // new nullable field
        ]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(!comparison.is_identical());
        assert!(comparison.is_compatible());
        assert_eq!(comparison.new_fields.len(), 1);
        assert_eq!(comparison.new_fields[0].name(), "email");
        assert!(comparison.missing_fields.is_empty());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_new_required_field_rejected() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            ("required_field", DataType::Utf8, false), // new required field
        ]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(!comparison.is_identical());
        assert!(!comparison.is_compatible()); // incompatible due to required field
        assert!(comparison.has_new_required_fields());
        assert_eq!(comparison.new_fields.len(), 1);
        assert_eq!(comparison.new_fields[0].name(), "required_field");
    }

    #[test]
    fn test_compare_type_widening_int32_to_int64() {
        let table = make_schema(vec![("id", DataType::Int32, false)]);
        let incoming = make_schema(vec![("id", DataType::Int64, false)]);

        let comparison = compare_schemas(&table, &incoming);

        // Type widening is allowed
        assert!(comparison.is_compatible());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_type_widening_float32_to_float64() {
        let table = make_schema(vec![("value", DataType::Float32, true)]);
        let incoming = make_schema(vec![("value", DataType::Float64, true)]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(comparison.is_compatible());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_type_narrowing_rejected() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![("id", DataType::Int32, false)]); // narrowing

        let comparison = compare_schemas(&table, &incoming);

        assert!(!comparison.is_compatible());
        assert_eq!(comparison.type_changes.len(), 1);
        assert_eq!(comparison.type_changes[0].0, "id");
        assert_eq!(comparison.type_changes[0].1, DataType::Int64);
        assert_eq!(comparison.type_changes[0].2, DataType::Int32);
    }

    #[test]
    fn test_compare_timestamp_nanosecond_to_microsecond() {
        let table = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]);
        let incoming = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(comparison.is_compatible());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_timestamp_millisecond_to_microsecond() {
        let table = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]);
        let incoming = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(comparison.is_compatible());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_timestamp_with_timezone_coercion() {
        let tz = Some("UTC".into());
        let table = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
            true,
        )]);
        let incoming = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, tz),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(comparison.is_compatible());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_timestamp_timezone_mismatch_rejected() {
        let table = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        )]);
        let incoming = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("America/New_York".into())),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(!comparison.is_compatible());
        assert_eq!(comparison.type_changes.len(), 1);
    }

    #[test]
    fn test_compare_timestamp_microsecond_to_nanosecond_rejected() {
        // Narrowing from microsecond to nanosecond is not allowed
        let table = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        let incoming = make_schema(vec![(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(!comparison.is_compatible());
        assert_eq!(comparison.type_changes.len(), 1);
    }

    #[test]
    fn test_compare_incompatible_type_change() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![("id", DataType::Utf8, false)]); // incompatible

        let comparison = compare_schemas(&table, &incoming);

        assert!(!comparison.is_compatible());
        assert_eq!(comparison.type_changes.len(), 1);
    }

    #[test]
    fn test_compare_missing_fields() {
        let table = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            // missing "name" field
        ]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(!comparison.is_identical());
        assert!(comparison.is_compatible()); // missing fields are OK
        assert!(comparison.new_fields.is_empty());
        assert_eq!(comparison.missing_fields.len(), 1);
        assert_eq!(comparison.missing_fields[0].name(), "name");
    }

    #[test]
    fn test_validate_strict_mode_rejects_new_field() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            ("email", DataType::Utf8, true),
        ]);

        let result = validate_schema_evolution(&table, &incoming, SchemaEvolutionMode::Strict);

        assert!(result.is_err());
        match result.unwrap_err() {
            SchemaError::IncompatibleSchema { .. } => {}
            e => panic!("Expected IncompatibleSchema error, got: {e:?}"),
        }
    }

    #[test]
    fn test_validate_merge_mode_allows_new_nullable_field() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            ("email", DataType::Utf8, true),
        ]);

        let result = validate_schema_evolution(&table, &incoming, SchemaEvolutionMode::Merge);

        assert!(result.is_ok());
        match result.unwrap() {
            EvolutionAction::Merge { new_schema } => {
                assert_eq!(new_schema.fields().len(), 2);
            }
            action => panic!("Expected Merge action, got: {action:?}"),
        }
    }

    #[test]
    fn test_validate_merge_mode_allows_missing_fields() {
        let table = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            // missing "name" field - allowed, filled with NULL on read
        ]);

        let result = validate_schema_evolution(&table, &incoming, SchemaEvolutionMode::Merge);

        assert!(result.is_ok());
        match result.unwrap() {
            EvolutionAction::None => {} // No schema change needed
            action => panic!("Expected None action, got: {action:?}"),
        }
    }

    #[test]
    fn test_validate_overwrite_mode_accepts_any_schema() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![
            ("completely", DataType::Utf8, false),
            ("different", DataType::Float64, true),
        ]);

        let result = validate_schema_evolution(&table, &incoming, SchemaEvolutionMode::Overwrite);

        assert!(result.is_ok());
        match result.unwrap() {
            EvolutionAction::Overwrite { new_schema } => {
                assert_eq!(new_schema.fields().len(), 2);
                assert_eq!(new_schema.field(0).name(), "completely");
            }
            action => panic!("Expected Overwrite action, got: {action:?}"),
        }
    }

    #[test]
    fn test_validate_identical_schemas_returns_none() {
        let schema = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);

        let result = validate_schema_evolution(&schema, &schema, SchemaEvolutionMode::Merge);

        assert!(result.is_ok());
        match result.unwrap() {
            EvolutionAction::None => {}
            action => panic!("Expected None action, got: {action:?}"),
        }
    }

    #[test]
    fn test_compare_list_with_timestamp_coercion() {
        use std::sync::Arc;

        let table = make_schema(vec![(
            "timestamps",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ))),
            true,
        )]);
        let incoming = make_schema(vec![(
            "timestamps",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ))),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(comparison.is_compatible());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_list_with_integer_widening() {
        use std::sync::Arc;

        let table = make_schema(vec![(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]);
        let incoming = make_schema(vec![(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(comparison.is_compatible());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_list_element_field_name_mismatch_is_equivalent() {
        use std::sync::Arc;

        // Delta/Arrow uses "element", Parquet uses "item" — these are semantically identical
        let table = make_schema(vec![(
            "tags",
            DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
            true,
        )]);
        let incoming = make_schema(vec![(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(comparison.is_compatible());
        assert!(comparison.type_changes.is_empty());
        assert!(comparison.is_identical());
    }

    #[test]
    fn test_compare_list_incompatible_type_rejected() {
        use std::sync::Arc;

        let table = make_schema(vec![(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]);
        let incoming = make_schema(vec![(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(!comparison.is_compatible());
        assert_eq!(comparison.type_changes.len(), 1);
    }

    #[test]
    fn test_compare_struct_with_timestamp_coercion() {
        use std::sync::Arc;

        let table = make_schema(vec![(
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
        )]);
        let incoming = make_schema(vec![(
            "meta",
            DataType::Struct(
                vec![
                    Arc::new(Field::new(
                        "created_at",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    )),
                    Arc::new(Field::new("id", DataType::Int32, true)),
                ]
                .into(),
            ),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(comparison.is_compatible());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_struct_field_name_mismatch_rejected() {
        use std::sync::Arc;

        let table = make_schema(vec![(
            "meta",
            DataType::Struct(vec![Arc::new(Field::new("id", DataType::Int32, true))].into()),
            true,
        )]);
        let incoming = make_schema(vec![(
            "meta",
            DataType::Struct(vec![Arc::new(Field::new("user_id", DataType::Int32, true))].into()),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(!comparison.is_compatible());
        assert_eq!(comparison.type_changes.len(), 1);
    }

    #[test]
    fn test_compare_nested_list_in_struct_with_coercion() {
        use std::sync::Arc;

        let table = make_schema(vec![(
            "data",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "timestamps",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    ))),
                    true,
                ))]
                .into(),
            ),
            true,
        )]);
        let incoming = make_schema(vec![(
            "data",
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "timestamps",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ))),
                    true,
                ))]
                .into(),
            ),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(comparison.is_compatible());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_struct_field_reordering_is_equivalent() {
        use std::sync::Arc;

        // JSON field ordering is not guaranteed — same fields in different order
        // should be treated as equivalent.
        let table = make_schema(vec![(
            "fills",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("createdAt", DataType::Utf8, true)),
                        Arc::new(Field::new("executionID", DataType::Utf8, true)),
                        Arc::new(Field::new("price", DataType::Float64, true)),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        )]);
        let incoming = make_schema(vec![(
            "fills",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("createdAt", DataType::Utf8, true)),
                        Arc::new(Field::new("price", DataType::Float64, true)),
                        Arc::new(Field::new("executionID", DataType::Utf8, true)),
                    ]
                    .into(),
                ),
                true,
            ))),
            true,
        )]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(comparison.is_identical());
        assert!(comparison.is_compatible());
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_validate_merge_coerces_null_fields() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            ("maybe_null", DataType::Null, true),
        ]);

        let result = validate_schema_evolution(&table, &incoming, SchemaEvolutionMode::Merge);

        assert!(result.is_ok());
        match result.unwrap() {
            EvolutionAction::Merge { new_schema } => {
                assert_eq!(new_schema.fields().len(), 2);
                assert_eq!(new_schema.field(1).data_type(), &DataType::Utf8);
            }
            action => panic!("Expected Merge action, got: {action:?}"),
        }
    }

    #[test]
    fn test_validate_overwrite_coerces_null_fields() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            ("maybe_null", DataType::Null, true),
        ]);

        let result = validate_schema_evolution(&table, &incoming, SchemaEvolutionMode::Overwrite);

        assert!(result.is_ok());
        match result.unwrap() {
            EvolutionAction::Overwrite { new_schema } => {
                assert_eq!(new_schema.fields().len(), 2);
                assert_eq!(new_schema.field(1).data_type(), &DataType::Utf8);
            }
            action => panic!("Expected Overwrite action, got: {action:?}"),
        }
    }
}
