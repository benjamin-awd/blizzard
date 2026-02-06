//! Schema evolution support for Delta Lake tables.
//!
//! This module provides utilities for comparing schemas and handling schema
//! evolution when incoming parquet files have different schemas than the
//! existing Delta table.
//!
//! Based on delta-rs patterns from `crates/core/src/operations/write/mod.rs`.

use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    /// Whether the schemas are compatible for merge mode.
    pub is_compatible: bool,
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
    let table_fields: HashMap<&str, &Field> = table
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    let incoming_fields: HashMap<&str, &Field> = incoming
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    let mut new_fields = Vec::new();
    let mut missing_fields = Vec::new();
    let mut type_changes = Vec::new();
    let mut is_compatible = true;

    // Find new fields (in incoming but not in table)
    for field in incoming.fields() {
        if let Some(table_field) = table_fields.get(field.name().as_str()) {
            // Field exists in both - check for type changes
            if table_field.data_type() != field.data_type() {
                // Check if this is an allowed type widening
                if !is_type_widening(table_field.data_type(), field.data_type()) {
                    type_changes.push((
                        field.name().clone(),
                        table_field.data_type().clone(),
                        field.data_type().clone(),
                    ));
                    is_compatible = false;
                }
            }
        } else {
            // New field
            new_fields.push(field.as_ref().clone());
            // New required fields make the schema incompatible for merge
            if !field.is_nullable() {
                is_compatible = false;
            }
        }
    }

    // Find missing fields (in table but not in incoming)
    for field in table.fields() {
        if !incoming_fields.contains_key(field.name().as_str()) {
            missing_fields.push(field.as_ref().clone());
            // Missing fields are OK - they'll be filled with NULL on read
        }
    }

    SchemaComparison {
        new_fields,
        missing_fields,
        type_changes,
        is_compatible,
    }
}

/// Check if a type change represents valid type widening or coercion.
///
/// Allowed widenings:
/// - Integer widening: Int8 -> Int16 -> Int32 -> Int64
/// - Unsigned integer widening: UInt8 -> UInt16 -> UInt32 -> UInt64
/// - Float widening: Float32 -> Float64
/// - Date widening: Date32 -> Date64
/// - Timestamp precision coercion: Nanosecond/Millisecond -> Microsecond
///   (Delta Lake requires microsecond precision for timestamps)
/// - Nested types (List, Struct): recursively checks inner types
fn is_type_widening(from: &DataType, to: &DataType) -> bool {
    // Handle timestamp precision coercion (Delta Lake requires microsecond precision)
    if let (DataType::Timestamp(from_unit, from_tz), DataType::Timestamp(to_unit, to_tz)) =
        (from, to)
    {
        // Timezone must match (or both be None)
        if from_tz != to_tz {
            return false;
        }
        // Allow coercion to microseconds from nanoseconds or milliseconds
        return matches!(
            (from_unit, to_unit),
            (TimeUnit::Nanosecond, TimeUnit::Microsecond)
                | (TimeUnit::Millisecond, TimeUnit::Microsecond)
        );
    }

    // Handle List types - recursively check inner field type
    if let (DataType::List(from_field), DataType::List(to_field)) = (from, to) {
        return is_type_widening(from_field.data_type(), to_field.data_type());
    }

    // Handle LargeList types
    if let (DataType::LargeList(from_field), DataType::LargeList(to_field)) = (from, to) {
        return is_type_widening(from_field.data_type(), to_field.data_type());
    }

    // Handle Struct types - all fields must be compatible
    if let (DataType::Struct(from_fields), DataType::Struct(to_fields)) = (from, to) {
        // Must have same number of fields for widening
        if from_fields.len() != to_fields.len() {
            return false;
        }
        // Check each field by position (names must match, types must be compatible)
        for (from_field, to_field) in from_fields.iter().zip(to_fields.iter()) {
            if from_field.name() != to_field.name() {
                return false;
            }
            // If types differ, check if it's a valid widening
            if from_field.data_type() != to_field.data_type()
                && !is_type_widening(from_field.data_type(), to_field.data_type())
            {
                return false;
            }
        }
        return true;
    }

    matches!(
        (from, to),
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

/// Merge a table schema with an incoming schema.
///
/// Returns a new schema that includes:
/// - All fields from the table schema
/// - New nullable fields from the incoming schema
///
/// # Errors
///
/// Returns an error if:
/// - The incoming schema has new required (non-nullable) fields
/// - There are incompatible type changes
pub fn merge_schemas(table: &Schema, incoming: &Schema) -> Result<SchemaRef, SchemaError> {
    let comparison = compare_schemas(table, incoming);

    // Check for type changes that aren't allowed
    if !comparison.type_changes.is_empty() {
        let (field, from, to) = &comparison.type_changes[0];
        return Err(SchemaError::TypeChangeNotAllowed {
            field: field.clone(),
            from: format!("{from:?}"),
            to: format!("{to:?}"),
        });
    }

    // Check for new required fields
    if let Some(field) = comparison.first_new_required_field() {
        return Err(SchemaError::RequiredFieldAddition {
            field_name: field.name().clone(),
        });
    }

    // Build merged schema: start with table fields, add new nullable fields
    let mut merged_fields: Vec<Arc<Field>> = table.fields().iter().cloned().collect();

    // Add new nullable fields from incoming schema
    for field in &comparison.new_fields {
        merged_fields.push(Arc::new(field.clone()));
    }

    Ok(Arc::new(Schema::new(merged_fields)))
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
            if !comparison.is_compatible {
                // Check for specific error cases
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
                let details = format_incompatibility(&comparison);
                return Err(SchemaError::IncompatibleSchema { details });
            }

            // Merge is possible
            if comparison.new_fields.is_empty() {
                // Only missing fields - no schema change needed
                Ok(EvolutionAction::None)
            } else {
                let new_schema = merge_schemas(table_schema, incoming_schema)?;
                Ok(EvolutionAction::Merge { new_schema })
            }
        }
        SchemaEvolutionMode::Overwrite => {
            // Overwrite mode always accepts the incoming schema
            Ok(EvolutionAction::Overwrite {
                new_schema: Arc::new(incoming_schema.clone()),
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
        assert!(comparison.is_compatible);
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
        assert!(comparison.is_compatible);
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
        assert!(!comparison.is_compatible); // incompatible due to required field
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
        assert!(comparison.is_compatible);
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_type_widening_float32_to_float64() {
        let table = make_schema(vec![("value", DataType::Float32, true)]);
        let incoming = make_schema(vec![("value", DataType::Float64, true)]);

        let comparison = compare_schemas(&table, &incoming);

        assert!(comparison.is_compatible);
        assert!(comparison.type_changes.is_empty());
    }

    #[test]
    fn test_compare_type_narrowing_rejected() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![("id", DataType::Int32, false)]); // narrowing

        let comparison = compare_schemas(&table, &incoming);

        assert!(!comparison.is_compatible);
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

        assert!(comparison.is_compatible);
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

        assert!(comparison.is_compatible);
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

        assert!(comparison.is_compatible);
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

        assert!(!comparison.is_compatible);
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

        assert!(!comparison.is_compatible);
        assert_eq!(comparison.type_changes.len(), 1);
    }

    #[test]
    fn test_compare_incompatible_type_change() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![("id", DataType::Utf8, false)]); // incompatible

        let comparison = compare_schemas(&table, &incoming);

        assert!(!comparison.is_compatible);
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
        assert!(comparison.is_compatible); // missing fields are OK
        assert!(comparison.new_fields.is_empty());
        assert_eq!(comparison.missing_fields.len(), 1);
        assert_eq!(comparison.missing_fields[0].name(), "name");
    }

    #[test]
    fn test_merge_schemas_adds_nullable_field() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            ("email", DataType::Utf8, true),
        ]);

        let merged = merge_schemas(&table, &incoming).unwrap();

        assert_eq!(merged.fields().len(), 2);
        assert_eq!(merged.field(0).name(), "id");
        assert_eq!(merged.field(1).name(), "email");
    }

    #[test]
    fn test_merge_schemas_rejects_required_field() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            ("required", DataType::Utf8, false), // not nullable
        ]);

        let result = merge_schemas(&table, &incoming);

        assert!(result.is_err());
        match result.unwrap_err() {
            SchemaError::RequiredFieldAddition { field_name } => {
                assert_eq!(field_name, "required");
            }
            e => panic!("Expected RequiredFieldAddition error, got: {e:?}"),
        }
    }

    #[test]
    fn test_merge_schemas_rejects_type_change() {
        let table = make_schema(vec![("id", DataType::Int64, false)]);
        let incoming = make_schema(vec![("id", DataType::Utf8, false)]);

        let result = merge_schemas(&table, &incoming);

        assert!(result.is_err());
        match result.unwrap_err() {
            SchemaError::TypeChangeNotAllowed { field, .. } => {
                assert_eq!(field, "id");
            }
            e => panic!("Expected TypeChangeNotAllowed error, got: {e:?}"),
        }
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

        assert!(comparison.is_compatible);
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

        assert!(comparison.is_compatible);
        assert!(comparison.type_changes.is_empty());
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

        assert!(!comparison.is_compatible);
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

        assert!(comparison.is_compatible);
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

        assert!(!comparison.is_compatible);
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

        assert!(comparison.is_compatible);
        assert!(comparison.type_changes.is_empty());
    }
}
