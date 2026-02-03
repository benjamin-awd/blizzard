//! Schema evolution manager.
//!
//! This module provides a centralized manager for schema evolution operations,
//! consolidating the logic for schema validation and evolution that was
//! previously scattered across multiple modules.

use deltalake::arrow::datatypes::{Schema, SchemaRef};
use tracing::info;

use crate::error::SchemaError;

use super::evolution::{EvolutionAction, SchemaEvolutionMode, validate_schema_evolution};

/// Manager for schema evolution operations.
///
/// This struct encapsulates schema validation and evolution logic,
/// providing a single point of control for schema changes.
pub struct SchemaManager {
    /// The current table schema (if any).
    table_schema: Option<SchemaRef>,
    /// The configured evolution mode.
    mode: SchemaEvolutionMode,
    /// Table identifier for logging.
    table_name: String,
}

impl SchemaManager {
    /// Create a new schema manager.
    pub fn new(
        table_schema: Option<SchemaRef>,
        mode: SchemaEvolutionMode,
        table_name: String,
    ) -> Self {
        Self {
            table_schema,
            mode,
            table_name,
        }
    }

    /// Get the current table schema.
    pub fn table_schema(&self) -> Option<&SchemaRef> {
        self.table_schema.as_ref()
    }

    /// Get the configured evolution mode.
    pub fn mode(&self) -> SchemaEvolutionMode {
        self.mode
    }

    /// Update the table schema after evolution.
    pub fn set_table_schema(&mut self, schema: SchemaRef) {
        self.table_schema = Some(schema);
    }

    /// Validate an incoming schema against the table schema.
    ///
    /// Returns the evolution action to take based on the configured mode.
    /// If no table schema exists, returns `EvolutionAction::None` to accept
    /// the incoming schema as the initial schema.
    pub fn validate(&self, incoming: &Schema) -> Result<EvolutionAction, SchemaError> {
        let table_schema = match &self.table_schema {
            Some(schema) => schema,
            None => {
                // No cached schema - accept incoming schema
                return Ok(EvolutionAction::None);
            }
        };

        validate_schema_evolution(table_schema, incoming, self.mode)
    }

    /// Validate and log the schema evolution action.
    ///
    /// This method validates the incoming schema and logs appropriate
    /// messages about what action will be taken.
    pub fn validate_and_log(&self, incoming: &Schema) -> Result<EvolutionAction, SchemaError> {
        let action = self.validate(incoming)?;

        match &action {
            EvolutionAction::None => {
                // Schema is compatible, no changes needed
            }
            EvolutionAction::Merge { new_schema } => {
                let existing_field_count = self.table_schema.as_ref().map_or(0, |s| s.fields().len());
                let new_field_names: Vec<_> = new_schema
                    .fields()
                    .iter()
                    .skip(existing_field_count)
                    .map(|f| f.name().as_str())
                    .collect();
                info!(
                    target = %self.table_name,
                    new_fields = new_field_names.len(),
                    field_names = ?new_field_names,
                    "Schema evolution: adding new fields"
                );
            }
            EvolutionAction::Overwrite { new_schema } => {
                info!(
                    target = %self.table_name,
                    fields = new_schema.fields().len(),
                    "Schema evolution: overwriting schema"
                );
            }
        }

        Ok(action)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    fn make_schema(fields: Vec<(&str, DataType, bool)>) -> Schema {
        Schema::new(
            fields
                .into_iter()
                .map(|(name, dtype, nullable)| Field::new(name, dtype, nullable))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn test_schema_manager_no_table_schema() {
        let manager = SchemaManager::new(None, SchemaEvolutionMode::Merge, "test".to_string());

        let incoming = make_schema(vec![("id", DataType::Int64, false)]);
        let action = manager.validate(&incoming).unwrap();

        assert!(matches!(action, EvolutionAction::None));
    }

    #[test]
    fn test_schema_manager_identical_schemas() {
        let schema = Arc::new(make_schema(vec![("id", DataType::Int64, false)]));
        let manager =
            SchemaManager::new(Some(schema.clone()), SchemaEvolutionMode::Merge, "test".to_string());

        let action = manager.validate(&schema).unwrap();

        assert!(matches!(action, EvolutionAction::None));
    }

    #[test]
    fn test_schema_manager_merge_new_field() {
        let table_schema = Arc::new(make_schema(vec![("id", DataType::Int64, false)]));
        let manager = SchemaManager::new(
            Some(table_schema),
            SchemaEvolutionMode::Merge,
            "test".to_string(),
        );

        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let action = manager.validate(&incoming).unwrap();

        assert!(matches!(action, EvolutionAction::Merge { .. }));
    }

    #[test]
    fn test_schema_manager_strict_rejects_new_field() {
        let table_schema = Arc::new(make_schema(vec![("id", DataType::Int64, false)]));
        let manager = SchemaManager::new(
            Some(table_schema),
            SchemaEvolutionMode::Strict,
            "test".to_string(),
        );

        let incoming = make_schema(vec![
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let result = manager.validate(&incoming);

        assert!(result.is_err());
    }

    #[test]
    fn test_schema_manager_overwrite_accepts_any() {
        let table_schema = Arc::new(make_schema(vec![("id", DataType::Int64, false)]));
        let manager = SchemaManager::new(
            Some(table_schema),
            SchemaEvolutionMode::Overwrite,
            "test".to_string(),
        );

        let incoming = make_schema(vec![("completely_different", DataType::Utf8, true)]);
        let action = manager.validate(&incoming).unwrap();

        assert!(matches!(action, EvolutionAction::Overwrite { .. }));
    }

    #[test]
    fn test_schema_manager_set_schema() {
        let mut manager =
            SchemaManager::new(None, SchemaEvolutionMode::Merge, "test".to_string());

        assert!(manager.table_schema().is_none());

        let schema = Arc::new(make_schema(vec![("id", DataType::Int64, false)]));
        manager.set_table_schema(schema.clone());

        assert!(manager.table_schema().is_some());
        assert_eq!(manager.table_schema().unwrap().fields().len(), 1);
    }
}
