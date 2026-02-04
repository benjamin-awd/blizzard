//! Delta Lake table management.
//!
//! This module provides functions for creating and loading Delta Lake tables.

use std::sync::Arc;

use deltalake::DeltaTable;
use deltalake::arrow::datatypes::{DataType, Field, Schema};
use deltalake::operations::create::CreateBuilder;
use object_store::path::Path;
use tracing::{debug, info};
use url::Url;

use blizzard_core::storage::{BackendConfig, StorageProvider};

use crate::error::DeltaError;

/// Ensure Delta Lake cloud storage handlers are registered.
///
/// This is idempotent - calling multiple times is safe.
pub fn ensure_handlers_registered() {
    deltalake::aws::register_handlers(None);
    deltalake::gcp::register_handlers(None);
}

/// Convert an Arrow schema to a Delta schema.
pub fn arrow_schema_to_delta(schema: &Schema) -> Result<deltalake::kernel::StructType, DeltaError> {
    use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
    use deltalake::kernel::{DataType as DeltaType, StructField, StructType};

    let fields: Vec<StructField> = schema
        .fields()
        .iter()
        .map(|field| {
            let delta_type: DeltaType = field
                .data_type()
                .try_into_kernel()
                .map_err(|source| DeltaError::SchemaConversion { source })?;
            Ok(StructField::new(
                field.name(),
                delta_type,
                field.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>, DeltaError>>()?;

    StructType::try_new(fields).map_err(|e| DeltaError::StructType {
        message: e.to_string(),
    })
}

/// Construct the Delta table URL from a storage provider.
pub fn build_table_url(storage_provider: &StorageProvider) -> Result<String, DeltaError> {
    let empty_path = Path::parse("").map_err(|_| DeltaError::PathParse {
        path: String::new(),
    })?;

    let table_url = match storage_provider.config() {
        BackendConfig::S3(s3) => {
            format!(
                "s3://{}/{}",
                s3.bucket,
                storage_provider.qualify_path(&empty_path)
            )
        }
        BackendConfig::Gcs(gcs) => {
            format!(
                "gs://{}/{}",
                gcs.bucket,
                storage_provider.qualify_path(&empty_path)
            )
        }
        BackendConfig::Azure(azure) => {
            format!(
                "abfs://{}/{}",
                azure.container,
                storage_provider.qualify_path(&empty_path)
            )
        }
        BackendConfig::Local(local) => {
            format!("file://{}", local.path)
        }
    };

    Ok(table_url)
}

/// Try to open an existing Delta Lake table.
///
/// Returns an error if the table doesn't exist. Use `DeltaError::is_table_not_found()`
/// to check if the error indicates a missing table.
pub async fn try_open_table(
    storage_provider: &StorageProvider,
    table_name: &str,
) -> Result<DeltaTable, DeltaError> {
    let table_url = build_table_url(storage_provider)?;

    let parsed_url = Url::parse(&table_url).map_err(|_| DeltaError::UrlParse {
        url: table_url.clone(),
    })?;

    let table = deltalake::open_table_with_storage_options(
        parsed_url,
        storage_provider.storage_options().clone(),
    )
    .await
    .map_err(|source| DeltaError::DeltaOperation { source })?;

    info!(
        target = %table_name,
        "Opened existing Delta table at version {}",
        table.version().unwrap_or(-1)
    );
    Ok(table)
}

/// Add partition columns to the schema if they don't already exist.
///
/// In Hive-style partitioned parquet files, partition columns are stored in
/// the directory path (e.g., `date=2024-01-28/file.parquet`) rather than in
/// the parquet file itself. When inferring the schema from the parquet file,
/// these partition columns are missing. This function adds them as String
/// fields so that Delta Lake table creation succeeds.
fn add_partition_columns_to_schema(schema: &Schema, partition_by: &[String]) -> Schema {
    let existing_fields: std::collections::HashSet<&str> =
        schema.fields().iter().map(|f| f.name().as_str()).collect();

    let mut fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();

    for partition_col in partition_by {
        if !existing_fields.contains(partition_col.as_str()) {
            debug!(
                "Adding partition column '{partition_col}' to schema (not present in parquet file)"
            );
            // Partition columns are always strings in Hive-style partitioning
            fields.push(Arc::new(Field::new(partition_col, DataType::Utf8, true)));
        }
    }

    Schema::new(fields)
}

/// Load or create a Delta Lake table with the given schema.
pub async fn load_or_create_table(
    storage_provider: &StorageProvider,
    schema: &Schema,
    partition_by: &[String],
    table_name: &str,
) -> Result<DeltaTable, DeltaError> {
    let table_url = build_table_url(storage_provider)?;

    // Try to open existing table
    let parsed_url = Url::parse(&table_url).map_err(|_| DeltaError::UrlParse {
        url: table_url.clone(),
    })?;
    match deltalake::open_table_with_storage_options(
        parsed_url.clone(),
        storage_provider.storage_options().clone(),
    )
    .await
    {
        Ok(table) => {
            info!(
                target = %table_name,
                "Loaded existing Delta table at version {}",
                table.version().unwrap_or(-1)
            );
            Ok(table)
        }
        Err(_) => {
            // Table doesn't exist, create it
            info!(target = %table_name, "Creating new Delta table at {table_url}");

            // Add partition columns to schema if they don't exist
            // (Hive-style partitioning stores them in paths, not in parquet files)
            let schema_with_partitions = add_partition_columns_to_schema(schema, partition_by);

            // Convert Arrow schema to Delta schema
            let delta_schema = arrow_schema_to_delta(&schema_with_partitions)?;

            let mut builder = CreateBuilder::new()
                .with_location(&table_url)
                .with_columns(delta_schema.fields().cloned())
                .with_storage_options(storage_provider.storage_options().clone());

            // Add partition columns if configured
            if !partition_by.is_empty() {
                info!(target = %table_name, "Creating table with partition columns: {:?}", partition_by);
                builder = builder.with_partition_columns(partition_by);
            }

            let table = builder
                .await
                .map_err(|source| DeltaError::DeltaOperation { source })?;

            Ok(table)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_partition_columns_to_schema_adds_missing_columns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let partition_by = vec!["date".to_string(), "hour".to_string()];
        let result = add_partition_columns_to_schema(&schema, &partition_by);

        assert_eq!(result.fields().len(), 4);
        assert_eq!(result.field(0).name(), "id");
        assert_eq!(result.field(1).name(), "name");
        assert_eq!(result.field(2).name(), "date");
        assert_eq!(result.field(2).data_type(), &DataType::Utf8);
        assert!(result.field(2).is_nullable());
        assert_eq!(result.field(3).name(), "hour");
        assert_eq!(result.field(3).data_type(), &DataType::Utf8);
        assert!(result.field(3).is_nullable());
    }

    #[test]
    fn test_add_partition_columns_to_schema_skips_existing_columns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("date", DataType::Date32, false), // Already exists with different type
        ]);

        let partition_by = vec!["date".to_string(), "hour".to_string()];
        let result = add_partition_columns_to_schema(&schema, &partition_by);

        // Only 3 fields - date already existed, hour is added
        assert_eq!(result.fields().len(), 3);
        assert_eq!(result.field(0).name(), "id");
        assert_eq!(result.field(1).name(), "date");
        assert_eq!(result.field(1).data_type(), &DataType::Date32); // Preserves original type
        assert_eq!(result.field(2).name(), "hour");
        assert_eq!(result.field(2).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_add_partition_columns_to_schema_no_partition_columns() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let partition_by: Vec<String> = vec![];
        let result = add_partition_columns_to_schema(&schema, &partition_by);

        assert_eq!(result.fields().len(), 2);
    }

    #[test]
    fn test_add_partition_columns_to_schema_empty_schema() {
        let schema = Schema::new(Vec::<Field>::new());

        let partition_by = vec!["date".to_string()];
        let result = add_partition_columns_to_schema(&schema, &partition_by);

        assert_eq!(result.fields().len(), 1);
        assert_eq!(result.field(0).name(), "date");
    }
}
