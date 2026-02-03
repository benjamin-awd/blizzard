//! Delta Lake table management.
//!
//! This module provides functions for creating and loading Delta Lake tables.

use deltalake::DeltaTable;
use deltalake::arrow::datatypes::Schema;
use deltalake::operations::create::CreateBuilder;
use object_store::path::Path;
use tracing::info;
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
            info!(target = %table_name, "Creating new Delta table at {}", table_url);

            // Convert Arrow schema to Delta schema
            let delta_schema = arrow_schema_to_delta(schema)?;

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
