//! Blizzard: File loader for streaming NDJSON.gz files to Parquet.
//!
//! This crate handles:
//! - Reading compressed NDJSON files from cloud storage (S3, GCS, Azure, local)
//! - Parsing and validating records against a schema
//! - Writing Parquet files with rolling policies
//! - Writing finished files directly to Delta table directories
//! - Dead letter queue for failed records

pub mod checkpoint;
pub mod config;
pub mod dlq;
pub mod error;
pub mod parquet;
pub mod pipeline;
pub mod source;

/// Re-export storage module from blizzard-core for convenience
pub mod storage {
    pub use blizzard_core::storage::*;
}

/// Re-export watermark module from blizzard-core for convenience
pub mod watermark {
    pub use blizzard_core::watermark::*;
}

// Re-export commonly used items
pub use config::Config;
pub use error::PipelineError;
pub use pipeline::Pipeline;

// Re-export from blizzard-core
pub use blizzard_core::{
    Application, CliArgs, KB, MB, MetricsConfig, ParquetCompression, PipelineContext,
    StorageProvider, StorageProviderRef, init_metrics, init_tracing, run_pipelines,
    shutdown_signal,
};

#[cfg(test)]
pub(crate) mod test_util {
    use std::collections::HashMap;
    use std::sync::Arc;

    use deltalake::arrow::array::{Int64Array, RecordBatch, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use tempfile::TempDir;

    pub fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    pub fn test_batch(num_rows: usize) -> RecordBatch {
        let ids: Vec<String> = (0..num_rows).map(|i| format!("id_{i}")).collect();
        let values: Vec<i64> = (0..num_rows).map(|i| i64::try_from(i).unwrap()).collect();
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    pub async fn create_test_storage(temp_dir: &TempDir) -> blizzard_core::StorageProviderRef {
        Arc::new(
            blizzard_core::storage::StorageProvider::for_url_with_options(
                temp_dir.path().to_str().unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap(),
        )
    }

    pub fn create_files(temp_dir: &TempDir, paths: &[&str]) {
        for path in paths {
            let full_path = temp_dir.path().join(path);
            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(full_path, b"").unwrap();
        }
    }
}
