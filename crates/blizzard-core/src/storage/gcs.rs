//! Google Cloud Storage backend implementation.

use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::multipart::MultipartStore;
use object_store::path::Path;
use object_store::{ObjectStore, RetryConfig};
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use crate::error::{GcsConfigSnafu, StorageError};

use super::{BackendConfig, StorageProvider};

/// Google Cloud Storage configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsConfig {
    pub bucket: String,
    pub key: Option<Path>,
}

/// Create a standard retry configuration for cloud storage operations.
fn default_retry_config() -> RetryConfig {
    RetryConfig::default()
}

impl StorageProvider {
    pub(super) async fn construct_gcs(config: GcsConfig) -> Result<Self, StorageError> {
        let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(&config.bucket);

        builder = builder.with_retry(default_retry_config());

        if let Ok(service_account_key) = std::env::var("GOOGLE_SERVICE_ACCOUNT_KEY") {
            debug!("Constructing GCS builder with service account key");
            builder = builder.with_service_account_key(&service_account_key);
        }

        let mut canonical_url = format!("https://{}.storage.googleapis.com", config.bucket);
        if let Some(key) = &config.key {
            canonical_url = format!("{canonical_url}/{key}");
        }

        let gcs_store = Arc::new(builder.build().context(GcsConfigSnafu)?);
        // GCS supports MultipartStore for parallel part uploads
        let multipart_store: Option<Arc<dyn MultipartStore>> = Some(gcs_store.clone());
        let object_store: Arc<dyn ObjectStore> = gcs_store;

        Ok(Self {
            config: BackendConfig::Gcs(config),
            object_store,
            multipart_store,
            canonical_url,
            storage_options: HashMap::new(),
        })
    }
}
