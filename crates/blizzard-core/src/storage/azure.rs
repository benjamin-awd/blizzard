//! Azure Blob Storage backend implementation.

use object_store::azure::MicrosoftAzureBuilder;
use object_store::multipart::MultipartStore;
use object_store::path::Path;
use object_store::{ObjectStore, RetryConfig};
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{AzureConfigSnafu, StorageError};

use super::{BackendConfig, StorageProvider};

/// Azure Blob Storage configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureConfig {
    pub account: String,
    pub container: String,
    pub key: Option<Path>,
}

/// Create a standard retry configuration for cloud storage operations.
fn default_retry_config() -> RetryConfig {
    RetryConfig::default()
}

impl StorageProvider {
    pub(super) async fn construct_azure(config: AzureConfig) -> Result<Self, StorageError> {
        let builder = MicrosoftAzureBuilder::from_env()
            .with_container_name(&config.container)
            .with_retry(default_retry_config());

        let canonical_url = format!(
            "https://{}.blob.core.windows.net/{}",
            config.account, config.container
        );

        let azure_store = Arc::new(builder.build().context(AzureConfigSnafu)?);
        // Azure supports MultipartStore for parallel part uploads
        let multipart_store: Option<Arc<dyn MultipartStore>> = Some(azure_store.clone());
        let object_store: Arc<dyn ObjectStore> = azure_store;

        Ok(Self {
            config: BackendConfig::Azure(config),
            object_store,
            multipart_store,
            canonical_url,
            storage_options: HashMap::new(),
        })
    }
}
