//! S3 storage backend implementation.

use object_store::aws::AmazonS3Builder;
use object_store::multipart::MultipartStore;
use object_store::path::Path;
use object_store::{ObjectStore, RetryConfig};
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{S3ConfigSnafu, StorageError};

use super::{BackendConfig, StorageProvider};

/// S3 storage configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub bucket: String,
    pub key: Option<Path>,
}

/// Create a standard retry configuration for cloud storage operations.
fn default_retry_config() -> RetryConfig {
    RetryConfig::default()
}

impl StorageProvider {
    pub(super) async fn construct_s3(
        config: S3Config,
        options: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);

        for (key, value) in &options {
            builder = builder.with_config(key.parse().context(S3ConfigSnafu)?, value.clone());
        }

        builder = builder.with_retry(default_retry_config());

        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }

        if let Some(endpoint) = &config.endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(false)
                .with_allow_http(true);
        }

        let canonical_url = match (&config.region, &config.endpoint) {
            (_, Some(endpoint)) => format!("s3::{}/{}", endpoint, config.bucket),
            (Some(region), _) => format!("https://s3.{}.amazonaws.com/{}", region, config.bucket),
            _ => format!("https://s3.amazonaws.com/{}", config.bucket),
        };

        let canonical_url = if let Some(key) = &config.key {
            format!("{}/{}", canonical_url, key)
        } else {
            canonical_url
        };

        let s3_store = Arc::new(builder.build().context(S3ConfigSnafu)?);
        // S3 supports MultipartStore for parallel part uploads
        let multipart_store: Option<Arc<dyn MultipartStore>> = Some(s3_store.clone());
        let object_store: Arc<dyn ObjectStore> = s3_store;

        Ok(Self {
            config: BackendConfig::S3(config),
            object_store,
            multipart_store,
            canonical_url,
            storage_options: options,
        })
    }
}
