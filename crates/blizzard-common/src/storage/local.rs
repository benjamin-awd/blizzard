//! Local filesystem storage backend implementation.

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use snafu::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{IoSnafu, ObjectStoreSnafu, StorageError};

use super::{BackendConfig, StorageProvider};

/// Local filesystem configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalConfig {
    pub path: String,
    pub key: Option<Path>,
}

impl StorageProvider {
    pub(super) async fn construct_local(config: LocalConfig) -> Result<Self, StorageError> {
        tokio::fs::create_dir_all(&config.path)
            .await
            .context(IoSnafu)?;

        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&config.path).context(ObjectStoreSnafu)?);

        let canonical_url = format!("file://{}", config.path);

        Ok(Self {
            config: BackendConfig::Local(config),
            object_store,
            // Local filesystem does not support MultipartStore
            multipart_store: None,
            canonical_url,
            storage_options: HashMap::new(),
        })
    }
}
