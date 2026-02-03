//! Connection pooling for storage providers.
//!
//! Shares underlying ObjectStore connections between providers accessing the same
//! bucket. Each StorageProvider has its own path prefix (config.key), but they
//! share the HTTP connection for better multiplexing.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use object_store::ObjectStore;
use object_store::multipart::MultipartStore;

use crate::error::StorageError;
use crate::storage::BackendConfig;
use crate::{StorageProvider, StorageProviderRef};

/// Reference-counted handle to a [`StoragePool`].
pub type StoragePoolRef = Arc<StoragePool>;

/// Cached ObjectStore connection that can be shared across providers.
#[derive(Clone)]
struct SharedStore {
    object_store: Arc<dyn ObjectStore>,
    multipart_store: Option<Arc<dyn MultipartStore>>,
}

/// Extracts the bucket/container identifier from a URL for connection sharing.
fn extract_bucket_key(url: &str) -> String {
    let url_lower = url.to_lowercase();

    if let Some(rest) = url_lower
        .strip_prefix("s3://")
        .or_else(|| url_lower.strip_prefix("s3a://"))
    {
        let bucket = rest.split('/').next().unwrap_or("");
        return format!("s3://{}", bucket);
    }

    if let Some(rest) = url_lower.strip_prefix("gs://") {
        let bucket = rest.split('/').next().unwrap_or("");
        return format!("gs://{}", bucket);
    }

    if let Some(rest) = url_lower
        .strip_prefix("abfss://")
        .or_else(|| url_lower.strip_prefix("abfs://"))
    {
        let container_account = rest.split('/').next().unwrap_or("");
        return format!("abfs://{}", container_account);
    }

    if (url_lower.contains(".blob.core.windows.net") || url_lower.contains(".dfs.core.windows.net"))
        && let Some(after_scheme) = url_lower.strip_prefix("https://")
        && let Some(dot_pos) = after_scheme.find('.')
    {
        let account = &after_scheme[..dot_pos];
        if let Some(container_start) = after_scheme.find(".net/") {
            let after_domain = &after_scheme[container_start + 5..];
            let container = after_domain.split('/').next().unwrap_or("");
            return format!("az://{}@{}", container, account);
        }
    }

    if url_lower.starts_with("file://")
        || url_lower.starts_with("file:")
        || url_lower.starts_with('/')
    {
        return "file://".to_string();
    }

    url.to_string()
}

/// Pool of storage connections, shared across providers using the same bucket.
///
/// The pool caches ObjectStore connections by bucket for HTTP/2 multiplexing.
/// Each StorageProvider has its own path prefix (via `config.key()`), but they
/// share the underlying HTTP connection.
#[derive(Default)]
pub struct StoragePool {
    /// Cached ObjectStore connections keyed by bucket.
    stores: RwLock<HashMap<String, SharedStore>>,
    /// Cached StorageProviders keyed by full URL.
    providers: RwLock<HashMap<String, StorageProviderRef>>,
}

impl std::fmt::Debug for StoragePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoragePool").finish_non_exhaustive()
    }
}

impl StoragePool {
    /// Create a new empty storage pool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create a storage provider for the given URL.
    ///
    /// Providers with URLs in the same bucket share the underlying ObjectStore
    /// connection, but each has its own path prefix via `config.key()`.
    pub async fn get_or_create(
        &self,
        url: &str,
        options: HashMap<String, String>,
    ) -> Result<StorageProviderRef, StorageError> {
        let provider_key = url.to_lowercase().trim_end_matches('/').to_string();

        // Fast path: check if provider already exists
        {
            let providers = self.providers.read().await;
            if let Some(provider) = providers.get(&provider_key) {
                return Ok(provider.clone());
            }
        }

        // Slow path: need to create provider
        let bucket_key = extract_bucket_key(url);

        // Check if we have a cached ObjectStore for this bucket
        let shared_store = {
            let stores = self.stores.read().await;
            stores.get(&bucket_key).cloned()
        };

        let provider = if let Some(store) = shared_store {
            // Reuse existing ObjectStore with new config (different path prefix)
            let config = BackendConfig::parse_url(url, false)?;

            Arc::new(StorageProvider::with_shared_store(
                config,
                store.object_store,
                store.multipart_store,
                url.to_string(),
                options,
            ))
        } else {
            // Create new ObjectStore and cache it
            // First, create a provider for the bucket root (no path prefix)
            let root_provider =
                StorageProvider::for_url_with_options(&bucket_key, options.clone()).await?;

            // Cache the root ObjectStore for future providers in same bucket
            let shared = SharedStore {
                object_store: root_provider.object_store.clone(),
                multipart_store: root_provider.multipart_store.clone(),
            };
            {
                let mut stores = self.stores.write().await;
                stores.insert(bucket_key, shared.clone());
            }

            // Now create the actual provider with the correct config (path prefix)
            let config = BackendConfig::parse_url(url, false)?;

            Arc::new(StorageProvider::with_shared_store(
                config,
                shared.object_store,
                shared.multipart_store,
                url.to_string(),
                options,
            ))
        };

        // Cache the provider
        {
            let mut providers = self.providers.write().await;
            providers.insert(provider_key, provider.clone());
        }

        Ok(provider)
    }

    /// Number of cached ObjectStore connections.
    pub async fn store_count(&self) -> usize {
        self.stores.read().await.len()
    }

    /// Number of cached StorageProviders.
    pub async fn provider_count(&self) -> usize {
        self.providers.read().await.len()
    }

    /// Returns true if the pool is empty.
    pub async fn is_empty(&self) -> bool {
        self.providers.read().await.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_bucket_key_s3() {
        assert_eq!(extract_bucket_key("s3://bucket/path/file"), "s3://bucket");
        assert_eq!(extract_bucket_key("s3://my-bucket/dir/"), "s3://my-bucket");
        assert_eq!(extract_bucket_key("S3://BUCKET/path"), "s3://bucket");
        assert_eq!(extract_bucket_key("s3a://bucket/path"), "s3://bucket");
    }

    #[test]
    fn test_extract_bucket_key_gcs() {
        assert_eq!(extract_bucket_key("gs://bucket/path/file"), "gs://bucket");
        assert_eq!(extract_bucket_key("gs://my-bucket/dir/"), "gs://my-bucket");
        assert_eq!(extract_bucket_key("GS://BUCKET/path"), "gs://bucket");
    }

    #[test]
    fn test_extract_bucket_key_azure() {
        assert_eq!(
            extract_bucket_key("abfss://container@account.dfs.core.windows.net/path"),
            "abfs://container@account.dfs.core.windows.net"
        );
    }

    #[test]
    fn test_extract_bucket_key_local() {
        assert_eq!(extract_bucket_key("file:///tmp/data"), "file://");
        assert_eq!(extract_bucket_key("/tmp/data"), "file://");
    }

    #[test]
    fn test_same_bucket_same_key() {
        let key1 = extract_bucket_key("s3://bucket/path1/file1.json");
        let key2 = extract_bucket_key("s3://bucket/path2/file2.json");
        assert_eq!(key1, key2, "Same bucket should have same key");
    }

    #[test]
    fn test_different_bucket_different_key() {
        let key1 = extract_bucket_key("s3://bucket-a/path/file.json");
        let key2 = extract_bucket_key("s3://bucket-b/path/file.json");
        assert_ne!(key1, key2, "Different buckets should have different keys");
    }

    #[tokio::test]
    async fn test_pool_shares_object_store_for_same_bucket() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path();

        // Create two different paths in the same "bucket" (directory)
        std::fs::create_dir_all(base.join("table_a")).unwrap();
        std::fs::create_dir_all(base.join("table_b")).unwrap();

        let pool = StoragePool::new();

        let url_a = format!("{}/table_a", base.display());
        let url_b = format!("{}/table_b", base.display());

        let provider_a = pool.get_or_create(&url_a, HashMap::new()).await.unwrap();
        let provider_b = pool.get_or_create(&url_b, HashMap::new()).await.unwrap();

        // Should have 1 ObjectStore (shared) but 2 providers
        assert_eq!(pool.store_count().await, 1);
        assert_eq!(pool.provider_count().await, 2);

        // Providers should be different instances
        assert!(!Arc::ptr_eq(&provider_a, &provider_b));

        // But share the same ObjectStore
        assert!(Arc::ptr_eq(
            &provider_a.object_store,
            &provider_b.object_store
        ));
    }

    #[tokio::test]
    async fn test_pool_reuses_provider_for_same_url() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let url = temp_dir.path().to_str().unwrap();

        let pool = StoragePool::new();

        let provider1 = pool.get_or_create(url, HashMap::new()).await.unwrap();
        let provider2 = pool.get_or_create(url, HashMap::new()).await.unwrap();

        // Same URL should return same provider instance
        assert!(Arc::ptr_eq(&provider1, &provider2));
        assert_eq!(pool.provider_count().await, 1);
    }
}
