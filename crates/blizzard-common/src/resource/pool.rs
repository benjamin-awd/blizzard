//! Connection pooling for storage providers.
//!
//! Shares storage connections between pipelines reading from the same bucket,
//! reducing connection overhead and enabling better HTTP/2 multiplexing.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::StorageError;
use crate::{StorageProvider, StorageProviderRef};

/// Extracts the bucket/container identifier from a URL.
///
/// This is used as the pool key to share connections between pipelines
/// that access the same storage bucket.
fn extract_pool_key(url: &str) -> String {
    // s3://bucket/path -> s3://bucket
    // gs://bucket/path -> gs://bucket
    // az://container@account... -> az://container@account
    // file:///path -> file://
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

    // Azure abfs:// URLs: abfss://container@account.dfs.core.windows.net/path
    if let Some(rest) = url_lower
        .strip_prefix("abfss://")
        .or_else(|| url_lower.strip_prefix("abfs://"))
    {
        // Extract container@account portion
        let container_account = rest.split('/').next().unwrap_or("");
        return format!("abfs://{}", container_account);
    }

    // Azure HTTPS URLs: https://account.blob.core.windows.net/container/path
    if url_lower.contains(".blob.core.windows.net")
        || url_lower.contains(".dfs.core.windows.net")
    {
        // Extract account and container
        if url_lower.starts_with("https://") {
            let after_scheme = &url_lower[8..];
            if let Some(dot_pos) = after_scheme.find('.') {
                let account = &after_scheme[..dot_pos];
                // Find container after the domain
                if let Some(container_start) = after_scheme.find(".net/") {
                    let after_domain = &after_scheme[container_start + 5..];
                    let container = after_domain.split('/').next().unwrap_or("");
                    return format!("az://{}@{}", container, account);
                }
            }
        }
    }

    // Local filesystem - share one provider
    if url_lower.starts_with("file://") || url_lower.starts_with("file:") || url_lower.starts_with('/') {
        return "file://".to_string();
    }

    // Unknown - use as-is (will get separate provider)
    url.to_string()
}

/// Pool of storage providers, shared across pipelines using the same bucket.
///
/// This reduces connection overhead and allows better HTTP/2 multiplexing.
/// Pipelines using the same bucket (e.g., `s3://bucket/path1` and `s3://bucket/path2`)
/// will share the same underlying storage connection.
#[derive(Debug, Default)]
pub struct StoragePool {
    providers: RwLock<HashMap<String, StorageProviderRef>>,
}

impl StoragePool {
    /// Create a new empty storage pool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create a storage provider for the given URL.
    ///
    /// Providers are keyed by bucket/container, so `s3://bucket/a` and
    /// `s3://bucket/b` share the same provider.
    pub async fn get_or_create(
        &self,
        url: &str,
        options: HashMap<String, String>,
    ) -> Result<StorageProviderRef, StorageError> {
        let key = extract_pool_key(url);

        // Fast path: check if already exists
        {
            let providers = self.providers.read().await;
            if let Some(provider) = providers.get(&key) {
                return Ok(provider.clone());
            }
        }

        // Slow path: create new provider
        let mut providers = self.providers.write().await;

        // Double-check after acquiring write lock (another task may have created it)
        if let Some(provider) = providers.get(&key) {
            return Ok(provider.clone());
        }

        let provider = Arc::new(StorageProvider::for_url_with_options(url, options).await?);
        providers.insert(key, provider.clone());
        Ok(provider)
    }

    /// Number of pooled providers.
    pub async fn len(&self) -> usize {
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
    fn test_extract_pool_key_s3() {
        assert_eq!(extract_pool_key("s3://bucket/path/file"), "s3://bucket");
        assert_eq!(extract_pool_key("s3://my-bucket/dir/"), "s3://my-bucket");
        assert_eq!(extract_pool_key("S3://BUCKET/path"), "s3://bucket");
        assert_eq!(extract_pool_key("s3a://bucket/path"), "s3://bucket");
    }

    #[test]
    fn test_extract_pool_key_gcs() {
        assert_eq!(extract_pool_key("gs://bucket/path/file"), "gs://bucket");
        assert_eq!(extract_pool_key("gs://my-bucket/dir/"), "gs://my-bucket");
        assert_eq!(extract_pool_key("GS://BUCKET/path"), "gs://bucket");
    }

    #[test]
    fn test_extract_pool_key_azure_abfs() {
        assert_eq!(
            extract_pool_key("abfss://container@account.dfs.core.windows.net/path"),
            "abfs://container@account.dfs.core.windows.net"
        );
        assert_eq!(
            extract_pool_key("abfs://container@account.dfs.core.windows.net/path"),
            "abfs://container@account.dfs.core.windows.net"
        );
    }

    #[test]
    fn test_extract_pool_key_local() {
        assert_eq!(extract_pool_key("file:///tmp/data"), "file://");
        assert_eq!(extract_pool_key("file:/tmp/data"), "file://");
        assert_eq!(extract_pool_key("/tmp/data"), "file://");
    }

    #[test]
    fn test_same_bucket_same_key() {
        let key1 = extract_pool_key("s3://bucket/path1/file1.json");
        let key2 = extract_pool_key("s3://bucket/path2/file2.json");
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_different_bucket_different_key() {
        let key1 = extract_pool_key("s3://bucket-a/path/file.json");
        let key2 = extract_pool_key("s3://bucket-b/path/file.json");
        assert_ne!(key1, key2);
    }
}
