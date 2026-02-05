//! Multi-cloud storage abstraction.
//!
//! Provides a unified interface for working with S3, GCS, Azure Blob Storage,
//! and local filesystem.

mod azure;
mod gcs;
mod local;
mod prefix;
mod s3;
mod url_parser;

pub use prefix::DatePrefixGenerator;
pub use url_parser::BackendConfig;

use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt, future::ready};
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{Attribute, AttributeValue, Attributes, ObjectStore, PutOptions, PutPayload};
use snafu::prelude::*;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tracing::debug;

use crate::emit;
use crate::error::{ObjectStoreSnafu, StorageError};
use crate::metrics::events::{
    ActiveMultipartParts, MultipartUploadCompleted, RequestStatus, StorageOperation,
    StorageRequest, StorageRequestDuration,
};

// Re-export config types
pub use azure::AzureConfig;
pub use gcs::GcsConfig;
pub use local::LocalConfig;
pub use s3::S3Config;

/// A reference-counted storage provider.
pub type StorageProviderRef = Arc<StorageProvider>;

use crate::resource::StoragePoolRef;

/// Create a storage provider, using the pool if available.
///
/// This is a convenience function that handles the common pattern of:
/// - Using pooled storage if a pool is provided (for connection sharing)
/// - Creating a standalone provider if no pool is available
pub async fn get_or_create_storage(
    pool: &Option<StoragePoolRef>,
    url: &str,
    options: HashMap<String, String>,
) -> Result<StorageProviderRef, StorageError> {
    match pool {
        Some(p) => p.get_or_create(url, options).await,
        None => Ok(Arc::new(
            StorageProvider::for_url_with_options(url, options).await?,
        )),
    }
}

/// Storage provider that abstracts over different cloud storage backends.
#[derive(Clone)]
pub struct StorageProvider {
    pub(crate) config: BackendConfig,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    /// MultipartStore for parallel part uploads with explicit part numbering.
    /// Some backends (S3, GCS, Azure) support this; local filesystem does not.
    pub(crate) multipart_store: Option<Arc<dyn MultipartStore>>,
    pub(crate) canonical_url: String,
    pub(crate) storage_options: HashMap<String, String>,
}

impl std::fmt::Debug for StorageProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageProvider<{}>", self.canonical_url)
    }
}

impl StorageProvider {
    /// Create a storage provider for the given URL with storage options.
    pub async fn for_url_with_options(
        url: &str,
        options: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let config = BackendConfig::parse_url(url, false)?;

        match config {
            BackendConfig::S3(config) => Self::construct_s3(config, options).await,
            BackendConfig::Gcs(config) => Self::construct_gcs(config).await,
            BackendConfig::Azure(config) => Self::construct_azure(config).await,
            BackendConfig::Local(config) => Self::construct_local(config).await,
        }
    }

    /// Create a storage provider with a shared ObjectStore connection.
    ///
    /// This allows multiple providers to share the same HTTP connection
    /// while having different path prefixes (configs).
    pub(crate) fn with_shared_store(
        config: BackendConfig,
        object_store: Arc<dyn ObjectStore>,
        multipart_store: Option<Arc<dyn MultipartStore>>,
        canonical_url: String,
        storage_options: HashMap<String, String>,
    ) -> Self {
        Self {
            config,
            object_store,
            multipart_store,
            canonical_url,
            storage_options,
        }
    }

    /// List files in the storage location.
    /// Returns paths relative to the configured key prefix.
    pub async fn list(
        &self,
        include_subdirectories: bool,
    ) -> Result<impl Stream<Item = Result<Path, object_store::Error>> + '_, StorageError> {
        // Emit metric for list operation initiation
        emit!(StorageRequest {
            operation: StorageOperation::List,
            status: RequestStatus::Success,
        });

        let key_path: Option<Path> = self.config.key().map(|key| key.to_string().into());
        let key_part_count = key_path
            .as_ref()
            .map(|key| key.parts().count())
            .unwrap_or_default();

        let list = self
            .object_store
            .list(key_path.as_ref())
            .filter_map(move |meta| {
                let result = match meta {
                    Ok(metadata) => {
                        let path = metadata.location;
                        if !include_subdirectories && path.parts().count() != key_part_count + 1 {
                            None
                        } else {
                            // Strip the prefix from the path so callers get relative paths
                            // This matches the contract expected by get/put/etc which qualify paths
                            let relative_path: Path = path.parts().skip(key_part_count).collect();
                            Some(Ok(relative_path))
                        }
                    }
                    Err(err) => Some(Err(err)),
                };
                ready(result)
            });

        Ok(list)
    }

    /// Get the contents of a file.
    pub async fn get(&self, path: impl Into<Path>) -> Result<Bytes, StorageError> {
        let path = path.into();
        let start = Instant::now();
        let result = self.object_store.get(&self.qualify_path(&path)).await;

        let status = if result.is_ok() {
            RequestStatus::Success
        } else {
            RequestStatus::Error
        };
        emit!(StorageRequest {
            operation: StorageOperation::Get,
            status,
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::Get,
            duration: start.elapsed(),
        });

        let bytes = result
            .context(ObjectStoreSnafu)?
            .bytes()
            .await
            .context(ObjectStoreSnafu)?;
        Ok(bytes)
    }

    /// Put bytes to a path.
    #[cfg(test)]
    pub async fn put(&self, path: impl Into<Path>, bytes: Vec<u8>) -> Result<(), StorageError> {
        let bytes = PutPayload::from(Bytes::from(bytes));
        let path = path.into();
        let path = self.qualify_path(&path);
        let start = Instant::now();
        let result = self.object_store.put(&path, bytes).await;

        let status = if result.is_ok() {
            RequestStatus::Success
        } else {
            RequestStatus::Error
        };
        emit!(StorageRequest {
            operation: StorageOperation::Put,
            status,
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::Put,
            duration: start.elapsed(),
        });

        result.context(ObjectStoreSnafu)?;
        Ok(())
    }

    /// Qualify a path with the configured key prefix.
    pub fn qualify_path<'a>(&self, path: &'a Path) -> Cow<'a, Path> {
        match self.config.key() {
            Some(prefix) => Cow::Owned(prefix.parts().chain(path.parts()).collect()),
            None => Cow::Borrowed(path),
        }
    }

    /// List files under a specific prefix (relative to configured base prefix).
    ///
    /// The prefix is appended to the configured key prefix.
    /// Returns paths relative to the configured base prefix.
    pub async fn list_with_prefix(
        &self,
        prefix: &str,
    ) -> Result<impl Stream<Item = Result<Path, object_store::Error>> + '_, StorageError> {
        emit!(StorageRequest {
            operation: StorageOperation::List,
            status: RequestStatus::Success,
        });

        // Combine the configured key prefix with the additional prefix
        let full_prefix: Path = match self.config.key() {
            Some(key) => key.parts().chain(Path::from(prefix).parts()).collect(),
            None => Path::from(prefix),
        };

        let key_part_count = self
            .config
            .key()
            .map(|key| key.parts().count())
            .unwrap_or_default();

        let list = self
            .object_store
            .list(Some(&full_prefix))
            .filter_map(move |meta| {
                let result = match meta {
                    Ok(metadata) => {
                        // Strip the base prefix from the path so callers get relative paths
                        let relative_path: Path =
                            metadata.location.parts().skip(key_part_count).collect();
                        Some(Ok(relative_path))
                    }
                    Err(err) => Some(Err(err)),
                };
                ready(result)
            });

        Ok(list)
    }

    /// Get storage options for external integrations (e.g., Delta Lake).
    pub fn storage_options(&self) -> &HashMap<String, String> {
        &self.storage_options
    }

    /// Get the backend configuration.
    pub fn config(&self) -> &BackendConfig {
        &self.config
    }

    /// Put a payload to a path.
    pub async fn put_payload(&self, path: &Path, payload: PutPayload) -> Result<(), StorageError> {
        self.put_payload_with_opts(path, payload, PutOptions::default())
            .await
    }

    /// Put a Parquet file to a path with the correct content type.
    ///
    /// Sets `Content-Type: application/vnd.apache.parquet` on cloud storage backends.
    /// Local filesystem doesn't support attributes, so they are skipped.
    pub async fn put_parquet(&self, path: &Path, payload: PutPayload) -> Result<(), StorageError> {
        // Local filesystem doesn't support content-type attributes
        if matches!(self.config, BackendConfig::Local(_)) {
            return self.put_payload(path, payload).await;
        }

        let opts = PutOptions {
            attributes: Attributes::from_iter([(
                Attribute::ContentType,
                AttributeValue::from("application/vnd.apache.parquet"),
            )]),
            ..Default::default()
        };
        self.put_payload_with_opts(path, payload, opts).await
    }

    /// Put a payload to a path with options.
    async fn put_payload_with_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<(), StorageError> {
        let path = self.qualify_path(path);
        let start = Instant::now();
        let result = self.object_store.put_opts(&path, payload, opts).await;

        let status = if result.is_ok() {
            RequestStatus::Success
        } else {
            RequestStatus::Error
        };
        emit!(StorageRequest {
            operation: StorageOperation::Put,
            status,
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::Put,
            duration: start.elapsed(),
        });

        result.context(ObjectStoreSnafu)?;
        Ok(())
    }

    /// Delete a file at the given path.
    pub async fn delete(&self, path: &Path) -> Result<(), StorageError> {
        let path = self.qualify_path(path);
        let start = Instant::now();
        let result = self.object_store.delete(&path).await;

        let status = if result.is_ok() {
            RequestStatus::Success
        } else {
            RequestStatus::Error
        };
        emit!(StorageRequest {
            operation: StorageOperation::Delete,
            status,
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::Delete,
            duration: start.elapsed(),
        });

        result.context(ObjectStoreSnafu)?;
        Ok(())
    }

    /// Atomically write content to a path using temp file + rename.
    ///
    /// This ensures the target file is never partially written:
    /// 1. Write to `{path}.tmp`
    /// 2. Rename `{path}.tmp` to `{path}`
    ///
    /// If the write or rename fails, the original file (if any) is unchanged.
    pub async fn atomic_write(&self, path: &Path, content: Vec<u8>) -> Result<(), StorageError> {
        let temp_path = Path::from(format!("{path}.tmp"));
        self.put_payload(&temp_path, PutPayload::from(Bytes::from(content)))
            .await?;
        self.rename(&temp_path, path).await
    }

    /// Atomically write a payload to a path using temp file + rename.
    ///
    /// This ensures the target file is never partially written:
    /// 1. Write to `{path}.tmp`
    /// 2. Rename `{path}.tmp` to `{path}`
    ///
    /// If the write or rename fails, the original file (if any) is unchanged.
    pub async fn atomic_put_payload(
        &self,
        path: &Path,
        payload: PutPayload,
    ) -> Result<(), StorageError> {
        let temp_path = Path::from(format!("{path}.tmp"));
        self.put_payload(&temp_path, payload).await?;
        self.rename(&temp_path, path).await
    }

    /// Server-side rename (move) operation.
    ///
    /// This is a zero-copy operation on cloud storage (GCS, S3, Azure).
    /// The object is NOT downloaded - it's renamed server-side.
    ///
    /// Backend implementations:
    /// - **GCS**: Server-side `rewriteObject` API (free, no egress)
    /// - **S3**: Server-side `CopyObject` + `DeleteObject` (free, no egress)
    /// - **Ceph RadosGW**: Server-side `CopyObject` + `DeleteObject` (S3-compatible, free)
    /// - **Azure**: Server-side copy operation
    /// - **Local**: `std::fs::rename`
    pub async fn rename(&self, from: &Path, to: &Path) -> Result<(), StorageError> {
        let from_qualified = self.qualify_path(from);
        let to_qualified = self.qualify_path(to);
        let start = Instant::now();
        let result = self
            .object_store
            .rename(&from_qualified, &to_qualified)
            .await;

        let status = if result.is_ok() {
            RequestStatus::Success
        } else {
            RequestStatus::Error
        };
        emit!(StorageRequest {
            operation: StorageOperation::Rename,
            status,
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::Rename,
            duration: start.elapsed(),
        });

        result.context(ObjectStoreSnafu)?;
        Ok(())
    }

    /// Upload bytes using parallel multipart upload.
    ///
    /// Uses the `MultipartStore` trait which provides explicit part numbering,
    /// allowing parts to be uploaded in parallel.
    ///
    /// For backends that don't support `MultipartStore` or for small files,
    /// falls back to simple PUT.
    pub async fn put_multipart_bytes(
        &self,
        path: &Path,
        bytes: Bytes,
        part_size: usize,
        min_multipart_size: usize,
        max_concurrent_parts: usize,
    ) -> Result<(), StorageError> {
        // Small files use simple PUT
        if bytes.len() < min_multipart_size || self.multipart_store.is_none() {
            return self.put_payload(path, PutPayload::from(bytes)).await;
        }

        let multipart_store = self.multipart_store.as_ref().unwrap();
        let qualified_path = self.qualify_path(path).into_owned();

        // Start multipart upload
        let create_start = Instant::now();
        let create_result = multipart_store.create_multipart(&qualified_path).await;
        emit!(StorageRequest {
            operation: StorageOperation::CreateMultipart,
            status: if create_result.is_ok() {
                RequestStatus::Success
            } else {
                RequestStatus::Error
            },
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::CreateMultipart,
            duration: create_start.elapsed(),
        });
        let multipart_id = create_result.context(ObjectStoreSnafu)?;

        // Create parts with indices
        let parts: Vec<(usize, Bytes)> = (0..)
            .zip((0..bytes.len()).step_by(part_size))
            .map(|(i, offset)| {
                let end = std::cmp::min(offset + part_size, bytes.len());
                (i, bytes.slice(offset..end))
            })
            .collect();

        let total_parts = parts.len();
        debug!(
            "Starting parallel multipart upload for {} ({} bytes, {} parts, concurrency={})",
            path,
            bytes.len(),
            total_parts,
            max_concurrent_parts
        );

        // Upload parts in parallel using explicit part numbers
        let multipart_id = Arc::new(multipart_id);
        let qualified_path = Arc::new(qualified_path);
        let active_parts = Arc::new(AtomicUsize::new(0));

        let results: Vec<(usize, PartId)> = futures::stream::iter(parts)
            .map(|(idx, data)| {
                let multipart_store = multipart_store.clone();
                let qualified_path = qualified_path.clone();
                let multipart_id = multipart_id.clone();
                let active_parts = active_parts.clone();
                async move {
                    let count = active_parts.fetch_add(1, Ordering::Relaxed) + 1;
                    emit!(ActiveMultipartParts { count });

                    let part_start = Instant::now();
                    let result = multipart_store
                        .put_part(&qualified_path, &multipart_id, idx, data.into())
                        .await;

                    emit!(StorageRequest {
                        operation: StorageOperation::PutPart,
                        status: if result.is_ok() {
                            RequestStatus::Success
                        } else {
                            RequestStatus::Error
                        },
                    });
                    emit!(StorageRequestDuration {
                        operation: StorageOperation::PutPart,
                        duration: part_start.elapsed(),
                    });

                    let count = active_parts.fetch_sub(1, Ordering::Relaxed) - 1;
                    emit!(ActiveMultipartParts { count });

                    let part_id = result.context(ObjectStoreSnafu)?;
                    let part_num = idx + 1;
                    debug!("Uploaded part {part_num}/{total_parts}");
                    Ok::<_, StorageError>((idx, part_id))
                }
            })
            .buffer_unordered(max_concurrent_parts)
            .try_collect()
            .await?;

        // Sort by index and extract PartIds (parts may complete out of order)
        let mut results = results;
        results.sort_by_key(|(idx, _)| *idx);
        let part_ids: Vec<PartId> = results.into_iter().map(|(_, id)| id).collect();

        // Complete the upload
        let complete_start = Instant::now();
        let complete_result = self
            .multipart_store
            .as_ref()
            .unwrap()
            .complete_multipart(&qualified_path, &multipart_id, part_ids)
            .await;

        emit!(StorageRequest {
            operation: StorageOperation::CompleteMultipart,
            status: if complete_result.is_ok() {
                RequestStatus::Success
            } else {
                RequestStatus::Error
            },
        });
        emit!(StorageRequestDuration {
            operation: StorageOperation::CompleteMultipart,
            duration: complete_start.elapsed(),
        });

        complete_result.context(ObjectStoreSnafu)?;
        emit!(MultipartUploadCompleted);
        debug!("Completed parallel multipart upload for {path}");

        Ok(())
    }
}

/// List NDJSON.gz files recursively.
pub async fn list_ndjson_files(storage: &StorageProvider) -> Result<Vec<String>, StorageError> {
    let mut files = Vec::new();
    let mut stream = storage.list(true).await?;
    let mut total_listed = 0;

    while let Some(result) = stream.next().await {
        let path = result.context(ObjectStoreSnafu)?;
        total_listed += 1;

        // Check extension without allocating for non-matching files
        if path.as_ref().ends_with(".ndjson.gz") {
            files.push(path.to_string());
        }
    }

    tracing::debug!(
        "Listed {} total files, {} are .ndjson.gz",
        total_listed,
        files.len()
    );

    // Sort by path for consistent ordering
    files.sort();

    Ok(files)
}

/// List NDJSON.gz files with optional prefix filtering and early termination.
///
/// When prefixes are provided, only lists files under those prefixes.
/// When a limit is provided, stops listing once enough files are found.
/// This is much more efficient for operations that only need a few files
/// (like schema inference which only needs 1-3 files).
///
/// Results are sorted for consistent ordering.
pub async fn list_ndjson_files_with_limit(
    storage: &StorageProvider,
    prefixes: Option<&[String]>,
    limit: Option<usize>,
    pipeline: &str,
) -> Result<Vec<String>, StorageError> {
    // For unlimited listing, use the full listing function
    let Some(max_files) = limit else {
        return list_ndjson_files_with_prefixes(storage, prefixes, pipeline).await;
    };

    if max_files == 0 {
        return Ok(Vec::new());
    }

    let mut files = Vec::with_capacity(max_files);

    match prefixes {
        None | Some([]) => {
            // List all files but stop early
            let mut stream = storage.list(true).await?;
            while let Some(result) = stream.next().await {
                let path = result.context(ObjectStoreSnafu)?;
                if path.as_ref().ends_with(".ndjson.gz") {
                    files.push(path.to_string());
                    if files.len() >= max_files {
                        tracing::debug!(
                            target = %pipeline,
                            "Found {} files for schema inference, stopping early",
                            files.len()
                        );
                        break;
                    }
                }
            }
        }
        Some(prefixes) => {
            tracing::debug!(
                target = %pipeline,
                "Listing up to {} files under {} prefixes for schema inference",
                max_files,
                prefixes.len()
            );

            'outer: for prefix in prefixes {
                let stream_result = storage.list_with_prefix(prefix).await;
                let mut stream = match stream_result {
                    Ok(s) => s,
                    Err(e) if e.is_not_found() => continue,
                    Err(e) => return Err(e),
                };

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(path) => {
                            if path.as_ref().ends_with(".ndjson.gz") {
                                files.push(path.to_string());
                                if files.len() >= max_files {
                                    tracing::debug!(
                                        target = %pipeline,
                                        "Found {} files for schema inference, stopping early",
                                        files.len()
                                    );
                                    break 'outer;
                                }
                            }
                        }
                        Err(e) => {
                            if !e.to_string().contains("not found") {
                                return Err(StorageError::ObjectStore { source: e });
                            }
                        }
                    }
                }
            }
        }
    }

    files.sort();
    Ok(files)
}

/// List NDJSON.gz files with optional prefix filtering.
///
/// When prefixes are provided, only lists files under those prefixes.
/// This is much more efficient for partitioned data where only recent
/// partitions need to be processed.
///
/// Results are deduplicated and sorted for consistent ordering.
pub async fn list_ndjson_files_with_prefixes(
    storage: &StorageProvider,
    prefixes: Option<&[String]>,
    pipeline: &str,
) -> Result<Vec<String>, StorageError> {
    match prefixes {
        None => list_ndjson_files(storage).await,
        Some([]) => list_ndjson_files(storage).await,
        Some(prefixes) => {
            let mut files = Vec::new();
            let mut total_listed = 0;

            tracing::debug!(
                target = %pipeline,
                "Listing files under {} date prefixes: {:?}",
                prefixes.len(),
                if prefixes.len() <= 5 {
                    prefixes.to_vec()
                } else {
                    let mut preview = prefixes[..3].to_vec();
                    preview.push(format!("... and {} more", prefixes.len() - 3));
                    preview
                }
            );

            for prefix in prefixes {
                let stream_result = storage.list_with_prefix(prefix).await;

                // Handle missing prefixes gracefully - they just return empty results
                let mut stream = match stream_result {
                    Ok(s) => s,
                    Err(e) if e.is_not_found() => {
                        tracing::debug!("Prefix not found (skipping): {}", prefix);
                        continue;
                    }
                    Err(e) => return Err(e),
                };

                while let Some(result) = stream.next().await {
                    match result {
                        Ok(path) => {
                            total_listed += 1;
                            if path.as_ref().ends_with(".ndjson.gz") {
                                files.push(path.to_string());
                            }
                        }
                        Err(e) => {
                            // Not found errors during listing are fine (empty prefix)
                            if !e.to_string().contains("not found") {
                                return Err(StorageError::ObjectStore { source: e });
                            }
                        }
                    }
                }
            }

            tracing::debug!(
                "Listed {} total files under {} prefixes, {} are .ndjson.gz",
                total_listed,
                prefixes.len(),
                files.len()
            );

            // Sort and deduplicate (prefixes might overlap at boundaries)
            files.sort();
            files.dedup();

            Ok(files)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_list_returns_relative_paths() {
        // Create a temp directory with nested structure simulating partitioned data
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create nested directories: prefix/partition1/partition2/file.ndjson.gz
        let prefix = "region=us-east-1/subject=orders";
        let nested_path = base_path.join(prefix).join("date=2024-01-01");
        std::fs::create_dir_all(&nested_path).unwrap();

        // Create test files
        let file1 = nested_path.join("file1.ndjson.gz");
        let file2 = nested_path.join("file2.ndjson.gz");
        std::fs::write(&file1, b"test data 1").unwrap();
        std::fs::write(&file2, b"test data 2").unwrap();

        // Create storage provider pointing to the prefix
        let storage_url = format!("{}/{}", base_path.display(), prefix);
        let storage = StorageProvider::for_url_with_options(&storage_url, HashMap::new())
            .await
            .unwrap();

        // List files - should return relative paths (not including the prefix)
        let mut stream = storage.list(true).await.unwrap();
        let mut listed_paths = Vec::new();
        while let Some(result) = stream.next().await {
            let path = result.unwrap();
            listed_paths.push(path.to_string());
        }

        listed_paths.sort();

        // Paths should be relative to the prefix, e.g., "date=2024-01-01/file1.ndjson.gz"
        assert_eq!(listed_paths.len(), 2);
        assert_eq!(listed_paths[0], "date=2024-01-01/file1.ndjson.gz");
        assert_eq!(listed_paths[1], "date=2024-01-01/file2.ndjson.gz");

        // Verify that we can get() these files using the relative paths
        for path in &listed_paths {
            let content = storage.get(path.as_str()).await.unwrap();
            assert!(!content.is_empty());
        }
    }

    #[tokio::test]
    async fn test_storage_provider_rename() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create source and target directories
        let source_dir = base_path.join("date=2024-01-01");
        let target_dir = base_path.join("date=2024-01-02");
        std::fs::create_dir_all(&source_dir).unwrap();
        std::fs::create_dir_all(&target_dir).unwrap();

        let source_file = source_dir.join("test.parquet");
        std::fs::write(&source_file, b"parquet data").unwrap();

        // Create storage provider
        let storage =
            StorageProvider::for_url_with_options(base_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap();

        // Rename file between partition directories
        let from = Path::from("date=2024-01-01/test.parquet");
        let to = Path::from("date=2024-01-02/test.parquet");
        storage.rename(&from, &to).await.unwrap();

        // Verify source no longer exists
        assert!(!source_file.exists(), "Source file should be removed");

        // Verify destination exists with correct content
        let dest_file = target_dir.join("test.parquet");
        assert!(dest_file.exists(), "Destination file should exist");
        assert_eq!(
            std::fs::read(&dest_file).unwrap(),
            b"parquet data",
            "Content should be preserved"
        );
    }

    #[tokio::test]
    async fn test_list_ndjson_files_with_limit_stops_early() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create many files (more than the limit)
        let partition = base_path.join("date=2024-01-01");
        std::fs::create_dir_all(&partition).unwrap();

        for i in 0..10 {
            std::fs::write(partition.join(format!("file{i:02}.ndjson.gz")), b"").unwrap();
        }

        let storage =
            StorageProvider::for_url_with_options(base_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap();

        // Request only 3 files
        let files = list_ndjson_files_with_limit(&storage, None, Some(3), "test")
            .await
            .unwrap();

        // Should return exactly 3 files (early termination)
        assert_eq!(files.len(), 3);
    }

    #[tokio::test]
    async fn test_list_ndjson_files_with_limit_with_prefixes() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create multiple partitions with files
        let partition1 = base_path.join("date=2024-01-01");
        let partition2 = base_path.join("date=2024-01-02");
        std::fs::create_dir_all(&partition1).unwrap();
        std::fs::create_dir_all(&partition2).unwrap();

        for i in 0..5 {
            std::fs::write(partition1.join(format!("file{i:02}.ndjson.gz")), b"").unwrap();
            std::fs::write(partition2.join(format!("file{i:02}.ndjson.gz")), b"").unwrap();
        }

        let storage =
            StorageProvider::for_url_with_options(base_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap();

        let prefixes = vec!["date=2024-01-01".to_string(), "date=2024-01-02".to_string()];

        // Request only 3 files with prefix filter
        let files = list_ndjson_files_with_limit(&storage, Some(&prefixes), Some(3), "test")
            .await
            .unwrap();

        // Should return exactly 3 files (early termination)
        assert_eq!(files.len(), 3);
    }

    #[tokio::test]
    async fn test_list_ndjson_files_with_limit_none_returns_all() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        let partition = base_path.join("date=2024-01-01");
        std::fs::create_dir_all(&partition).unwrap();

        for i in 0..5 {
            std::fs::write(partition.join(format!("file{i:02}.ndjson.gz")), b"").unwrap();
        }

        let storage =
            StorageProvider::for_url_with_options(base_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap();

        // No limit - should return all files
        let files = list_ndjson_files_with_limit(&storage, None, None, "test")
            .await
            .unwrap();

        assert_eq!(files.len(), 5);
    }

    #[tokio::test]
    async fn test_list_ndjson_files_with_limit_zero_returns_empty() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        let partition = base_path.join("date=2024-01-01");
        std::fs::create_dir_all(&partition).unwrap();
        std::fs::write(partition.join("file.ndjson.gz"), b"").unwrap();

        let storage =
            StorageProvider::for_url_with_options(base_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap();

        // Limit of 0 should return empty
        let files = list_ndjson_files_with_limit(&storage, None, Some(0), "test")
            .await
            .unwrap();

        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn test_list_ndjson_files_with_limit_fewer_files_than_limit() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        let partition = base_path.join("date=2024-01-01");
        std::fs::create_dir_all(&partition).unwrap();

        // Create only 2 files
        std::fs::write(partition.join("file01.ndjson.gz"), b"").unwrap();
        std::fs::write(partition.join("file02.ndjson.gz"), b"").unwrap();

        let storage =
            StorageProvider::for_url_with_options(base_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap();

        // Request 10 files but only 2 exist
        let files = list_ndjson_files_with_limit(&storage, None, Some(10), "test")
            .await
            .unwrap();

        // Should return all 2 files
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_atomic_write() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        let storage =
            StorageProvider::for_url_with_options(base_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap();

        let path = Path::from("test_file.json");
        let content = b"test content".to_vec();

        // Atomic write should create the file
        storage.atomic_write(&path, content.clone()).await.unwrap();

        // Verify the file exists with correct content
        let read_content = storage.get("test_file.json").await.unwrap();
        assert_eq!(read_content.as_ref(), content.as_slice());

        // Temp file should not exist
        let temp_path = base_path.join("test_file.json.tmp");
        assert!(!temp_path.exists(), "Temp file should be cleaned up");
    }

    #[tokio::test]
    async fn test_atomic_write_overwrites_existing() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path();

        // Create initial file
        std::fs::write(base_path.join("test_file.json"), b"old content").unwrap();

        let storage =
            StorageProvider::for_url_with_options(base_path.to_str().unwrap(), HashMap::new())
                .await
                .unwrap();

        let path = Path::from("test_file.json");
        let new_content = b"new content".to_vec();

        // Atomic write should overwrite atomically
        storage
            .atomic_write(&path, new_content.clone())
            .await
            .unwrap();

        // Verify new content
        let read_content = storage.get("test_file.json").await.unwrap();
        assert_eq!(read_content.as_ref(), new_content.as_slice());
    }
}
