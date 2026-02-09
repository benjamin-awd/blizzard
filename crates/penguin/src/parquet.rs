//! Shared parquet utilities.

use blizzard_core::error::StorageError;
use blizzard_core::storage::StorageProvider;
use deltalake::parquet::errors::ParquetError;
use deltalake::parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use tracing::warn;

/// Maximum footer prefetch size (64 KB).
///
/// Parquet footers are typically a few KB. 64 KB covers virtually all files
/// in a single range read, avoiding a full-file download.
const FOOTER_PREFETCH: u64 = 64 * 1024;

/// Error from footer-based parquet metadata reads.
#[derive(Debug)]
pub(crate) enum FooterReadError {
    Storage(StorageError),
    Parquet(ParquetError),
}

/// Read parquet metadata using a footer-only suffix-range read.
///
/// Fetches the last 64 KB of the file and parses the parquet footer.
/// Falls back to a full file download if the footer exceeds the prefetch.
/// Returns the total file size and parsed metadata.
pub(crate) async fn read_parquet_footer(
    storage: &StorageProvider,
    path: &str,
    table: &str,
) -> Result<(u64, ParquetMetaData), FooterReadError> {
    let (object_meta, tail_bytes) = storage
        .get_suffix(path, FOOTER_PREFETCH)
        .await
        .map_err(FooterReadError::Storage)?;
    let file_size = object_meta.size;

    let mut reader = ParquetMetaDataReader::new();
    match reader.try_parse_sized(&tail_bytes, file_size) {
        Ok(()) => {
            let metadata = reader.finish().map_err(FooterReadError::Parquet)?;
            Ok((file_size, metadata))
        }
        Err(ParquetError::NeedMoreData(_)) => {
            warn!(
                target = %table,
                path = %path,
                tail_size = tail_bytes.len(),
                "Footer exceeded prefetch, falling back to full read"
            );
            let full_bytes = storage.get(path).await.map_err(FooterReadError::Storage)?;
            let metadata = ParquetMetaDataReader::new()
                .parse_and_finish(&full_bytes)
                .map_err(FooterReadError::Parquet)?;
            Ok((file_size, metadata))
        }
        Err(e) => Err(FooterReadError::Parquet(e)),
    }
}
