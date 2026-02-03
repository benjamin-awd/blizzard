//! Compression codec abstraction for extensible decompression.
//!
//! Provides a trait-based approach to handling different compression formats,
//! allowing new formats to be added without modifying existing code.

use std::io::{BufRead, BufReader, Cursor, Read};

use bytes::Bytes;

/// Error type for decompression operations.
#[derive(Debug)]
pub struct DecompressionError {
    /// Description of the error.
    pub message: String,
}

impl std::fmt::Display for DecompressionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for DecompressionError {}

impl From<std::io::Error> for DecompressionError {
    fn from(e: std::io::Error) -> Self {
        Self {
            message: e.to_string(),
        }
    }
}

/// Trait for compression codecs that can decompress data.
///
/// Implementations provide both streaming and full decompression methods,
/// allowing callers to choose the most appropriate approach for their use case.
///
/// # Example
///
/// ```ignore
/// let codec = GzipCodec;
/// let reader = codec.create_reader(&compressed_data)?;
/// // Read from the streaming reader...
/// ```
pub trait CompressionCodec: Send + Sync {
    /// Create a streaming reader that decompresses data on-the-fly.
    ///
    /// This is preferred for large files as it avoids loading the entire
    /// decompressed content into memory.
    ///
    /// # Arguments
    /// * `data` - The compressed data to decompress
    ///
    /// # Returns
    /// A boxed `BufRead` that yields decompressed data
    fn create_reader<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<Box<dyn BufRead + Send + 'a>, DecompressionError>;

    /// Decompress data fully into memory.
    ///
    /// Use this for small files or when the full decompressed content is needed
    /// (e.g., for schema inference which needs random access).
    ///
    /// # Arguments
    /// * `data` - The compressed data to decompress
    ///
    /// # Returns
    /// The fully decompressed data as a `Vec<u8>`
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, DecompressionError>;

    /// Human-readable name of this codec (for logging/debugging).
    fn name(&self) -> &'static str;
}

/// Gzip compression codec using flate2.
#[derive(Debug, Clone, Copy, Default)]
pub struct GzipCodec;

impl CompressionCodec for GzipCodec {
    fn create_reader<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<Box<dyn BufRead + Send + 'a>, DecompressionError> {
        Ok(Box::new(BufReader::new(flate2::read::GzDecoder::new(data))))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, DecompressionError> {
        let mut decoder = flate2::read::GzDecoder::new(data);
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf)?;
        Ok(buf)
    }

    fn name(&self) -> &'static str {
        "gzip"
    }
}

/// Zstandard compression codec using zstd.
#[derive(Debug, Clone, Copy, Default)]
pub struct ZstdCodec;

impl CompressionCodec for ZstdCodec {
    fn create_reader<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<Box<dyn BufRead + Send + 'a>, DecompressionError> {
        let decoder = zstd::stream::Decoder::new(data).map_err(|e| DecompressionError {
            message: format!("Failed to create zstd decoder: {e}"),
        })?;
        Ok(Box::new(BufReader::new(decoder)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, DecompressionError> {
        let mut decoder = zstd::stream::Decoder::new(data).map_err(|e| DecompressionError {
            message: format!("Failed to create zstd decoder: {e}"),
        })?;
        let mut buf = Vec::new();
        decoder.read_to_end(&mut buf)?;
        Ok(buf)
    }

    fn name(&self) -> &'static str {
        "zstd"
    }
}

/// No-op codec for uncompressed data.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopCodec;

impl CompressionCodec for NoopCodec {
    fn create_reader<'a>(
        &self,
        data: &'a [u8],
    ) -> Result<Box<dyn BufRead + Send + 'a>, DecompressionError> {
        Ok(Box::new(Cursor::new(data)))
    }

    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, DecompressionError> {
        Ok(data.to_vec())
    }

    fn name(&self) -> &'static str {
        "none"
    }
}

/// Extension trait for creating codecs from Bytes.
///
/// This provides convenience methods that work directly with `Bytes`,
/// which is the common input type in the pipeline.
pub trait CompressionCodecExt: CompressionCodec {
    /// Create a streaming reader from Bytes.
    fn create_reader_from_bytes<'a>(
        &self,
        data: &'a Bytes,
    ) -> Result<Box<dyn BufRead + Send + 'a>, DecompressionError> {
        self.create_reader(data.as_ref())
    }

    /// Decompress Bytes fully into memory.
    fn decompress_bytes(&self, data: &Bytes) -> Result<Vec<u8>, DecompressionError> {
        self.decompress(data.as_ref())
    }
}

// Blanket implementation for all CompressionCodec types
impl<T: CompressionCodec + ?Sized> CompressionCodecExt for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const TEST_DATA: &[u8] = b"Hello, World!\nThis is a test.\n";

    fn make_gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn make_zstd(data: &[u8]) -> Vec<u8> {
        zstd::encode_all(data, 3).unwrap()
    }

    #[test]
    fn test_gzip_codec_streaming() {
        let compressed = make_gzip(TEST_DATA);
        let codec = GzipCodec;

        let mut reader = codec.create_reader(&compressed).unwrap();
        let mut result = String::new();
        reader.read_to_string(&mut result).unwrap();

        assert_eq!(result.as_bytes(), TEST_DATA);
    }

    #[test]
    fn test_gzip_codec_full() {
        let compressed = make_gzip(TEST_DATA);
        let codec = GzipCodec;

        let result = codec.decompress(&compressed).unwrap();

        assert_eq!(result, TEST_DATA);
    }

    #[test]
    fn test_zstd_codec_streaming() {
        let compressed = make_zstd(TEST_DATA);
        let codec = ZstdCodec;

        let mut reader = codec.create_reader(&compressed).unwrap();
        let mut result = String::new();
        reader.read_to_string(&mut result).unwrap();

        assert_eq!(result.as_bytes(), TEST_DATA);
    }

    #[test]
    fn test_zstd_codec_full() {
        let compressed = make_zstd(TEST_DATA);
        let codec = ZstdCodec;

        let result = codec.decompress(&compressed).unwrap();

        assert_eq!(result, TEST_DATA);
    }

    #[test]
    fn test_noop_codec_streaming() {
        let codec = NoopCodec;

        let mut reader = codec.create_reader(TEST_DATA).unwrap();
        let mut result = String::new();
        reader.read_to_string(&mut result).unwrap();

        assert_eq!(result.as_bytes(), TEST_DATA);
    }

    #[test]
    fn test_noop_codec_full() {
        let codec = NoopCodec;

        let result = codec.decompress(TEST_DATA).unwrap();

        assert_eq!(result, TEST_DATA);
    }

    #[test]
    fn test_codec_names() {
        assert_eq!(GzipCodec.name(), "gzip");
        assert_eq!(ZstdCodec.name(), "zstd");
        assert_eq!(NoopCodec.name(), "none");
    }

    #[test]
    fn test_bytes_extension() {
        let compressed = Bytes::from(make_gzip(TEST_DATA));
        let codec = GzipCodec;

        let result = codec.decompress_bytes(&compressed).unwrap();

        assert_eq!(result, TEST_DATA);
    }
}
