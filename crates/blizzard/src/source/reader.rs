//! NDJSON.gz async reader.
//!
//! Reads gzip-compressed newline-delimited JSON files and converts them
//! to Arrow RecordBatches using a user-provided schema.

use bytes::Bytes;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;
use deltalake::arrow::json::ReaderBuilder;
use snafu::prelude::*;
use std::io::{BufRead, BufReader, Cursor};
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

use crate::config::CompressionFormat;
use crate::error::{DecoderBuildSnafu, JsonDecodeSnafu, ReaderError, ZstdDecompressionSnafu};
use blizzard_common::emit;
use blizzard_common::metrics::events::{BytesRead, FileDecompressionCompleted};

/// Configuration for the NDJSON reader.
#[derive(Debug, Clone)]
pub struct NdjsonReaderConfig {
    /// Number of records per batch.
    pub batch_size: usize,
    /// Compression format of input files.
    pub compression: CompressionFormat,
}

impl NdjsonReaderConfig {
    /// Create a new reader configuration.
    pub fn new(batch_size: usize, compression: CompressionFormat) -> Self {
        Self {
            batch_size,
            compression,
        }
    }
}

/// Result of reading and parsing a file.
#[derive(Debug)]
pub struct ReadResult {
    /// Parsed record batches.
    pub batches: Vec<RecordBatch>,
    /// Total number of records read.
    pub total_records: usize,
}

/// A reader for NDJSON.gz files that yields Arrow RecordBatches.
pub struct NdjsonReader {
    schema: SchemaRef,
    config: NdjsonReaderConfig,
    /// Pipeline identifier for metrics labeling.
    pipeline: String,
}

impl NdjsonReader {
    /// Create a new NDJSON reader with the given schema and configuration.
    pub fn new(schema: SchemaRef, config: NdjsonReaderConfig, pipeline: String) -> Self {
        Self {
            schema,
            config,
            pipeline,
        }
    }

    /// Read compressed data and parse it into record batches.
    ///
    /// This method uses streaming decompression to avoid loading the entire
    /// decompressed file into memory:
    /// 1. Creates a streaming decompressor based on the configured compression format
    /// 2. Parses NDJSON content into Arrow RecordBatches as data is decompressed
    /// 3. Optionally skips a number of records (for checkpoint recovery)
    ///
    /// # Arguments
    /// * `compressed` - The compressed file data
    /// * `skip_records` - Number of records to skip from the beginning (for resuming)
    /// * `path` - File path (used for error messages and logging)
    pub fn read(
        &self,
        compressed: Bytes,
        skip_records: usize,
        path: &str,
    ) -> Result<ReadResult, ReaderError> {
        // Emit bytes read metric
        emit!(BytesRead {
            bytes: compressed.len() as u64,
            target: self.pipeline.clone(),
        });

        let start = Instant::now();
        let compressed_len = compressed.len();

        // Create a streaming reader based on compression format.
        // This avoids loading the entire decompressed file into memory.
        let reader: Box<dyn BufRead> = match self.config.compression {
            CompressionFormat::Gzip => Box::new(BufReader::new(flate2::read::GzDecoder::new(
                &compressed[..],
            ))),
            CompressionFormat::Zstd => Box::new(BufReader::new(
                zstd::stream::Decoder::new(&compressed[..]).context(ZstdDecompressionSnafu {
                    path: path.to_string(),
                })?,
            )),
            CompressionFormat::None => Box::new(Cursor::new(compressed)),
        };

        // Build streaming JSON reader that processes data as it's decompressed
        let json_reader = ReaderBuilder::new(Arc::clone(&self.schema))
            .with_batch_size(self.config.batch_size)
            .with_strict_mode(false)
            .build(reader)
            .map_err(|e| {
                DecoderBuildSnafu {
                    message: e.to_string(),
                }
                .build()
            })?;

        let mut batches = Vec::new();
        let mut total_records = 0;
        let mut records_to_skip = skip_records;

        // Stream through batches as they're produced
        for batch_result in json_reader {
            let batch = batch_result.map_err(|e| {
                JsonDecodeSnafu {
                    path: path.to_string(),
                    message: e.to_string(),
                }
                .build()
            })?;

            // Apply skip logic for checkpoint recovery
            if records_to_skip > 0 {
                let batch_rows = batch.num_rows();
                if records_to_skip >= batch_rows {
                    records_to_skip -= batch_rows;
                    continue;
                } else {
                    // Partial skip
                    let skip = records_to_skip;
                    records_to_skip = 0;
                    let sliced = batch.slice(skip, batch_rows - skip);
                    total_records += sliced.num_rows();
                    batches.push(sliced);
                }
            } else {
                total_records += batch.num_rows();
                batches.push(batch);
            }
        }

        emit!(FileDecompressionCompleted {
            duration: start.elapsed(),
            target: self.pipeline.clone(),
        });

        debug!(
            "Streamed and parsed {} bytes -> {} batches ({} records) from {}",
            compressed_len,
            batches.len(),
            total_records,
            path
        );

        Ok(ReadResult {
            batches,
            total_records,
        })
    }
}
