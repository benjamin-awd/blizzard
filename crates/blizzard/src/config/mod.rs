//! Configuration for the blizzard file loader.

pub mod builder;

blizzard_core::define_component_key!(PipelineKey);

use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use blizzard_core::AppConfig;
pub use blizzard_core::config::{
    ConfigPath, ErrorHandlingConfig, InterpolationResult, Mergeable, MetricsConfig,
    ParquetCompression, PartitionByConfig, PartitionFilterConfig, Resource, interpolate,
    load_from_paths,
};
use blizzard_core::config::{
    default_max_concurrent_parts, default_max_concurrent_uploads, default_min_multipart_size_mb,
    default_part_size_mb,
};
use blizzard_core::storage::DatePrefixGenerator;
use blizzard_core::topology::PipelineContext;
pub use blizzard_core::{GlobalConfig, KB, MB};

use blizzard_core::error::ConfigError;

use crate::pipeline::Pipeline;

/// Configuration for the input source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    /// Path to the input directory (supports S3, GCS, Azure, local).
    pub path: String,
    /// Compression format of input files.
    #[serde(default)]
    pub compression: CompressionFormat,
    /// Number of records per batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Poll interval in seconds.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Optional partition filter for efficient listing.
    pub partition_filter: Option<PartitionFilterConfig>,
    /// Storage options for source storage (credentials, region, etc.)
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
    /// Enable watermark-based source tracking.
    ///
    /// When true, uses a persistent high-watermark checkpoint to track processed files.
    /// This replaces the unbounded in-memory HashMap with efficient lexicographic filtering.
    ///
    /// Requirements:
    /// - Source files must be lexicographically sortable (e.g., timestamp prefixes, UUIDv7)
    /// - Checkpoint is stored at `{table_uri}/_blizzard/{pipeline}_checkpoint.json`
    ///
    /// First run performs a full scan; subsequent runs only list files above the watermark.
    #[serde(default)]
    pub use_watermark: bool,

    /// Configuration for incremental checkpoint saves during processing.
    ///
    /// Only applies when `use_watermark` is true.
    #[serde(default)]
    pub checkpoint: CheckpointConfig,
}

fn default_batch_size() -> usize {
    8192
}

fn default_max_concurrent_files() -> usize {
    4
}

fn default_sink_parallelism() -> usize {
    4
}

fn default_poll_interval() -> u64 {
    60
}

fn default_checkpoint_interval_files() -> usize {
    100
}

fn default_checkpoint_interval_secs() -> u64 {
    30
}

/// Configuration for incremental checkpoint saves during iteration processing.
///
/// Checkpoints are saved periodically to prevent progress loss if blizzard crashes
/// while processing a large backlog of files.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointConfig {
    /// Number of files to process before saving a checkpoint.
    #[serde(default = "default_checkpoint_interval_files")]
    pub interval_files: usize,

    /// Maximum seconds between checkpoint saves.
    #[serde(default = "default_checkpoint_interval_secs")]
    pub interval_secs: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval_files: default_checkpoint_interval_files(),
            interval_secs: default_checkpoint_interval_secs(),
        }
    }
}

impl SourceConfig {
    /// Create a new source config with required path and sensible defaults.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            compression: CompressionFormat::default(),
            batch_size: default_batch_size(),
            poll_interval_secs: default_poll_interval(),
            partition_filter: None,
            storage_options: HashMap::new(),
            use_watermark: false,
            checkpoint: CheckpointConfig::default(),
        }
    }

    /// Set the compression format.
    pub fn with_compression(mut self, compression: CompressionFormat) -> Self {
        self.compression = compression;
        self
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Enable watermark-based source tracking.
    pub fn with_watermark(mut self) -> Self {
        self.use_watermark = true;
        self
    }

    /// Generate date prefixes for partition filtering.
    ///
    /// Returns `None` if no partition filter is configured.
    pub fn date_prefixes(&self) -> Option<Vec<String>> {
        self.partition_filter.as_ref().map(|pf| {
            DatePrefixGenerator::new(&pf.prefix_template, pf.lookback).generate_prefixes()
        })
    }
}

/// Compression format of input files.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionFormat {
    #[default]
    Gzip,
    Zstd,
    None,
}

impl CompressionFormat {
    /// Get the compression codec for this format.
    ///
    /// Returns a boxed codec that implements the [`CompressionCodec`] trait,
    /// allowing compression handling to be abstracted away from callers.
    pub fn codec(&self) -> Box<dyn crate::source::CompressionCodec> {
        use crate::source::{GzipCodec, NoopCodec, ZstdCodec};
        match self {
            CompressionFormat::Gzip => Box::new(GzipCodec),
            CompressionFormat::Zstd => Box::new(ZstdCodec),
            CompressionFormat::None => Box::new(NoopCodec),
        }
    }
}

/// Configuration for the sink Delta table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkConfig {
    /// URI of the Delta table (supports S3, GCS, Azure, local).
    /// Parquet files are written directly to {table_uri}/{partition}/
    pub table_uri: String,
    /// Target Parquet file size in MB.
    #[serde(default = "default_file_size_mb")]
    pub file_size_mb: usize,
    /// Row group size in bytes for memory management.
    #[serde(default = "default_row_group_size")]
    pub row_group_size_bytes: usize,
    /// Parquet compression codec.
    #[serde(default)]
    pub compression: ParquetCompression,
    /// Partition configuration with strftime-style prefix template.
    pub partition_by: Option<PartitionByConfig>,
    /// Storage options for table storage (credentials, region, etc.).
    #[serde(default)]
    pub storage_options: HashMap<String, String>,
    /// Maximum concurrent upload operations.
    #[serde(default = "default_max_concurrent_uploads")]
    pub max_concurrent_uploads: usize,
    /// Roll file after it has been open for this many seconds.
    ///
    /// Provides an upper bound on file age, ensuring data is committed regularly
    /// even during continuous high-throughput writes.
    pub rollover_timeout_secs: Option<u64>,
    /// Part size for multipart uploads in MB.
    #[serde(default = "default_part_size_mb")]
    pub part_size_mb: usize,
    /// Minimum file size to use multipart upload in MB.
    #[serde(default = "default_min_multipart_size_mb")]
    pub min_multipart_size_mb: usize,
    /// Maximum concurrent parts per multipart upload.
    #[serde(default = "default_max_concurrent_parts")]
    pub max_concurrent_parts: usize,
}

impl SinkConfig {
    /// Create a new sink config with required table URI and sensible defaults.
    pub fn new(table_uri: impl Into<String>) -> Self {
        Self {
            table_uri: table_uri.into(),
            file_size_mb: default_file_size_mb(),
            row_group_size_bytes: default_row_group_size(),
            compression: ParquetCompression::default(),
            partition_by: None,
            storage_options: HashMap::new(),
            max_concurrent_uploads: default_max_concurrent_uploads(),
            rollover_timeout_secs: None,
            part_size_mb: default_part_size_mb(),
            min_multipart_size_mb: default_min_multipart_size_mb(),
            max_concurrent_parts: default_max_concurrent_parts(),
        }
    }

    /// Set the target file size in MB.
    pub fn with_file_size_mb(mut self, file_size_mb: usize) -> Self {
        self.file_size_mb = file_size_mb;
        self
    }

    /// Set the rollover timeout in seconds.
    pub fn with_rollover_timeout_secs(mut self, secs: u64) -> Self {
        self.rollover_timeout_secs = Some(secs);
        self
    }
}

fn default_file_size_mb() -> usize {
    128
}

fn default_row_group_size() -> usize {
    128 * MB
}

/// Schema field configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldConfig {
    /// Field name.
    pub name: String,
    /// Field type.
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Whether the field is nullable.
    #[serde(default = "default_nullable")]
    pub nullable: bool,
}

fn default_nullable() -> bool {
    true
}

/// Supported field types.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    Timestamp,
    Date,
    Json,
    Binary,
}

impl FieldType {
    /// Convert to Arrow DataType.
    pub fn to_arrow_type(self) -> DataType {
        match self {
            FieldType::String => DataType::Utf8,
            FieldType::Int32 => DataType::Int32,
            FieldType::Int64 => DataType::Int64,
            FieldType::Float32 => DataType::Float32,
            FieldType::Float64 => DataType::Float64,
            FieldType::Boolean => DataType::Boolean,
            FieldType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            FieldType::Date => DataType::Date32,
            FieldType::Json => DataType::Utf8, // JSON stored as string
            FieldType::Binary => DataType::Binary,
        }
    }
}

/// Schema configuration - either explicit fields or inference mode.
///
/// This enum makes invalid states unrepresentable: you either infer the schema
/// (with optional conflict coercion) or provide explicit fields. The two modes
/// cannot be mixed.
#[derive(Debug, Clone)]
pub enum SchemaConfig {
    /// Infer schema from the first source file.
    Infer {
        /// When true, type conflicts during inference are coerced to Utf8.
        coerce_conflicts_to_utf8: bool,
    },
    /// Explicit field definitions.
    Explicit {
        /// List of fields in the schema.
        fields: Vec<FieldConfig>,
    },
}

/// Raw helper struct for deserializing SchemaConfig from YAML while keeping the
/// original config format unchanged.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSchemaConfig {
    #[serde(default)]
    infer: bool,
    #[serde(default)]
    fields: Vec<FieldConfig>,
    #[serde(default)]
    coerce_conflicts_to_utf8: bool,
}

impl<'de> Deserialize<'de> for SchemaConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = RawSchemaConfig::deserialize(deserializer)?;
        let has_fields = !raw.fields.is_empty();

        if raw.infer && has_fields {
            return Err(serde::de::Error::custom(
                "cannot specify both 'infer: true' and 'fields'",
            ));
        }
        if !raw.infer && !has_fields {
            return Err(serde::de::Error::custom(
                "empty schema (specify either 'infer: true' or 'fields')",
            ));
        }

        if raw.infer {
            Ok(SchemaConfig::Infer {
                coerce_conflicts_to_utf8: raw.coerce_conflicts_to_utf8,
            })
        } else {
            Ok(SchemaConfig::Explicit { fields: raw.fields })
        }
    }
}

impl Serialize for SchemaConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        match self {
            SchemaConfig::Infer {
                coerce_conflicts_to_utf8,
            } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("infer", &true)?;
                if *coerce_conflicts_to_utf8 {
                    map.serialize_entry("coerce_conflicts_to_utf8", &true)?;
                }
                map.end()
            }
            SchemaConfig::Explicit { fields } => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("fields", fields)?;
                map.end()
            }
        }
    }
}

impl SchemaConfig {
    /// Create an infer schema config with default options.
    pub fn infer() -> Self {
        SchemaConfig::Infer {
            coerce_conflicts_to_utf8: false,
        }
    }

    /// Create an explicit schema config from field definitions.
    pub fn explicit(fields: Vec<FieldConfig>) -> Self {
        SchemaConfig::Explicit { fields }
    }

    /// Returns true if this config uses schema inference.
    pub fn is_infer(&self) -> bool {
        matches!(self, SchemaConfig::Infer { .. })
    }

    /// Returns true if type conflicts should be coerced to Utf8 during inference.
    pub fn coerce_conflicts_to_utf8(&self) -> bool {
        matches!(
            self,
            SchemaConfig::Infer {
                coerce_conflicts_to_utf8: true,
            }
        )
    }

    /// Convert to Arrow Schema. Returns an error if schema is set to infer mode.
    pub fn to_arrow_schema(&self) -> Result<SchemaRef, ConfigError> {
        match self {
            SchemaConfig::Infer { .. } => Err(ConfigError::Internal {
                message: "Cannot convert infer schema config to Arrow schema".to_string(),
            }),
            SchemaConfig::Explicit { fields } => {
                let arrow_fields: Vec<Field> = fields
                    .iter()
                    .map(|f| Field::new(&f.name, f.field_type.to_arrow_type(), f.nullable))
                    .collect();
                Ok(Arc::new(Schema::new(arrow_fields)))
            }
        }
    }
}

/// Configuration for a single pipeline (source(s) → sink with schema).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    /// Named source configurations.
    pub sources: IndexMap<String, SourceConfig>,
    /// Sink configuration.
    pub sink: SinkConfig,
    /// Schema configuration - either explicit fields or `infer: true`.
    pub schema: SchemaConfig,
    /// Maximum concurrent file downloads across all sources.
    #[serde(default = "default_max_concurrent_files")]
    pub max_concurrent_files: usize,
    /// Number of parallel sink workers for concurrent file writing.
    #[serde(default = "default_sink_parallelism")]
    pub sink_parallelism: usize,
    /// Error handling configuration.
    #[serde(default)]
    pub error_handling: ErrorHandlingConfig,
}

impl PipelineConfig {
    /// Returns exclusive resources used by this pipeline configuration.
    ///
    /// Resources are things that cannot be shared between pipelines running in
    /// the same process. This is used during config validation to detect
    /// conflicts like two pipelines reading from the same source directory
    /// or writing to the same table.
    pub fn resources(&self) -> Vec<Resource> {
        let mut resources: Vec<Resource> = self
            .sources
            .values()
            .map(|s| Resource::directory(s.path.trim_end_matches('/')))
            .collect();
        resources.push(Resource::directory(
            self.sink.table_uri.trim_end_matches('/'),
        ));
        resources
    }
}

/// Main configuration for blizzard.
///
/// # Example
///
/// ```yaml
/// pipelines:
///   events:
///     source:
///       path: "gs://bucket/events-raw"
///       compression: gzip
///     sink:
///       table_uri: "gs://bucket/delta/events"
///     schema:
///       fields:
///         - name: id
///           type: string
///
///   logs:
///     source:
///       path: "gs://bucket/logs-raw"
///     sink:
///       table_uri: "gs://bucket/delta/logs"
///     schema:
///       fields:
///         - name: log_id
///           type: string
///
/// global:
///   total_concurrency: 8
///
/// metrics:
///   enabled: true
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Named pipeline configurations.
    #[serde(default)]
    pub pipelines: IndexMap<PipelineKey, PipelineConfig>,
    /// Global configuration options.
    #[serde(default)]
    pub global: GlobalConfig,
    /// Metrics configuration.
    #[serde(default)]
    pub metrics: MetricsConfig,
}

blizzard_core::impl_mergeable!(Config, PipelineKey, PipelineConfig, pipelines);

impl Config {
    fn validate_config(&self) -> Result<(), ConfigError> {
        let mut errors = Vec::new();

        for (key, pipeline) in &self.pipelines {
            let pipeline_id = key.id();

            if pipeline.sources.is_empty() {
                errors.push(format!("Pipeline '{pipeline_id}': no sources configured"));
            }

            for (source_name, source) in &pipeline.sources {
                if source.path.is_empty() {
                    errors.push(format!(
                        "Pipeline '{pipeline_id}': source '{source_name}' has empty path"
                    ));
                }
            }

            if pipeline.sink.table_uri.is_empty() {
                errors.push(format!("Pipeline '{pipeline_id}': sink.table_uri is empty"));
            }
        }

        let conflicts = Resource::conflicts(
            self.pipelines
                .iter()
                .map(|(key, config)| (key.id().to_string(), config.resources())),
        );
        Resource::extend_errors(&mut errors, conflicts);

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::MultipleErrors { errors })
        }
    }
}

impl AppConfig for Config {
    type Pipeline = Pipeline;

    const COMPONENT_NAME: &'static str = "pipeline";

    fn from_paths(paths: &[ConfigPath]) -> Result<Self, ConfigError> {
        let config: Self = load_from_paths(paths)?;
        config.validate()?;
        Ok(config)
    }

    fn create_pipelines(&self, context: PipelineContext) -> Vec<Self::Pipeline> {
        Pipeline::from_config(self, context)
    }

    fn log_startup_info(&self) {
        let pipeline_count = self.pipelines.len();
        info!("Starting blizzard file loader with {pipeline_count} pipeline(s)");
        for (key, cfg) in &self.pipelines {
            let sink = &cfg.sink.table_uri;
            if let Some(source) = cfg.sources.values().next()
                && cfg.sources.len() == 1
            {
                info!("  Pipeline: {key} ({} -> {sink})", source.path);
            } else {
                let source_names: Vec<_> = cfg.sources.keys().collect();
                info!(
                    "  Pipeline: {key} ({} sources: {:?} -> {sink})",
                    cfg.sources.len(),
                    source_names
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal valid config YAML with explicit fields schema.
    const MINIMAL_YAML: &str = r#"
pipelines:
  events:
    sources:
      default:
        path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields:
        - name: id
          type: string
"#;

    /// Parse YAML and return the first pipeline's config.
    fn parse_first(yaml: &str) -> PipelineConfig {
        Config::parse(yaml).unwrap().into_first().unwrap()
    }

    /// Assert that parsing the given YAML produces an error containing all substrings.
    fn assert_parse_err(yaml: &str, substrings: &[&str]) {
        let err = Config::parse(yaml).unwrap_err().to_string();
        for s in substrings {
            assert!(
                err.contains(s),
                "Expected error to contain '{s}', got: {err}"
            );
        }
    }

    /// Build a two-pipeline YAML config with fields schema for resource conflict tests.
    fn two_pipeline_yaml(path_a: &str, uri_a: &str, path_b: &str, uri_b: &str) -> String {
        format!(
            r#"
pipelines:
  a:
    sources:
      default:
        path: {path_a}
    sink:
      table_uri: {uri_a}
    schema:
      fields:
        - name: id
          type: string
  b:
    sources:
      default:
        path: {path_b}
    sink:
      table_uri: {uri_b}
    schema:
      fields:
        - name: id
          type: string
"#
        )
    }

    /// Builder for single-pipeline test YAML. Defaults match `MINIMAL_YAML`.
    struct TestPipeline {
        source_path: String,
        source_lines: String,
        sink_uri: String,
        sink_lines: String,
        schema: String,
        pipeline_lines: String,
        top_level_lines: String,
    }

    impl Default for TestPipeline {
        fn default() -> Self {
            Self {
                source_path: "gs://bucket/raw".into(),
                source_lines: String::new(),
                sink_uri: "gs://bucket/delta/events".into(),
                sink_lines: String::new(),
                schema: "fields:\n  - name: id\n    type: string".into(),
                pipeline_lines: String::new(),
                top_level_lines: String::new(),
            }
        }
    }

    impl TestPipeline {
        fn source_path(mut self, p: &str) -> Self {
            self.source_path = p.into();
            self
        }
        fn source(mut self, s: &str) -> Self {
            self.source_lines = s.into();
            self
        }
        fn sink_uri(mut self, u: &str) -> Self {
            self.sink_uri = u.into();
            self
        }
        fn sink(mut self, s: &str) -> Self {
            self.sink_lines = s.into();
            self
        }
        fn schema(mut self, s: &str) -> Self {
            self.schema = s.into();
            self
        }
        fn schema_infer(self) -> Self {
            self.schema("infer: true")
        }
        fn pipeline(mut self, s: &str) -> Self {
            self.pipeline_lines = s.into();
            self
        }
        fn top_level(mut self, s: &str) -> Self {
            self.top_level_lines = s.into();
            self
        }

        fn build(&self) -> String {
            let mut y = String::from("pipelines:\n  events:\n    sources:\n      default:\n");
            append_indented(&mut y, &format!("path: {}", self.source_path), 8);
            if !self.source_lines.is_empty() {
                append_indented(&mut y, &self.source_lines, 8);
            }
            y.push_str("    sink:\n");
            append_indented(&mut y, &format!("table_uri: {}", self.sink_uri), 6);
            if !self.sink_lines.is_empty() {
                append_indented(&mut y, &self.sink_lines, 6);
            }
            y.push_str("    schema:\n");
            append_indented(&mut y, &self.schema, 6);
            if !self.pipeline_lines.is_empty() {
                append_indented(&mut y, &self.pipeline_lines, 4);
            }
            if !self.top_level_lines.is_empty() {
                append_indented(&mut y, &self.top_level_lines, 0);
            }
            y
        }
    }

    /// Append `text` to `out`, adding `spaces` leading spaces to each line.
    /// Auto-dedents: strips the common leading whitespace from all non-empty lines,
    /// so raw strings can be indented naturally in source code.
    fn append_indented(out: &mut String, text: &str, spaces: usize) {
        let all_lines: Vec<&str> = text.lines().collect();
        let start = all_lines
            .iter()
            .position(|l| !l.trim().is_empty())
            .unwrap_or(0);
        let end = all_lines
            .iter()
            .rposition(|l| !l.trim().is_empty())
            .map_or(0, |i| i + 1);
        let lines = &all_lines[start..end];
        let min_indent = lines
            .iter()
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.len() - l.trim_start().len())
            .min()
            .unwrap_or(0);
        let prefix = " ".repeat(spaces);
        for line in lines {
            let stripped = if line.len() > min_indent {
                &line[min_indent..]
            } else {
                line.trim()
            };
            out.push_str(&prefix);
            out.push_str(stripped);
            out.push('\n');
        }
    }

    #[test]
    fn test_single_pipeline_parse() {
        let yaml = TestPipeline::default()
            .source_path("gs://bucket/raw-data")
            .source("compression: gzip")
            .sink("file_size_mb: 128")
            .build();
        let config = Config::parse(&yaml).unwrap();
        assert_eq!(config.pipelines.len(), 1);

        let (key, pipeline) = config.pipelines.iter().next().unwrap();
        assert_eq!(key.id(), "events");
        assert_eq!(
            pipeline.sources.get("default").unwrap().path,
            "gs://bucket/raw-data"
        );
        assert_eq!(pipeline.sink.table_uri, "gs://bucket/delta/events");
    }

    #[test]
    fn test_multi_source_pipeline_parse() {
        let yaml = r#"
pipelines:
  events:
    sources:
      asia:
        path: gs://bucket/region=asia-northeast1/events
        compression: gzip
        use_watermark: true
      europe:
        path: gs://bucket/region=europe-west1/events
        compression: zstd
        use_watermark: true
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      infer: true
"#;
        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.pipelines.len(), 1);

        let (key, pipeline) = config.pipelines.iter().next().unwrap();
        assert_eq!(key.id(), "events");
        assert_eq!(pipeline.sources.len(), 2);

        let asia = pipeline.sources.get("asia").unwrap();
        assert_eq!(asia.path, "gs://bucket/region=asia-northeast1/events");
        assert!(matches!(asia.compression, CompressionFormat::Gzip));
        assert!(asia.use_watermark);

        let europe = pipeline.sources.get("europe").unwrap();
        assert_eq!(europe.path, "gs://bucket/region=europe-west1/events");
        assert!(matches!(europe.compression, CompressionFormat::Zstd));
        assert!(europe.use_watermark);
    }

    #[test]
    fn test_multi_pipeline_parse() {
        let yaml = r#"
pipelines:
  events:
    sources:
      default:
        path: gs://bucket/events-raw
        compression: gzip
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields:
        - name: id
          type: string

  logs:
    sources:
      default:
        path: gs://bucket/logs-raw
    sink:
      table_uri: gs://bucket/delta/logs
    schema:
      fields:
        - name: log_id
          type: string

global:
  total_concurrency: 8
"#;
        let config = Config::parse(yaml).unwrap();
        assert_eq!(config.pipelines.len(), 2);
        assert_eq!(config.global.total_concurrency, Some(8));

        let pipelines: Vec<_> = config.pipelines.iter().collect();
        assert_eq!(pipelines[0].0.id(), "events");
        assert_eq!(
            pipelines[0].1.sources.get("default").unwrap().path,
            "gs://bucket/events-raw"
        );
        assert_eq!(pipelines[1].0.id(), "logs");
        assert_eq!(
            pipelines[1].1.sources.get("default").unwrap().path,
            "gs://bucket/logs-raw"
        );
    }

    #[test]
    fn test_empty_source_error() {
        let yaml = r#"
pipelines:
  events:
    sources:
      default:
        path: ""
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields:
        - name: id
          type: string
"#;
        assert_parse_err(yaml, &["events", "empty path"]);
    }

    #[test]
    fn test_no_sources_error() {
        let yaml = r#"
pipelines:
  events:
    sources: {}
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields:
        - name: id
          type: string
"#;
        assert_parse_err(yaml, &["events", "no sources configured"]);
    }

    #[test]
    fn test_empty_sink_error() {
        let yaml = r#"
pipelines:
  events:
    sources:
      default:
        path: gs://bucket/raw
    sink:
      table_uri: ""
    schema:
      fields:
        - name: id
          type: string
"#;
        assert_parse_err(yaml, &["events", "sink.table_uri is empty"]);
    }

    #[test]
    fn test_empty_schema_error() {
        let yaml = r#"
pipelines:
  events:
    sources:
      default:
        path: gs://bucket/raw
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      fields: []
"#;
        assert_parse_err(yaml, &["events", "empty schema"]);
    }

    #[test]
    fn test_metrics_default() {
        let config = Config::parse(MINIMAL_YAML).unwrap();
        assert_eq!(config.metrics.address, "0.0.0.0:9090");
    }

    #[test]
    fn test_global_default() {
        let config = Config::parse(MINIMAL_YAML).unwrap();
        assert_eq!(config.global.total_concurrency, None);
    }

    #[test]
    fn test_source_config_defaults() {
        let pipeline = parse_first(MINIMAL_YAML);
        let source = pipeline.sources.get("default").unwrap();
        assert_eq!(source.batch_size, 8192);
        assert_eq!(source.poll_interval_secs, 60);
        assert!(source.partition_filter.is_none());
        assert_eq!(pipeline.max_concurrent_files, 4);
    }

    #[test]
    fn test_sink_config_defaults() {
        let pipeline = parse_first(MINIMAL_YAML);
        assert_eq!(pipeline.sink.file_size_mb, 128);
        assert_eq!(pipeline.sink.row_group_size_bytes, 128 * MB);
        assert!(pipeline.sink.partition_by.is_none());
        assert!(pipeline.sink.rollover_timeout_secs.is_none());
    }

    #[test]
    fn test_sink_config_rollover_timeout() {
        let yaml = TestPipeline::default()
            .sink("rollover_timeout_secs: 300")
            .build();
        assert_eq!(parse_first(&yaml).sink.rollover_timeout_secs, Some(300));
    }

    #[test]
    fn test_pipeline_config_resources() {
        let yaml = TestPipeline::default()
            .source_path("gs://bucket/raw-events")
            .build();
        let resources = parse_first(&yaml).resources();
        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0], Resource::directory("gs://bucket/raw-events"));
        assert_eq!(
            resources[1],
            Resource::directory("gs://bucket/delta/events")
        );
    }

    #[test]
    fn test_pipeline_config_resources_multi_source() {
        let yaml = r#"
pipelines:
  events:
    sources:
      asia:
        path: gs://bucket/asia/events
      europe:
        path: gs://bucket/europe/events
    sink:
      table_uri: gs://bucket/delta/events
    schema:
      infer: true
"#;
        let resources = parse_first(yaml).resources();
        assert_eq!(resources.len(), 3);
        assert!(resources.contains(&Resource::directory("gs://bucket/asia/events")));
        assert!(resources.contains(&Resource::directory("gs://bucket/europe/events")));
        assert!(resources.contains(&Resource::directory("gs://bucket/delta/events")));
    }

    #[test]
    fn test_pipeline_config_resources_trailing_slash() {
        let yaml = TestPipeline::default()
            .source_path("gs://bucket/raw-events/")
            .sink_uri("gs://bucket/delta/events/")
            .build();
        let resources = parse_first(&yaml).resources();
        assert_eq!(resources[0], Resource::directory("gs://bucket/raw-events"));
        assert_eq!(
            resources[1],
            Resource::directory("gs://bucket/delta/events")
        );
    }

    #[test]
    fn test_resource_conflict_same_source() {
        let yaml = two_pipeline_yaml(
            "gs://bucket/raw/same",
            "gs://bucket/delta/a",
            "gs://bucket/raw/same",
            "gs://bucket/delta/b",
        );
        assert_parse_err(&yaml, &["Resource conflict", "gs://bucket/raw/same"]);
    }

    #[test]
    fn test_resource_conflict_same_sink() {
        let yaml = two_pipeline_yaml(
            "gs://bucket/raw/a",
            "gs://bucket/delta/same",
            "gs://bucket/raw/b",
            "gs://bucket/delta/same",
        );
        assert_parse_err(&yaml, &["Resource conflict", "gs://bucket/delta/same"]);
    }

    #[test]
    fn test_resource_conflict_trailing_slash_normalization() {
        let yaml = two_pipeline_yaml(
            "gs://bucket/raw/same",
            "gs://bucket/delta/a",
            "gs://bucket/raw/same/",
            "gs://bucket/delta/b",
        );
        assert_parse_err(&yaml, &["Resource conflict"]);
    }

    #[test]
    fn test_no_resource_conflict_different_paths() {
        let yaml = two_pipeline_yaml(
            "gs://bucket/raw/a",
            "gs://bucket/delta/a",
            "gs://bucket/raw/b",
            "gs://bucket/delta/b",
        );
        assert!(Config::parse(&yaml).is_ok());
    }

    #[test]
    fn test_infer_schema_valid() {
        let yaml = TestPipeline::default().schema_infer().build();
        assert!(parse_first(&yaml).schema.is_infer());
    }

    #[test]
    fn test_infer_schema_false_error() {
        let yaml = TestPipeline::default().schema("infer: false").build();
        assert_parse_err(&yaml, &["events", "empty schema"]);
    }

    #[test]
    fn test_schema_both_infer_and_fields_error() {
        let yaml = TestPipeline::default()
            .schema(
                r#"
                infer: true
                fields:
                  - name: id
                    type: string
            "#,
            )
            .build();
        assert_parse_err(&yaml, &["cannot specify both"]);
    }

    #[test]
    fn test_partition_by_config_partition_columns() {
        let config = PartitionByConfig {
            prefix_template: "date=%Y-%m-%d/hour=%H".to_string(),
        };
        assert_eq!(config.partition_columns(), vec!["date", "hour"]);
    }

    #[test]
    fn test_partition_by_config_single_column() {
        let config = PartitionByConfig {
            prefix_template: "date=%Y-%m-%d".to_string(),
        };
        assert_eq!(config.partition_columns(), vec!["date"]);
    }

    #[test]
    fn test_partition_by_config_empty_template() {
        let config = PartitionByConfig {
            prefix_template: String::new(),
        };
        assert!(config.partition_columns().is_empty());
    }

    #[test]
    fn test_partition_by_config_yaml_parsing() {
        let yaml = TestPipeline::default()
            .sink(
                r#"
                partition_by:
                  prefix_template: "date=%Y-%m-%d"
            "#,
            )
            .build();
        let pipeline = parse_first(&yaml);
        let partition_by = pipeline.sink.partition_by.as_ref().unwrap();
        assert_eq!(partition_by.prefix_template, "date=%Y-%m-%d");
        assert_eq!(partition_by.partition_columns(), vec!["date"]);
    }

    #[test]
    fn test_unknown_field_rejected_in_source() {
        let yaml = TestPipeline::default()
            .source("batchsize: 100")
            .schema_infer()
            .build();
        assert_parse_err(&yaml, &["unknown field"]);
    }

    #[test]
    fn test_unknown_field_rejected_in_sink() {
        let yaml = TestPipeline::default()
            .sink("filesize: 128")
            .schema_infer()
            .build();
        assert_parse_err(&yaml, &["unknown field"]);
    }

    #[test]
    fn test_unknown_field_rejected_in_pipeline() {
        let yaml = TestPipeline::default()
            .schema_infer()
            .pipeline("unknown_key: value")
            .build();
        assert_parse_err(&yaml, &["unknown field"]);
    }

    #[test]
    fn test_unknown_field_rejected_at_top_level() {
        let yaml = TestPipeline::default()
            .schema_infer()
            .top_level("unknown_top_level: value")
            .build();
        assert_parse_err(&yaml, &["unknown field"]);
    }

    #[test]
    fn test_multiple_errors_collected() {
        let yaml = r#"
pipelines:
  a:
    sources:
      default:
        path: ""
    sink:
      table_uri: ""
    schema:
      infer: true
  b:
    sources:
      default:
        path: ""
    sink:
      table_uri: gs://bucket/b
    schema:
      infer: true
"#;
        assert_parse_err(yaml, &["Pipeline 'a'", "Pipeline 'b'", "empty path"]);
    }

    #[test]
    fn test_schema_infer_and_fields_error_at_parse_time() {
        let yaml = TestPipeline::default()
            .schema(
                r#"
                infer: true
                fields:
                  - name: id
                    type: string
            "#,
            )
            .build();
        assert_parse_err(&yaml, &["cannot specify both"]);
    }

    #[test]
    fn test_schema_empty_error_at_parse_time() {
        let yaml = TestPipeline::default().schema("infer: false").build();
        assert_parse_err(&yaml, &["empty schema"]);
    }

    #[test]
    fn test_use_watermark_default_false() {
        let source = parse_first(MINIMAL_YAML)
            .sources
            .into_values()
            .next()
            .unwrap();
        assert!(!source.use_watermark);
    }

    #[test]
    fn test_use_watermark_enabled() {
        let yaml = TestPipeline::default()
            .source(
                r#"
                use_watermark: true
                partition_filter:
                  prefix_template: "date=%Y-%m-%d"
                  lookback: 2
            "#,
            )
            .build();
        let source = parse_first(&yaml).sources.into_values().next().unwrap();
        assert!(source.use_watermark);
        assert!(source.partition_filter.is_some());
    }

    #[test]
    fn test_checkpoint_config_defaults() {
        let yaml = TestPipeline::default()
            .source("use_watermark: true")
            .build();
        let source = parse_first(&yaml).sources.into_values().next().unwrap();
        assert_eq!(source.checkpoint.interval_files, 100);
        assert_eq!(source.checkpoint.interval_secs, 30);
    }

    #[test]
    fn test_checkpoint_config_custom() {
        let yaml = TestPipeline::default()
            .source(
                r#"
                use_watermark: true
                checkpoint:
                  interval_files: 50
                  interval_secs: 15
            "#,
            )
            .build();
        let source = parse_first(&yaml).sources.into_values().next().unwrap();
        assert_eq!(source.checkpoint.interval_files, 50);
        assert_eq!(source.checkpoint.interval_secs, 15);
    }

    #[test]
    fn test_checkpoint_config_partial() {
        let yaml = TestPipeline::default()
            .source(
                r#"
                use_watermark: true
                checkpoint:
                  interval_files: 200
            "#,
            )
            .build();
        let source = parse_first(&yaml).sources.into_values().next().unwrap();
        assert_eq!(source.checkpoint.interval_files, 200);
        assert_eq!(source.checkpoint.interval_secs, 30);
    }

    #[test]
    fn test_schema_config_infer_round_trip() {
        let schema = SchemaConfig::Infer {
            coerce_conflicts_to_utf8: true,
        };
        let yaml = serde_yaml::to_string(&schema).unwrap();
        let restored: SchemaConfig = serde_yaml::from_str(&yaml).unwrap();

        assert!(restored.is_infer());
        assert!(restored.coerce_conflicts_to_utf8());
    }

    #[test]
    fn test_schema_config_explicit_round_trip() {
        let schema = SchemaConfig::explicit(vec![
            FieldConfig {
                name: "id".to_string(),
                field_type: FieldType::String,
                nullable: false,
            },
            FieldConfig {
                name: "value".to_string(),
                field_type: FieldType::Int64,
                nullable: true,
            },
        ]);
        let yaml = serde_yaml::to_string(&schema).unwrap();
        let restored: SchemaConfig = serde_yaml::from_str(&yaml).unwrap();

        assert!(!restored.is_infer());
        let arrow_schema = restored.to_arrow_schema().unwrap();
        assert_eq!(arrow_schema.fields().len(), 2);
    }

    #[test]
    fn test_schema_config_constructors() {
        let infer = SchemaConfig::infer();
        assert!(infer.is_infer());
        assert!(!infer.coerce_conflicts_to_utf8());

        let explicit = SchemaConfig::explicit(vec![FieldConfig {
            name: "id".to_string(),
            field_type: FieldType::String,
            nullable: false,
        }]);
        assert!(!explicit.is_infer());
        assert!(!explicit.coerce_conflicts_to_utf8());
    }
}
