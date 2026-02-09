//! Typestate builder for [`PipelineConfig`].
//!
//! Enforces "at least one source" at compile time via the [`NeedsSources`] /
//! [`HasSources`] marker types.
//!
//! # Example
//!
//! ```ignore
//! let config = PipelineConfigBuilder::new(sink, schema)
//!     .source("default", source)
//!     .with_max_concurrent_files(8)
//!     .build();
//! ```

use indexmap::IndexMap;
use std::marker::PhantomData;

use super::{
    ErrorHandlingConfig, PipelineConfig, SchemaConfig, SinkConfig, SourceConfig,
    default_max_concurrent_files, default_sink_parallelism,
};

/// Marker: builder needs at least one source before it can build.
pub struct NeedsSources;

/// Marker: builder has at least one source and can build.
pub struct HasSources;

/// Typestate builder for [`PipelineConfig`].
///
/// Starts in [`NeedsSources`] state. Calling [`.source()`](PipelineConfigBuilder::source)
/// transitions to [`HasSources`], where [`.build()`](PipelineConfigBuilder::build) becomes available.
pub struct PipelineConfigBuilder<State = NeedsSources> {
    sources: IndexMap<String, SourceConfig>,
    sink: SinkConfig,
    schema: SchemaConfig,
    max_concurrent_files: usize,
    sink_parallelism: usize,
    error_handling: ErrorHandlingConfig,
    _state: PhantomData<State>,
}

impl PipelineConfigBuilder<NeedsSources> {
    /// Create a new builder with the required sink and schema.
    pub fn new(sink: SinkConfig, schema: SchemaConfig) -> Self {
        Self {
            sources: IndexMap::new(),
            sink,
            schema,
            max_concurrent_files: default_max_concurrent_files(),
            sink_parallelism: default_sink_parallelism(),
            error_handling: ErrorHandlingConfig::default(),
            _state: PhantomData,
        }
    }

    /// Add a named source, transitioning to [`HasSources`] state.
    pub fn source(
        mut self,
        name: impl Into<String>,
        source: SourceConfig,
    ) -> PipelineConfigBuilder<HasSources> {
        self.sources.insert(name.into(), source);
        PipelineConfigBuilder {
            sources: self.sources,
            sink: self.sink,
            schema: self.schema,
            max_concurrent_files: self.max_concurrent_files,
            sink_parallelism: self.sink_parallelism,
            error_handling: self.error_handling,
            _state: PhantomData,
        }
    }
}

impl PipelineConfigBuilder<HasSources> {
    /// Add another named source (stays in [`HasSources`] state).
    pub fn source(mut self, name: impl Into<String>, source: SourceConfig) -> Self {
        self.sources.insert(name.into(), source);
        self
    }

    /// Build the [`PipelineConfig`].
    pub fn build(self) -> PipelineConfig {
        PipelineConfig {
            sources: self.sources,
            sink: self.sink,
            schema: self.schema,
            max_concurrent_files: self.max_concurrent_files,
            sink_parallelism: self.sink_parallelism,
            error_handling: self.error_handling,
        }
    }
}

/// Optional setters available in both states.
macro_rules! impl_optional_setters {
    ($state:ty) => {
        impl PipelineConfigBuilder<$state> {
            /// Set the maximum concurrent file downloads.
            pub fn with_max_concurrent_files(mut self, n: usize) -> Self {
                self.max_concurrent_files = n;
                self
            }

            /// Set the number of parallel sink workers.
            pub fn with_sink_parallelism(mut self, n: usize) -> Self {
                self.sink_parallelism = n;
                self
            }

            /// Set the error handling configuration.
            pub fn with_error_handling(mut self, config: ErrorHandlingConfig) -> Self {
                self.error_handling = config;
                self
            }
        }
    };
}

impl_optional_setters!(NeedsSources);
impl_optional_setters!(HasSources);

#[cfg(test)]
mod tests {
    use super::*;

    fn test_source() -> SourceConfig {
        SourceConfig::new("gs://bucket/raw")
    }

    fn test_sink() -> SinkConfig {
        SinkConfig::new("gs://bucket/delta/events")
    }

    #[test]
    fn test_builder_single_source() {
        let config = PipelineConfigBuilder::new(test_sink(), SchemaConfig::infer())
            .source("default", test_source())
            .build();

        assert_eq!(config.sources.len(), 1);
        assert!(config.sources.contains_key("default"));
        assert!(config.schema.is_infer());
        assert_eq!(config.sink.table_uri, "gs://bucket/delta/events");
    }

    #[test]
    fn test_builder_multiple_sources() {
        let config = PipelineConfigBuilder::new(test_sink(), SchemaConfig::infer())
            .source("asia", SourceConfig::new("gs://bucket/asia"))
            .source("europe", SourceConfig::new("gs://bucket/europe"))
            .build();

        assert_eq!(config.sources.len(), 2);
        assert!(config.sources.contains_key("asia"));
        assert!(config.sources.contains_key("europe"));
    }

    #[test]
    fn test_builder_optional_setters() {
        let config = PipelineConfigBuilder::new(test_sink(), SchemaConfig::infer())
            .with_max_concurrent_files(16)
            .source("default", test_source())
            .with_sink_parallelism(8)
            .build();

        assert_eq!(config.max_concurrent_files, 16);
        assert_eq!(config.sink_parallelism, 8);
    }

    #[test]
    fn test_builder_defaults() {
        let config = PipelineConfigBuilder::new(test_sink(), SchemaConfig::infer())
            .source("default", test_source())
            .build();

        assert_eq!(config.max_concurrent_files, 4);
        assert_eq!(config.sink_parallelism, 4);
    }

    #[test]
    fn test_builder_with_explicit_schema() {
        use crate::config::FieldConfig;
        use crate::config::FieldType;

        let fields = vec![FieldConfig {
            name: "id".to_string(),
            field_type: FieldType::String,
            nullable: false,
        }];
        let config = PipelineConfigBuilder::new(test_sink(), SchemaConfig::explicit(fields))
            .source("default", test_source())
            .build();

        assert!(!config.schema.is_infer());
        let arrow_schema = config.schema.to_arrow_schema().unwrap();
        assert_eq!(arrow_schema.fields().len(), 1);
    }

    #[test]
    fn test_builder_serde_round_trip() {
        let config = PipelineConfigBuilder::new(test_sink(), SchemaConfig::infer())
            .source("default", test_source())
            .with_max_concurrent_files(8)
            .build();

        // Serialize to YAML and back
        let yaml = serde_yaml::to_string(&config).unwrap();
        let deserialized: PipelineConfig = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(deserialized.sources.len(), 1);
        assert!(deserialized.schema.is_infer());
        assert_eq!(deserialized.max_concurrent_files, 8);
    }
}
