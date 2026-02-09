//! Multi-file configuration loading.

use std::path::Path;

use indexmap::IndexMap;

use serde::de::DeserializeOwned;

use crate::config::{ConfigPath, GlobalConfig, MetricsConfig, interpolate, is_yaml_file};
use crate::error::ConfigError;
use crate::topology::PipelineContext;

/// Implement the boilerplate accessors for [`Mergeable`].
///
/// Generates the `Mergeable` trait impl and `Default` impl for a config struct
/// that has `global: GlobalConfig`, `metrics: MetricsConfig`, and a components
/// field of type `IndexMap<K, C>`. The `validate()` trait method delegates to
/// an inherent `validate_config(&self)` method that you must provide.
///
/// # Usage
///
/// ```ignore
/// blizzard_core::impl_mergeable!(Config, PipelineKey, PipelineConfig, pipelines);
/// ```
#[macro_export]
macro_rules! impl_mergeable {
    ($config:ty, $key:ty, $component:ty, $field:ident) => {
        impl Default for $config {
            fn default() -> Self {
                Self {
                    $field: ::indexmap::IndexMap::new(),
                    global: $crate::GlobalConfig::default(),
                    metrics: $crate::config::MetricsConfig::default(),
                }
            }
        }

        impl $crate::config::Mergeable for $config {
            type Key = $key;
            type Component = $component;

            fn components(&self) -> &::indexmap::IndexMap<Self::Key, Self::Component> {
                &self.$field
            }

            fn components_mut(&mut self) -> &mut ::indexmap::IndexMap<Self::Key, Self::Component> {
                &mut self.$field
            }

            fn global(&self) -> &$crate::GlobalConfig {
                &self.global
            }

            fn global_mut(&mut self) -> &mut $crate::GlobalConfig {
                &mut self.global
            }

            fn metrics(&self) -> &$crate::config::MetricsConfig {
                &self.metrics
            }

            fn metrics_mut(&mut self) -> &mut $crate::config::MetricsConfig {
                &mut self.metrics
            }

            fn into_components(self) -> ::indexmap::IndexMap<Self::Key, Self::Component> {
                self.$field
            }

            fn validate(&self) -> Result<(), $crate::error::ConfigError> {
                self.validate_config()
            }
        }
    };
}

/// Trait for configs that can be merged from multiple files.
pub trait Mergeable: Sized + Default + DeserializeOwned {
    type Key: Eq + std::hash::Hash + Clone + std::fmt::Display;
    type Component;

    fn components(&self) -> &IndexMap<Self::Key, Self::Component>;
    fn components_mut(&mut self) -> &mut IndexMap<Self::Key, Self::Component>;
    fn into_components(self) -> IndexMap<Self::Key, Self::Component>;
    fn global(&self) -> &GlobalConfig;
    fn global_mut(&mut self) -> &mut GlobalConfig;
    fn metrics(&self) -> &MetricsConfig;
    fn metrics_mut(&mut self) -> &mut MetricsConfig;

    fn parse_yaml(contents: &str) -> Result<Self, ConfigError> {
        serde_yaml::from_str(contents).map_err(|source| ConfigError::YamlParse { source })
    }

    /// Validate the configuration.
    fn validate(&self) -> Result<(), ConfigError>;

    /// Load configuration from a file path.
    fn from_file(path: &str) -> Result<Self, ConfigError> {
        let contents =
            std::fs::read_to_string(path).map_err(|source| ConfigError::ReadFile { source })?;
        Self::parse(&contents)
    }

    /// Parse configuration from a YAML string with env interpolation and validation.
    fn parse(contents: &str) -> Result<Self, ConfigError> {
        // Interpolate environment variables
        let result = interpolate(contents);
        if !result.is_ok() {
            return Err(ConfigError::EnvInterpolation {
                message: result.errors.join("\n"),
            });
        }

        // Parse YAML
        let config = Self::parse_yaml(&result.text)?;

        // Validate
        config.validate()?;

        Ok(config)
    }

    fn merge(&mut self, mut other: Self) -> Result<(), ConfigError> {
        let duplicates: Vec<String> = other
            .components()
            .keys()
            .filter(|key| self.components().contains_key(*key))
            .map(|key| key.to_string())
            .collect();

        if !duplicates.is_empty() {
            return Err(ConfigError::DuplicateComponents { keys: duplicates });
        }

        for (key, component) in other.components_mut().drain(..) {
            self.components_mut().insert(key, component);
        }

        self.global_mut().merge_from(other.global());
        self.metrics_mut().merge_from(other.metrics());
        Ok(())
    }

    /// Consume the config and return the first component, if any.
    fn into_first(self) -> Option<Self::Component> {
        self.into_components().into_values().next()
    }

    /// Build pipeline instances from config components.
    fn build_pipelines<P, F>(&self, context: PipelineContext, builder: F) -> Vec<P>
    where
        Self::Component: Clone,
        F: Fn(Self::Key, Self::Component, PipelineContext) -> P,
    {
        self.components()
            .iter()
            .map(|(key, cfg)| builder(key.clone(), cfg.clone(), context.clone()))
            .collect()
    }
}

pub fn load_from_paths<C: Mergeable>(paths: &[ConfigPath]) -> Result<C, ConfigError> {
    let mut config = C::default();
    let mut errors = Vec::new();

    for path in paths {
        match path {
            ConfigPath::File(file_path) => match load_file::<C>(file_path) {
                Ok(partial) => {
                    if let Err(e) = config.merge(partial) {
                        errors.push(format!("{}: {}", file_path.display(), e));
                    }
                }
                Err(e) => errors.push(format!("{}: {}", file_path.display(), e)),
            },
            ConfigPath::Dir(dir_path) => match load_dir::<C>(dir_path) {
                Ok(partial) => {
                    if let Err(e) = config.merge(partial) {
                        errors.push(format!("{}: {}", dir_path.display(), e));
                    }
                }
                Err(e) => errors.push(format!("{}: {}", dir_path.display(), e)),
            },
        }
    }

    if !errors.is_empty() {
        return Err(ConfigError::MultipleErrors { errors });
    }
    Ok(config)
}

fn load_file<C: Mergeable>(path: &Path) -> Result<C, ConfigError> {
    if !is_yaml_file(path) {
        return Err(ConfigError::UnsupportedFormat {
            path: path.to_path_buf(),
        });
    }

    let contents =
        std::fs::read_to_string(path).map_err(|source| ConfigError::ReadFile { source })?;

    let result = interpolate(&contents);
    if !result.is_ok() {
        return Err(ConfigError::EnvInterpolation {
            message: result.errors.join("\n"),
        });
    }

    C::parse_yaml(&result.text)
}

fn load_dir<C: Mergeable>(dir: &Path) -> Result<C, ConfigError> {
    let mut config = C::default();
    let mut errors = Vec::new();

    let mut files: Vec<_> = std::fs::read_dir(dir)
        .map_err(|source| ConfigError::ReadDir {
            path: dir.to_path_buf(),
            source,
        })?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let path = entry.path();
            path.is_file() && is_yaml_file(&path)
        })
        .collect();

    files.sort_by_key(|e| e.path());

    for entry in files {
        let path = entry.path();
        match load_file::<C>(&path) {
            Ok(partial) => {
                if let Err(e) = config.merge(partial) {
                    errors.push(format!("{}: {}", path.display(), e));
                }
            }
            Err(e) => errors.push(format!("{}: {}", path.display(), e)),
        }
    }

    if !errors.is_empty() {
        return Err(ConfigError::MultipleErrors { errors });
    }
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_yaml_file() {
        assert!(is_yaml_file(Path::new("config.yaml")));
        assert!(is_yaml_file(Path::new("config.yml")));
        assert!(!is_yaml_file(Path::new("config.toml")));
        assert!(!is_yaml_file(Path::new("readme.md")));
    }
}
