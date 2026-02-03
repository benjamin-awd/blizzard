//! Multi-file configuration loading.

use std::path::Path;

use indexmap::IndexMap;

use crate::config::{ConfigPath, GlobalConfig, MetricsConfig, interpolate, is_yaml_file};
use crate::error::ConfigError;

/// Trait for configs that can be merged from multiple files.
pub trait Mergeable: Sized + Default {
    type Key: Eq + std::hash::Hash + Clone + std::fmt::Display;
    type Component;

    fn components(&self) -> &IndexMap<Self::Key, Self::Component>;
    fn components_mut(&mut self) -> &mut IndexMap<Self::Key, Self::Component>;
    fn global(&self) -> &GlobalConfig;
    fn global_mut(&mut self) -> &mut GlobalConfig;
    fn metrics(&self) -> &MetricsConfig;
    fn metrics_mut(&mut self) -> &mut MetricsConfig;
    fn parse_yaml(contents: &str) -> Result<Self, ConfigError>;

    fn merge(&mut self, mut other: Self) -> Result<(), ConfigError> {
        let duplicates: Vec<String> = other
            .components_mut()
            .keys()
            .filter(|key: &&Self::Key| self.components_mut().contains_key(*key))
            .map(|key: &Self::Key| key.to_string())
            .collect();

        if !duplicates.is_empty() {
            return Err(ConfigError::DuplicateComponents { keys: duplicates });
        }

        for (key, component) in other.components_mut().drain(..) {
            self.components_mut().insert(key, component);
        }

        self.global_mut()
            .merge_from(std::mem::take(other.global_mut()));
        self.metrics_mut()
            .merge_from(std::mem::take(other.metrics_mut()));
        Ok(())
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
