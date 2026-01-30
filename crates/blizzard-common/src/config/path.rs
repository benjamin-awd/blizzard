//! Configuration path types for multi-file loading.

use std::path::PathBuf;

/// A configuration source - either a single file or a directory.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ConfigPath {
    /// A single configuration file.
    File(PathBuf),
    /// A directory containing configuration files.
    Dir(PathBuf),
}

impl ConfigPath {
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self::File(path.into())
    }

    pub fn dir(path: impl Into<PathBuf>) -> Self {
        Self::Dir(path.into())
    }
}

/// Check if a path has a YAML extension.
pub fn is_yaml_file(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext == "yaml" || ext == "yml")
        .unwrap_or(false)
}
