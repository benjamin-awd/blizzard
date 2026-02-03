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

    /// Combine config file paths and config directory paths into a single list.
    ///
    /// Files are added first, then directories, preserving the order within each group.
    pub fn from_cli_args(config_files: &[PathBuf], config_dirs: &[PathBuf]) -> Vec<Self> {
        config_files
            .iter()
            .map(ConfigPath::file)
            .chain(config_dirs.iter().map(ConfigPath::dir))
            .collect()
    }
}

/// Check if a path has a YAML extension.
pub fn is_yaml_file(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext == "yaml" || ext == "yml")
        .unwrap_or(false)
}
