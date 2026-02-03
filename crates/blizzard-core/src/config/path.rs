//! Configuration path types for multi-file loading.

use std::path::PathBuf;

use clap::Parser;

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

#[derive(Parser, Debug)]
#[command(version)]
pub struct CliArgs {
    /// Path to configuration file (can be specified multiple times)
    #[arg(short, long)]
    pub config: Vec<PathBuf>,

    /// Path to configuration directory (can be specified multiple times)
    #[arg(short = 'C', long = "config-dir")]
    pub config_dirs: Vec<PathBuf>,
}

impl CliArgs {
    /// Convert CLI arguments to configuration paths.
    pub fn config_paths(&self) -> Vec<ConfigPath> {
        ConfigPath::from_cli_args(&self.config, &self.config_dirs)
    }
}
