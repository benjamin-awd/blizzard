//! Base global configuration.
//!
//!
//! Provides shared configuration options that apply across all components
//! in a multi-component setup. Individual crates can extend this with
//! their own global options.

use serde::{Deserialize, Serialize};

/// Base global configuration shared across all components.
///
/// This type provides configuration options that affect all components
/// running in the same process. Crates can embed this type and extend it
/// with their own fields.
///
/// # Examples
///
/// ```
/// use blizzard_common::config::GlobalConfig;
///
/// // Default configuration
/// let config = GlobalConfig::default();
/// assert_eq!(config.total_concurrency, None);
///
/// // With concurrency limit
/// let config = GlobalConfig {
///     total_concurrency: Some(8),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct GlobalConfig {
    /// Total concurrent operations across all components.
    ///
    /// When set, this limits the total number of concurrent operations
    /// (such as file uploads or downloads) across all components. This
    /// helps prevent resource exhaustion when running many components.
    ///
    /// If `None` (the default), no global limit is applied and each
    /// component manages its own concurrency.
    #[serde(default)]
    pub total_concurrency: Option<usize>,
}

impl GlobalConfig {
    /// Create a new GlobalConfig with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a GlobalConfig with a specific concurrency limit.
    pub fn with_concurrency(limit: usize) -> Self {
        Self {
            total_concurrency: Some(limit),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let config = GlobalConfig::default();
        assert_eq!(config.total_concurrency, None);
    }

    #[test]
    fn test_with_concurrency() {
        let config = GlobalConfig::with_concurrency(8);
        assert_eq!(config.total_concurrency, Some(8));
    }

    #[test]
    fn test_serde_default() {
        let json = "{}";
        let config: GlobalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.total_concurrency, None);
    }

    #[test]
    fn test_serde_with_value() {
        let json = r#"{"total_concurrency": 16}"#;
        let config: GlobalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.total_concurrency, Some(16));
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = GlobalConfig::with_concurrency(4);
        let json = serde_json::to_string(&config).unwrap();
        let parsed: GlobalConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_yaml_parsing() {
        let yaml = "total_concurrency: 8";
        let config: GlobalConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.total_concurrency, Some(8));
    }

    #[test]
    fn test_yaml_empty() {
        let yaml = "";
        let config: GlobalConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.total_concurrency, None);
    }
}
