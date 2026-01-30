//! Base global configuration.
//!
//!
//! Provides shared configuration options that apply across all components
//! in a multi-component setup. Individual crates can extend this with
//! their own global options.

use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

fn default_poll_jitter_secs() -> u64 {
    30
}

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
/// assert!(config.connection_pooling);
/// assert_eq!(config.poll_jitter_secs, 30);
///
/// // With concurrency limit
/// let config = GlobalConfig {
///     total_concurrency: Some(8),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

    /// Enable connection pooling for storage providers.
    ///
    /// When enabled (default), pipelines reading from the same bucket
    /// will share storage connections, reducing connection overhead
    /// and improving HTTP/2 multiplexing.
    #[serde(default = "default_true")]
    pub connection_pooling: bool,

    /// Maximum jitter (in seconds) added to poll intervals.
    ///
    /// Prevents thundering herd when many pipelines poll simultaneously.
    /// Each pipeline will have a random delay between 0 and this value
    /// added to its poll interval.
    ///
    /// Default: 30 seconds.
    #[serde(default = "default_poll_jitter_secs")]
    pub poll_jitter_secs: u64,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            total_concurrency: None,
            connection_pooling: default_true(),
            poll_jitter_secs: default_poll_jitter_secs(),
        }
    }
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
            ..Default::default()
        }
    }

    /// Merge values from another GlobalConfig (last-write-wins).
    pub fn merge_from(&mut self, other: Self) {
        if other.total_concurrency.is_some() {
            self.total_concurrency = other.total_concurrency;
        }
        // Always take explicit values for non-Option fields
        self.connection_pooling = other.connection_pooling;
        self.poll_jitter_secs = other.poll_jitter_secs;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default() {
        let config = GlobalConfig::default();
        assert_eq!(config.total_concurrency, None);
        assert!(config.connection_pooling);
        assert_eq!(config.poll_jitter_secs, 30);
    }

    #[test]
    fn test_with_concurrency() {
        let config = GlobalConfig::with_concurrency(8);
        assert_eq!(config.total_concurrency, Some(8));
        // Other fields should be defaults
        assert!(config.connection_pooling);
        assert_eq!(config.poll_jitter_secs, 30);
    }

    #[test]
    fn test_serde_default() {
        let json = "{}";
        let config: GlobalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.total_concurrency, None);
        assert!(config.connection_pooling);
        assert_eq!(config.poll_jitter_secs, 30);
    }

    #[test]
    fn test_serde_with_value() {
        let json = r#"{"total_concurrency": 16}"#;
        let config: GlobalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.total_concurrency, Some(16));
    }

    #[test]
    fn test_serde_with_all_fields() {
        let json = r#"{"total_concurrency": 16, "connection_pooling": false, "poll_jitter_secs": 60}"#;
        let config: GlobalConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.total_concurrency, Some(16));
        assert!(!config.connection_pooling);
        assert_eq!(config.poll_jitter_secs, 60);
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
        assert!(config.connection_pooling);
        assert_eq!(config.poll_jitter_secs, 30);
    }

    #[test]
    fn test_yaml_with_resource_fields() {
        let yaml = r#"
total_concurrency: 8
connection_pooling: true
poll_jitter_secs: 45
"#;
        let config: GlobalConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.total_concurrency, Some(8));
        assert!(config.connection_pooling);
        assert_eq!(config.poll_jitter_secs, 45);
    }

    #[test]
    fn test_merge_from() {
        let mut config = GlobalConfig::default();
        let other = GlobalConfig {
            total_concurrency: Some(16),
            connection_pooling: false,
            poll_jitter_secs: 60,
        };
        config.merge_from(other);
        assert_eq!(config.total_concurrency, Some(16));
        assert!(!config.connection_pooling);
        assert_eq!(config.poll_jitter_secs, 60);
    }
}
