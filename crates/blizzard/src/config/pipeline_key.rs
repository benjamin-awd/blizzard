//! Pipeline identifier for blizzard pipelines.
//!
//! This module provides `PipelineKey`, a blizzard-specific wrapper around
//! `ComponentKey` that identifies pipelines in multi-pipeline configurations.

use blizzard_core::ComponentKey;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Identifier for a pipeline in blizzard configuration.
///
/// `PipelineKey` wraps `ComponentKey` to provide blizzard-specific semantics
/// for pipeline identification. It reuses the `from_uri` logic from `ComponentKey`
/// for deriving keys from sink table URIs.
///
/// # Examples
///
/// ```
/// use blizzard::config::PipelineKey;
///
/// // Create from explicit name
/// let key = PipelineKey::new("events");
/// assert_eq!(key.id(), "events");
///
/// // Derive from URI
/// let key = PipelineKey::from_uri("gs://bucket/path/to/events");
/// assert_eq!(key.id(), "events");
/// ```
#[derive(Debug, Clone, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PipelineKey(ComponentKey);

impl PipelineKey {
    /// Create a new pipeline key from any string-like value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(ComponentKey::new(id))
    }

    /// Get the underlying identifier string.
    pub fn id(&self) -> &str {
        self.0.id()
    }

    /// Derive a pipeline key from a URI path.
    ///
    /// Extracts the last non-empty path segment as the key.
    /// Returns "default" if no valid segment can be extracted.
    ///
    /// # Examples
    ///
    /// ```
    /// use blizzard::config::PipelineKey;
    ///
    /// assert_eq!(PipelineKey::from_uri("gs://bucket/path/to/events").id(), "events");
    /// assert_eq!(PipelineKey::from_uri("gs://bucket/path/to/events/").id(), "events");
    /// ```
    pub fn from_uri(uri: &str) -> Self {
        Self(ComponentKey::from_uri(uri))
    }
}

impl fmt::Display for PipelineKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for PipelineKey {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let key = PipelineKey::new("my-pipeline");
        assert_eq!(key.id(), "my-pipeline");
    }

    #[test]
    fn test_from_uri_gcs() {
        let key = PipelineKey::from_uri("gs://bucket/path/to/events");
        assert_eq!(key.id(), "events");
    }

    #[test]
    fn test_from_uri_s3() {
        let key = PipelineKey::from_uri("s3://my-bucket/data/users");
        assert_eq!(key.id(), "users");
    }

    #[test]
    fn test_from_uri_trailing_slash() {
        let key = PipelineKey::from_uri("gs://bucket/path/to/events/");
        assert_eq!(key.id(), "events");
    }

    #[test]
    fn test_display() {
        let key = PipelineKey::new("events");
        assert_eq!(format!("{key}"), "events");
    }

    #[test]
    fn test_serde_roundtrip() {
        let key = PipelineKey::new("test-pipeline");
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, "\"test-pipeline\"");

        let parsed: PipelineKey = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, key);
    }

    #[test]
    fn test_ordering() {
        let a = PipelineKey::new("alpha");
        let b = PipelineKey::new("beta");
        let c = PipelineKey::new("alpha");

        assert!(a < b);
        assert_eq!(a, c);
    }

    #[test]
    fn test_hash_map_key() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert(PipelineKey::new("events"), "events_value");
        map.insert(PipelineKey::new("logs"), "logs_value");

        assert_eq!(map.get(&PipelineKey::new("events")), Some(&"events_value"));
        assert_eq!(map.get(&PipelineKey::new("logs")), Some(&"logs_value"));
    }
}
