//! Generic component identifier.
//!
//!
//! This type provides a generic identifier for any pipeline component.
//! It is specialized as:
//! - `PipelineKey` in blizzard for file loader pipelines
//! - `TableKey` in penguin for Delta table checkpointers

use serde::{Deserialize, Serialize};
use std::fmt;

/// Generic identifier for any pipeline component.
///
/// This is a transparent wrapper around a String that provides
/// consistent identification semantics across the codebase.
#[derive(Debug, Clone, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ComponentKey(String);

impl ComponentKey {
    /// Create a new component key from any string-like value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the underlying identifier string.
    pub fn id(&self) -> &str {
        &self.0
    }

    /// Derive a key from a URI path.
    ///
    /// Extracts the last non-empty path segment as the key.
    /// Returns "default" if no valid segment can be extracted.
    ///
    /// # Examples
    ///
    /// ```
    /// use blizzard_common::config::ComponentKey;
    ///
    /// assert_eq!(ComponentKey::from_uri("gs://bucket/path/to/table").id(), "table");
    /// assert_eq!(ComponentKey::from_uri("gs://bucket/path/to/table/").id(), "table");
    /// assert_eq!(ComponentKey::from_uri("s3://bucket/").id(), "bucket");
    /// assert_eq!(ComponentKey::from_uri("").id(), "default");
    /// ```
    pub fn from_uri(uri: &str) -> Self {
        // Strip scheme if present (e.g., "gs://", "s3://")
        let path = uri.find("://").map(|i| &uri[i + 3..]).unwrap_or(uri);

        // Find the last non-empty segment
        let key = path
            .trim_end_matches('/')
            .rsplit('/')
            .find(|s| !s.is_empty())
            .unwrap_or("default");

        Self(key.to_string())
    }
}

impl fmt::Display for ComponentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for ComponentKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let key = ComponentKey::new("my-component");
        assert_eq!(key.id(), "my-component");
    }

    #[test]
    fn test_from_uri_gcs() {
        let key = ComponentKey::from_uri("gs://bucket/path/to/table");
        assert_eq!(key.id(), "table");
    }

    #[test]
    fn test_from_uri_s3() {
        let key = ComponentKey::from_uri("s3://my-bucket/data/events");
        assert_eq!(key.id(), "events");
    }

    #[test]
    fn test_from_uri_trailing_slash() {
        let key = ComponentKey::from_uri("gs://bucket/path/to/table/");
        assert_eq!(key.id(), "table");
    }

    #[test]
    fn test_from_uri_bucket_only() {
        let key = ComponentKey::from_uri("s3://bucket/");
        assert_eq!(key.id(), "bucket");
    }

    #[test]
    fn test_from_uri_empty() {
        let key = ComponentKey::from_uri("");
        assert_eq!(key.id(), "default");
    }

    #[test]
    fn test_from_uri_no_scheme() {
        let key = ComponentKey::from_uri("/local/path/to/table");
        assert_eq!(key.id(), "table");
    }

    #[test]
    fn test_display() {
        let key = ComponentKey::new("my-component");
        assert_eq!(format!("{}", key), "my-component");
    }

    #[test]
    fn test_serde_roundtrip() {
        let key = ComponentKey::new("test-key");
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, "\"test-key\"");

        let parsed: ComponentKey = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, key);
    }

    #[test]
    fn test_ordering() {
        let a = ComponentKey::new("alpha");
        let b = ComponentKey::new("beta");
        let c = ComponentKey::new("alpha");

        assert!(a < b);
        assert_eq!(a, c);
    }
}
