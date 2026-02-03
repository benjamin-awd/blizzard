//! Table identifier for penguin Delta tables.
//!
//! This module provides `TableKey`, a penguin-specific wrapper around
//! `ComponentKey` that identifies Delta tables in multi-table configurations.

use blizzard_core::ComponentKey;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Identifier for a Delta table in penguin configuration.
///
/// `TableKey` wraps `ComponentKey` to provide penguin-specific semantics
/// for table identification. It reuses the `from_uri` logic from `ComponentKey`
/// for deriving keys from table URIs.
///
/// # Examples
///
/// ```
/// use penguin::config::TableKey;
///
/// // Create from explicit name
/// let key = TableKey::new("events");
/// assert_eq!(key.id(), "events");
///
/// // Derive from URI
/// let key = TableKey::from_uri("gs://bucket/path/to/events");
/// assert_eq!(key.id(), "events");
/// ```
#[derive(Debug, Clone, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TableKey(ComponentKey);

impl TableKey {
    /// Create a new table key from any string-like value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(ComponentKey::new(id))
    }

    /// Get the underlying identifier string.
    pub fn id(&self) -> &str {
        self.0.id()
    }

    /// Derive a table key from a URI path.
    ///
    /// Extracts the last non-empty path segment as the key.
    /// Returns "default" if no valid segment can be extracted.
    ///
    /// # Examples
    ///
    /// ```
    /// use penguin::config::TableKey;
    ///
    /// assert_eq!(TableKey::from_uri("gs://bucket/path/to/events").id(), "events");
    /// assert_eq!(TableKey::from_uri("gs://bucket/path/to/events/").id(), "events");
    /// ```
    pub fn from_uri(uri: &str) -> Self {
        Self(ComponentKey::from_uri(uri))
    }
}

impl fmt::Display for TableKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for TableKey {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let key = TableKey::new("my-table");
        assert_eq!(key.id(), "my-table");
    }

    #[test]
    fn test_from_uri_gcs() {
        let key = TableKey::from_uri("gs://bucket/path/to/events");
        assert_eq!(key.id(), "events");
    }

    #[test]
    fn test_from_uri_s3() {
        let key = TableKey::from_uri("s3://my-bucket/data/users");
        assert_eq!(key.id(), "users");
    }

    #[test]
    fn test_from_uri_trailing_slash() {
        let key = TableKey::from_uri("gs://bucket/path/to/events/");
        assert_eq!(key.id(), "events");
    }

    #[test]
    fn test_display() {
        let key = TableKey::new("events");
        assert_eq!(format!("{}", key), "events");
    }

    #[test]
    fn test_serde_roundtrip() {
        let key = TableKey::new("test-table");
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, "\"test-table\"");

        let parsed: TableKey = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, key);
    }

    #[test]
    fn test_ordering() {
        let a = TableKey::new("alpha");
        let b = TableKey::new("beta");
        let c = TableKey::new("alpha");

        assert!(a < b);
        assert_eq!(a, c);
    }

    #[test]
    fn test_hash_map_key() {
        use std::collections::HashMap;

        let mut map = HashMap::new();
        map.insert(TableKey::new("events"), "events_value");
        map.insert(TableKey::new("users"), "users_value");

        assert_eq!(map.get(&TableKey::new("events")), Some(&"events_value"));
        assert_eq!(map.get(&TableKey::new("users")), Some(&"users_value"));
    }
}
