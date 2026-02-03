//! Resource conflict detection.
//!
//!
//! Resources are things that cannot be shared between components running
//! in the same process. This module provides types for declaring resources
//! and detecting conflicts when multiple components claim the same resource.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;

/// A resource that cannot be shared between components.
///
/// When multiple components in the same process need exclusive access to
/// a resource, declaring them allows conflict detection at configuration
/// validation time rather than runtime.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Resource {
    /// A directory path (staging dir, output dir, checkpoint dir, etc.)
    Directory(String),
    /// A network port (metrics endpoint, API server, etc.)
    Port(u16),
    /// A file path (lock file, state file, etc.)
    File(String),
}

impl Resource {
    /// Create a directory resource with normalized path.
    ///
    /// Normalization strips trailing slashes to ensure paths like
    /// "/path/to/dir" and "/path/to/dir/" are treated as the same resource.
    ///
    /// # Examples
    ///
    /// ```
    /// use blizzard_core::config::Resource;
    ///
    /// let r1 = Resource::directory("gs://bucket/staging/");
    /// let r2 = Resource::directory("gs://bucket/staging");
    /// assert_eq!(r1, r2);
    /// ```
    pub fn directory(path: &str) -> Self {
        let normalized = path.trim_end_matches('/');
        Self::Directory(normalized.to_string())
    }

    /// Create a port resource.
    pub fn port(port: u16) -> Self {
        Self::Port(port)
    }

    /// Create a file resource with normalized path.
    pub fn file(path: &str) -> Self {
        let normalized = path.trim_end_matches('/');
        Self::File(normalized.to_string())
    }

    /// Detect resource conflicts from a set of component declarations.
    ///
    /// Returns a map from conflicting resource to the set of component keys
    /// that all claim that resource. Empty map means no conflicts.
    ///
    /// # Examples
    ///
    /// ```
    /// use blizzard_core::config::Resource;
    /// use std::collections::HashSet;
    ///
    /// let components = vec![
    ///     ("table_a", vec![Resource::directory("gs://bucket/staging/a")]),
    ///     ("table_b", vec![Resource::directory("gs://bucket/staging/b")]),
    ///     ("table_c", vec![Resource::directory("gs://bucket/staging/a")]), // conflict!
    /// ];
    ///
    /// let conflicts = Resource::conflicts(components);
    /// assert_eq!(conflicts.len(), 1);
    ///
    /// let conflicting_keys = conflicts.get(&Resource::directory("gs://bucket/staging/a")).unwrap();
    /// assert!(conflicting_keys.contains(&"table_a"));
    /// assert!(conflicting_keys.contains(&"table_c"));
    /// ```
    pub fn conflicts<K>(
        components: impl IntoIterator<Item = (K, Vec<Resource>)>,
    ) -> HashMap<Resource, HashSet<K>>
    where
        K: Eq + Hash + Clone,
    {
        let mut resource_to_keys: HashMap<Resource, HashSet<K>> = HashMap::new();

        for (key, resources) in components {
            for resource in resources {
                resource_to_keys
                    .entry(resource)
                    .or_default()
                    .insert(key.clone());
            }
        }

        // Filter to only resources claimed by more than one component
        resource_to_keys
            .into_iter()
            .filter(|(_, keys)| keys.len() > 1)
            .collect()
    }
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Directory(path) => write!(f, "directory:{path}"),
            Self::Port(port) => write!(f, "port:{port}"),
            Self::File(path) => write!(f, "file:{path}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_directory_normalization() {
        let r1 = Resource::directory("/path/to/dir/");
        let r2 = Resource::directory("/path/to/dir");
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_directory_normalization_multiple_slashes() {
        let r1 = Resource::directory("/path/to/dir///");
        let r2 = Resource::directory("/path/to/dir");
        assert_eq!(r1, r2);
    }

    #[test]
    fn test_no_conflicts() {
        let components = vec![
            ("a", vec![Resource::directory("/path/a")]),
            ("b", vec![Resource::directory("/path/b")]),
            ("c", vec![Resource::port(9090)]),
        ];

        let conflicts = Resource::conflicts(components);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_directory_conflict() {
        let components = vec![
            ("table_a", vec![Resource::directory("gs://bucket/staging")]),
            ("table_b", vec![Resource::directory("gs://bucket/other")]),
            ("table_c", vec![Resource::directory("gs://bucket/staging")]),
        ];

        let conflicts = Resource::conflicts(components);
        assert_eq!(conflicts.len(), 1);

        let keys = conflicts
            .get(&Resource::directory("gs://bucket/staging"))
            .unwrap();
        assert!(keys.contains(&"table_a"));
        assert!(keys.contains(&"table_c"));
        assert!(!keys.contains(&"table_b"));
    }

    #[test]
    fn test_port_conflict() {
        let components = vec![
            ("metrics_a", vec![Resource::port(9090)]),
            ("metrics_b", vec![Resource::port(9091)]),
            ("metrics_c", vec![Resource::port(9090)]),
        ];

        let conflicts = Resource::conflicts(components);
        assert_eq!(conflicts.len(), 1);

        let keys = conflicts.get(&Resource::port(9090)).unwrap();
        assert!(keys.contains(&"metrics_a"));
        assert!(keys.contains(&"metrics_c"));
    }

    #[test]
    fn test_multiple_conflicts() {
        let components = vec![
            (
                "a",
                vec![Resource::directory("/staging"), Resource::port(9090)],
            ),
            (
                "b",
                vec![Resource::directory("/staging"), Resource::port(9091)],
            ),
            (
                "c",
                vec![Resource::directory("/other"), Resource::port(9090)],
            ),
        ];

        let conflicts = Resource::conflicts(components);
        assert_eq!(conflicts.len(), 2);

        // Directory conflict between a and b
        let dir_keys = conflicts.get(&Resource::directory("/staging")).unwrap();
        assert!(dir_keys.contains(&"a"));
        assert!(dir_keys.contains(&"b"));

        // Port conflict between a and c
        let port_keys = conflicts.get(&Resource::port(9090)).unwrap();
        assert!(port_keys.contains(&"a"));
        assert!(port_keys.contains(&"c"));
    }

    #[test]
    fn test_three_way_conflict() {
        let components = vec![
            ("a", vec![Resource::file("/var/lock/app.lock")]),
            ("b", vec![Resource::file("/var/lock/app.lock")]),
            ("c", vec![Resource::file("/var/lock/app.lock")]),
        ];

        let conflicts = Resource::conflicts(components);
        assert_eq!(conflicts.len(), 1);

        let keys = conflicts
            .get(&Resource::file("/var/lock/app.lock"))
            .unwrap();
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_display() {
        assert_eq!(
            format!("{}", Resource::directory("/path/to/dir")),
            "directory:/path/to/dir"
        );
        assert_eq!(format!("{}", Resource::port(9090)), "port:9090");
        assert_eq!(
            format!("{}", Resource::file("/var/lock/app.lock")),
            "file:/var/lock/app.lock"
        );
    }

    #[test]
    fn test_empty_components() {
        let components: Vec<(&str, Vec<Resource>)> = vec![];
        let conflicts = Resource::conflicts(components);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_component_with_no_resources() {
        let components = vec![("a", vec![]), ("b", vec![Resource::directory("/path")])];
        let conflicts = Resource::conflicts(components);
        assert!(conflicts.is_empty());
    }
}
