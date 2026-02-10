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

impl Resource {
    /// Append formatted error messages for resource conflicts to an error list.
    ///
    /// This is a helper for config validation that formats conflicts from
    /// [`Resource::conflicts`] into human-readable error strings.
    ///
    /// # Examples
    ///
    /// ```
    /// use blizzard_core::config::Resource;
    ///
    /// let mut errors = Vec::new();
    /// let components = vec![
    ///     ("table_a", vec![Resource::directory("gs://bucket/staging")]),
    ///     ("table_b", vec![Resource::directory("gs://bucket/staging")]),
    /// ];
    /// let conflicts = Resource::conflicts(components);
    /// Resource::extend_errors(&mut errors, conflicts);
    ///
    /// assert_eq!(errors.len(), 1);
    /// assert!(errors[0].contains("Resource conflict"));
    /// ```
    pub fn extend_errors<K: fmt::Debug>(
        errors: &mut Vec<String>,
        conflicts: HashMap<Resource, HashSet<K>>,
    ) {
        for (resource, keys) in conflicts {
            let keys_list: Vec<_> = keys.iter().collect();
            errors.push(format!(
                "Resource conflict: {resource} claimed by {keys_list:?}"
            ));
        }
    }
}

impl fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Directory(path) => write!(f, "directory:{path}"),
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
    fn test_display() {
        assert_eq!(
            format!("{}", Resource::directory("/path/to/dir")),
            "directory:/path/to/dir"
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
