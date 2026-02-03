//! Common types shared between blizzard and penguin.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Information about a completed Parquet file.
///
/// This type is used to communicate between blizzard (file loader) and penguin (delta checkpointer)
/// through the staging directory protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinishedFile {
    /// The path to the file (relative to the table root).
    pub filename: String,
    /// The size of the file in bytes.
    pub size: usize,
    /// Number of records in the file.
    pub record_count: usize,
    /// The parquet file bytes (to be uploaded to storage).
    /// None if the file was already uploaded (e.g., from checkpoint recovery).
    /// This field is skipped during serialization for the staging protocol.
    #[serde(skip)]
    pub bytes: Option<bytes::Bytes>,
    /// Partition values extracted from source path (e.g., {"date": "2026-01-28"}).
    pub partition_values: HashMap<String, String>,
    /// Original source file that produced this parquet file.
    #[serde(default)]
    pub source_file: Option<String>,
}

impl FinishedFile {
    /// Create a new FinishedFile with bytes for in-process use.
    pub fn with_bytes(
        filename: String,
        size: usize,
        record_count: usize,
        bytes: bytes::Bytes,
        partition_values: HashMap<String, String>,
    ) -> Self {
        Self {
            filename,
            size,
            record_count,
            bytes: Some(bytes),
            partition_values,
            source_file: None,
        }
    }

    /// Create a FinishedFile without bytes (e.g., when loaded from staging metadata).
    pub fn without_bytes(
        filename: String,
        size: usize,
        record_count: usize,
        partition_values: HashMap<String, String>,
        source_file: Option<String>,
    ) -> Self {
        Self {
            filename,
            size,
            record_count,
            bytes: None,
            partition_values,
            source_file,
        }
    }
}

/// Aggregate state for all source files.
///
/// Tracks which files have been completely processed.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceState {
    /// Set of file paths that have been finished.
    /// Uses a HashMap for backwards compatibility with existing checkpoints.
    pub files: HashMap<String, ()>,
}

impl SourceState {
    /// Create a new empty source state.
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    /// Mark a file as finished.
    pub fn mark_finished(&mut self, path: &str) {
        self.files.insert(path.to_string(), ());
    }

    /// Check if a file has been completely processed.
    pub fn is_file_finished(&self, path: &str) -> bool {
        self.files.contains_key(path)
    }

    /// Filter and return owned files that need processing.
    ///
    /// Takes ownership of the input to avoid re-allocating strings.
    pub fn filter_pending_files(&self, all_files: Vec<String>) -> Vec<String> {
        all_files
            .into_iter()
            .filter(|f| !self.is_file_finished(f))
            .collect()
    }

    /// Compact the state by removing finished files that don't match any prefix.
    ///
    /// This reduces memory usage by pruning files that will never appear in
    /// future listings (because they're outside the partition filter window).
    ///
    /// Returns the number of files removed.
    ///
    /// # Warning
    ///
    /// Compacted files lose their "finished" status. If you later widen or disable
    /// the partition filter, those files may reappear in listings and be reprocessed.
    /// Only change the partition filter configuration if you're okay with potential
    /// duplicate processing of old files, or if you're certain those files no longer
    /// exist in the source location.
    pub fn compact(&mut self, prefixes: &[String]) -> usize {
        let before = self.files.len();

        self.files
            .retain(|path, _| prefixes.iter().any(|prefix| path.starts_with(prefix)));

        before - self.files.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_state() {
        let mut state = SourceState::new();

        state.mark_finished("file1.ndjson.gz");
        state.mark_finished("file2.ndjson.gz");

        assert!(state.is_file_finished("file1.ndjson.gz"));
        assert!(state.is_file_finished("file2.ndjson.gz"));
        assert!(!state.is_file_finished("file3.ndjson.gz"));
    }

    #[test]
    fn test_pending_files() {
        let mut state = SourceState::new();

        state.mark_finished("file1.ndjson.gz");

        let all_files = vec![
            "file1.ndjson.gz".to_string(),
            "file2.ndjson.gz".to_string(),
            "file3.ndjson.gz".to_string(),
        ];

        let pending = state.filter_pending_files(all_files);
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"file2.ndjson.gz".to_string()));
        assert!(pending.contains(&"file3.ndjson.gz".to_string()));
    }

    #[test]
    fn test_compact_removes_old_finished_files() {
        let mut state = SourceState::new();

        // Old finished files (should be removed)
        state.mark_finished("date=2026-01-25/hour=00/file1.ndjson.gz");
        state.mark_finished("date=2026-01-25/hour=01/file2.ndjson.gz");

        // Recent finished files (should be kept)
        state.mark_finished("date=2026-01-28/hour=08/file3.ndjson.gz");
        state.mark_finished("date=2026-01-28/hour=09/file4.ndjson.gz");

        assert_eq!(state.files.len(), 4);

        let prefixes = vec![
            "date=2026-01-28/hour=08".to_string(),
            "date=2026-01-28/hour=09".to_string(),
        ];

        let removed = state.compact(&prefixes);

        assert_eq!(removed, 2); // Two old finished files removed
        assert_eq!(state.files.len(), 2);

        // Recent finished files kept
        assert!(state.is_file_finished("date=2026-01-28/hour=08/file3.ndjson.gz"));
        assert!(state.is_file_finished("date=2026-01-28/hour=09/file4.ndjson.gz"));
    }
}
