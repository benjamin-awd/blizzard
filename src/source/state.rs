//! File progress tracking for checkpoint/recovery.
//!
//! Tracks which files have been processed and how many records
//! have been read from each file to enable exactly-once processing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// State of reading a single file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FileReadState {
    /// File has been completely processed.
    Finished,
    /// File is partially processed with this many records read.
    RecordsRead(usize),
}

/// Aggregate state for all source files.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SourceState {
    /// Map of file path to read state.
    pub files: HashMap<String, FileReadState>,
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
        self.files.insert(path.to_string(), FileReadState::Finished);
    }

    /// Update the record count for a file.
    pub fn update_records(&mut self, path: &str, records_read: usize) {
        self.files
            .insert(path.to_string(), FileReadState::RecordsRead(records_read));
    }

    /// Check if a file has been completely processed.
    pub fn is_file_finished(&self, path: &str) -> bool {
        matches!(self.files.get(path), Some(FileReadState::Finished))
    }

    /// Get the number of records to skip for a file.
    pub fn records_to_skip(&self, path: &str) -> usize {
        match self.files.get(path) {
            Some(FileReadState::RecordsRead(n)) => *n,
            _ => 0,
        }
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
    /// - In-progress files are always retained (they need to be resumed)
    /// - Finished files are only retained if they match at least one prefix
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

        self.files.retain(|path, state| {
            // Always keep in-progress files
            if matches!(state, FileReadState::RecordsRead(_)) {
                return true;
            }

            // Keep finished files that match any prefix
            prefixes.iter().any(|prefix| path.starts_with(prefix))
        });

        before - self.files.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_state() {
        let mut state = SourceState::new();

        state.update_records("file1.ndjson.gz", 100);
        state.mark_finished("file2.ndjson.gz");

        assert!(!state.is_file_finished("file1.ndjson.gz"));
        assert!(state.is_file_finished("file2.ndjson.gz"));
        assert_eq!(state.records_to_skip("file1.ndjson.gz"), 100);
        assert_eq!(state.records_to_skip("file2.ndjson.gz"), 0);
    }

    #[test]
    fn test_pending_files() {
        let mut state = SourceState::new();

        state.mark_finished("file1.ndjson.gz");
        state.update_records("file2.ndjson.gz", 50);

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

        // In-progress file outside window (should still be kept)
        state.update_records("date=2026-01-20/hour=00/file5.ndjson.gz", 100);

        assert_eq!(state.files.len(), 5);

        let prefixes = vec![
            "date=2026-01-28/hour=08".to_string(),
            "date=2026-01-28/hour=09".to_string(),
        ];

        let removed = state.compact(&prefixes);

        assert_eq!(removed, 2); // Two old finished files removed
        assert_eq!(state.files.len(), 3);

        // Recent finished files kept
        assert!(state.is_file_finished("date=2026-01-28/hour=08/file3.ndjson.gz"));
        assert!(state.is_file_finished("date=2026-01-28/hour=09/file4.ndjson.gz"));

        // In-progress file kept even though outside window
        assert!(!state.is_file_finished("date=2026-01-20/hour=00/file5.ndjson.gz"));
        assert_eq!(
            state.records_to_skip("date=2026-01-20/hour=00/file5.ndjson.gz"),
            100
        );
    }

    #[test]
    fn test_compact_with_empty_prefixes_removes_all_finished() {
        let mut state = SourceState::new();

        state.mark_finished("file1.ndjson.gz");
        state.mark_finished("file2.ndjson.gz");
        state.update_records("file3.ndjson.gz", 50);

        let removed = state.compact(&[]);

        assert_eq!(removed, 2);
        assert_eq!(state.files.len(), 1);
        assert!(!state.is_file_finished("file3.ndjson.gz"));
    }
}
