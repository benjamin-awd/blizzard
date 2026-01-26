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
    /// After pruning, only contains `RecordsRead` entries for in-progress files.
    pub files: HashMap<String, FileReadState>,
    /// High water mark - all files sorting before or equal to this are finished.
    /// This allows us to avoid storing every finished file path in memory.
    #[serde(default)]
    pub last_committed_file: Option<String>,
}

impl SourceState {
    /// Create a new empty source state.
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
            last_committed_file: None,
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
    ///
    /// A file is considered finished if:
    /// - It's explicitly marked as `Finished` in the HashMap, OR
    /// - It sorts before or equal to the high water mark (`last_committed_file`)
    pub fn is_file_finished(&self, path: &str) -> bool {
        // Check explicit Finished state in HashMap
        if matches!(self.files.get(path), Some(FileReadState::Finished)) {
            return true;
        }
        // Check high water mark - files at or before this point are finished
        if let Some(last) = &self.last_committed_file {
            return path <= last.as_str();
        }
        false
    }

    /// Prune finished entries after a successful commit.
    ///
    /// This updates the high water mark to the latest finished file and removes
    /// all `Finished` entries from memory. Only `RecordsRead` entries (in-progress
    /// files) are retained, keeping memory bounded.
    pub fn prune_finished(&mut self) {
        // Find the latest finished file to use as high water mark
        let max_finished = self
            .files
            .iter()
            .filter(|(_, state)| matches!(state, FileReadState::Finished))
            .map(|(path, _)| path.as_str())
            .max();

        if let Some(max) = max_finished {
            // Update high water mark if this is newer
            let should_update = match &self.last_committed_file {
                Some(current) => max > current.as_str(),
                None => true,
            };
            if should_update {
                self.last_committed_file = Some(max.to_string());
            }
        }

        // Remove all Finished entries - they're now tracked by high water mark
        self.files
            .retain(|_, state| matches!(state, FileReadState::RecordsRead(_)));
    }

    /// Get the number of records to skip for a file.
    pub fn records_to_skip(&self, path: &str) -> usize {
        match self.files.get(path) {
            Some(FileReadState::RecordsRead(n)) => *n,
            _ => 0,
        }
    }

    /// Get all files that need processing (not finished).
    pub fn pending_files<'a>(&'a self, all_files: &'a [String]) -> Vec<&'a str> {
        all_files
            .iter()
            .filter(|f| !self.is_file_finished(f))
            .map(|s| s.as_str())
            .collect()
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

        let pending = state.pending_files(&all_files);
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"file2.ndjson.gz"));
        assert!(pending.contains(&"file3.ndjson.gz"));
    }

    #[test]
    fn test_prune_finished() {
        let mut state = SourceState::new();

        // Add some finished and in-progress files
        state.mark_finished("file1.ndjson.gz");
        state.mark_finished("file2.ndjson.gz");
        state.mark_finished("file3.ndjson.gz");
        state.update_records("file4.ndjson.gz", 50);

        // Before pruning: 4 entries in HashMap
        assert_eq!(state.files.len(), 4);
        assert!(state.is_file_finished("file1.ndjson.gz"));
        assert!(state.is_file_finished("file3.ndjson.gz"));

        // Prune finished entries
        state.prune_finished();

        // After pruning: only 1 entry (the in-progress file)
        assert_eq!(state.files.len(), 1);
        assert!(state.files.contains_key("file4.ndjson.gz"));

        // High water mark should be set to the max finished file
        assert_eq!(
            state.last_committed_file,
            Some("file3.ndjson.gz".to_string())
        );

        // Finished files should still be detected via high water mark
        assert!(state.is_file_finished("file1.ndjson.gz"));
        assert!(state.is_file_finished("file2.ndjson.gz"));
        assert!(state.is_file_finished("file3.ndjson.gz"));

        // In-progress file should not be finished
        assert!(!state.is_file_finished("file4.ndjson.gz"));
    }

    #[test]
    fn test_high_water_mark_pending_files() {
        let mut state = SourceState::new();

        // Simulate processing and pruning
        state.mark_finished("2024-01-01.ndjson.gz");
        state.mark_finished("2024-01-02.ndjson.gz");
        state.prune_finished();

        // Now check pending files - files before high water mark should be excluded
        let all_files = vec![
            "2024-01-01.ndjson.gz".to_string(),
            "2024-01-02.ndjson.gz".to_string(),
            "2024-01-03.ndjson.gz".to_string(),
            "2024-01-04.ndjson.gz".to_string(),
        ];

        let pending = state.pending_files(&all_files);
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"2024-01-03.ndjson.gz"));
        assert!(pending.contains(&"2024-01-04.ndjson.gz"));
    }
}
