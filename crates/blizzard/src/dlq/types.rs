//! DLQ types for failure tracking.
//!
//! Contains the data structures for representing failed files and
//! aggregating failure statistics.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use blizzard_core::metrics::events::FailureStage;

/// A record representing a failed file in the DLQ.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedFile {
    /// Path to the file that failed.
    pub path: String,
    /// Error message describing the failure.
    pub error: String,
    /// Stage at which the failure occurred.
    pub stage: FailureStage,
    /// Timestamp when the failure was recorded.
    pub timestamp: DateTime<Utc>,
    /// Number of retry attempts (for future use).
    pub retry_count: usize,
}

/// Statistics about failures by stage.
#[derive(Debug, Clone, Default)]
pub struct FailureStats {
    pub download: usize,
    pub decompress: usize,
    pub parse: usize,
    pub upload: usize,
}

impl FailureStats {
    /// Increment the count for a specific stage.
    pub fn increment(&mut self, stage: FailureStage) {
        match stage {
            FailureStage::Download => self.download += 1,
            FailureStage::Decompress => self.decompress += 1,
            FailureStage::Parse => self.parse += 1,
            FailureStage::Upload => self.upload += 1,
        }
    }

    /// Get total failure count.
    pub fn total(&self) -> usize {
        self.download + self.decompress + self.parse + self.upload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failure_stats_increment() {
        let mut stats = FailureStats::default();
        stats.increment(FailureStage::Download);
        stats.increment(FailureStage::Download);
        stats.increment(FailureStage::Parse);

        assert_eq!(stats.download, 2);
        assert_eq!(stats.parse, 1);
        assert_eq!(stats.total(), 3);
    }
}
