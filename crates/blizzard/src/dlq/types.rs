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
