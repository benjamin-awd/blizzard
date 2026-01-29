//! Dead Letter Queue for failed file tracking.
//!
//! This module provides functionality for recording and tracking failed files
//! during pipeline execution. Failed files are written as NDJSON to a
//! configurable storage location for later inspection and reprocessing.

mod queue;
mod tracker;
mod types;

pub use queue::DeadLetterQueue;
pub use tracker::FailureTracker;
pub use types::{FailedFile, FailureStats};
