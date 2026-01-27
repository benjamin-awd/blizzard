//! Dead Letter Queue for failed file tracking.
//!
//! This module provides functionality for recording and tracking failed files
//! during pipeline execution. Failed files are written as NDJSON to a
//! configurable storage location for later inspection and reprocessing.
//!
//! # Components
//!
//! - [`DeadLetterQueue`] - Main DLQ implementation that buffers and writes failures
//! - [`FailureTracker`] - High-level tracker with max_failures enforcement

mod queue;
mod tracker;
mod types;

pub use queue::DeadLetterQueue;
pub use tracker::FailureTracker;
