//! Failure tracking with DLQ integration.
//!
//! Provides a high-level interface for tracking failures during pipeline
//! execution, with automatic DLQ recording and max_failures enforcement.

use std::sync::Arc;
use tracing::error;

use blizzard_common::emit;
use blizzard_common::metrics::events::{FailureStage, FileFailed, FileProcessed, FileStatus};

use crate::error::{MaxFailuresSnafu, PipelineError};

use super::DeadLetterQueue;

/// Tracks failures and handles DLQ recording with max_failures enforcement.
pub struct FailureTracker {
    count: usize,
    max_failures: usize,
    dlq: Option<Arc<DeadLetterQueue>>,
    /// Pipeline identifier for metrics labeling.
    pipeline: String,
}

impl FailureTracker {
    /// Create a new failure tracker.
    ///
    /// # Arguments
    /// * `max_failures` - Maximum failures before stopping (0 = unlimited)
    /// * `dlq` - Optional DLQ for recording failures
    /// * `pipeline` - Pipeline identifier for metrics labeling
    pub fn new(max_failures: usize, dlq: Option<Arc<DeadLetterQueue>>, pipeline: String) -> Self {
        Self {
            count: 0,
            max_failures,
            dlq,
            pipeline,
        }
    }

    /// Record a failure, emit metrics, and check max_failures limit.
    ///
    /// Returns `Err` if max_failures has been reached (after finalizing DLQ).
    pub async fn record_failure(
        &mut self,
        error: &str,
        stage: FailureStage,
    ) -> Result<(), PipelineError> {
        self.count += 1;
        emit!(FileProcessed {
            status: FileStatus::Failed,
            pipeline: self.pipeline.clone(),
        });
        emit!(FileFailed {
            stage,
            pipeline: self.pipeline.clone(),
        });

        if let Some(dlq) = &self.dlq {
            dlq.record_failure("unknown", error, stage).await;
        }

        if self.max_failures > 0 && self.count >= self.max_failures {
            error!("Max failures ({}) reached, stopping pipeline", self.count);
            self.finalize_dlq().await;
            return MaxFailuresSnafu { count: self.count }.fail();
        }

        Ok(())
    }

    /// Finalize DLQ, logging any errors.
    pub async fn finalize_dlq(&self) {
        if let Some(dlq) = &self.dlq
            && let Err(e) = dlq.finalize().await
        {
            error!("Failed to finalize DLQ: {}", e);
        }
    }

    /// Returns true if any failures were recorded.
    pub fn has_failures(&self) -> bool {
        self.count > 0
    }

    /// Returns the failure count.
    pub fn count(&self) -> usize {
        self.count
    }
}
