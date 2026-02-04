//! Generic polling loop trait and runner.
//!
//! Provides a reusable polling loop pattern for both blizzard and penguin.

use async_trait::async_trait;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::emit;
use crate::metrics::events::{IterationCompleted, IterationDuration, IterationResultType};
use crate::topology::random_jitter;

/// Result of a single processing iteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IterationResult {
    /// Items were processed successfully.
    ProcessedItems,
    /// No items were available to process.
    NoItems,
    /// Shutdown was requested.
    Shutdown,
}

/// Trait for implementing a polling-based processor.
///
/// Both blizzard (file loader) and penguin (delta checkpointer) implement
/// this trait to share the common polling loop logic.
#[async_trait]
pub trait PollingProcessor {
    /// The state type prepared for each iteration.
    type State: Send;
    /// The error type for this processor.
    type Error: std::error::Error + Send;

    /// Prepare state for a processing iteration.
    ///
    /// Called at the start of each iteration to set up the necessary state.
    /// Returns `None` if there's no work to do (e.g., no files to process).
    ///
    /// # Arguments
    /// * `cold_start` - True on the first iteration (for recovery logic)
    async fn prepare(&mut self, cold_start: bool) -> Result<Option<Self::State>, Self::Error>;

    /// Process the prepared state.
    ///
    /// Called after `prepare` returns `Some(state)` to do the actual work.
    async fn process(&mut self, state: Self::State) -> Result<IterationResult, Self::Error>;
}

/// Run a polling loop with the given processor.
///
/// This function handles the common polling logic:
/// 1. Call `prepare()` to set up state (with cold_start=true on first iteration)
/// 2. Call `process()` if there's work to do
/// 3. Wait for poll_interval (plus random jitter) or shutdown signal
/// 4. Repeat until shutdown
///
/// The `name` parameter identifies the pipeline/table in log messages and metrics.
/// The `service` parameter identifies the service ("blizzard" or "penguin") for metrics.
/// The `poll_jitter_secs` parameter adds random jitter (0 to N seconds) on each
/// iteration to prevent thundering herd when multiple pipelines poll simultaneously.
pub async fn run_polling_loop<P: PollingProcessor>(
    processor: &mut P,
    poll_interval: Duration,
    poll_jitter_secs: u64,
    shutdown: CancellationToken,
    name: &str,
    service: &'static str,
) -> Result<(), P::Error> {
    let mut first_iteration = true;

    loop {
        let iteration_start = Instant::now();
        // Race initialization against shutdown signal
        let shutdown_clone = shutdown.clone();
        let state = tokio::select! {
            biased;

            _ = shutdown_clone.cancelled() => {
                info!(target = name, "Shutdown requested during initialization");
                return Ok(());
            }

            result = async {
                let cold_start = first_iteration;
                first_iteration = false;
                processor.prepare(cold_start).await
            } => result?,
        };

        let result = match state {
            Some(s) => {
                // Race processing against shutdown signal
                let shutdown_clone = shutdown.clone();
                tokio::select! {
                    biased;

                    _ = shutdown_clone.cancelled() => {
                        info!(target = name, "Shutdown requested during processing");
                        IterationResult::Shutdown
                    }

                    result = processor.process(s) => result?,
                }
            }
            None => {
                debug!(target = name, "No items to process");
                IterationResult::NoItems
            }
        };

        // Emit iteration metrics and exit on shutdown
        match result {
            IterationResult::Shutdown => break,
            IterationResult::NoItems => {
                emit!(IterationCompleted {
                    service,
                    result: IterationResultType::NoItems,
                    target: name.to_string(),
                });
                emit!(IterationDuration {
                    service,
                    duration: iteration_start.elapsed(),
                    target: name.to_string(),
                });
                debug!(
                    target = name,
                    "No new items, waiting {}s before next poll",
                    poll_interval.as_secs()
                );
            }
            IterationResult::ProcessedItems => {
                emit!(IterationCompleted {
                    service,
                    result: IterationResultType::Processed,
                    target: name.to_string(),
                });
                emit!(IterationDuration {
                    service,
                    duration: iteration_start.elapsed(),
                    target: name.to_string(),
                });
                debug!(
                    target = name,
                    "Iteration complete, waiting {}s before next poll",
                    poll_interval.as_secs()
                );
            }
        }

        // Wait for poll interval (plus jitter) or shutdown
        let jitter = random_jitter(poll_jitter_secs);
        let sleep_duration = poll_interval + jitter;
        if shutdown
            .run_until_cancelled(tokio::time::sleep(sleep_duration))
            .await
            .is_none()
        {
            info!(target = name, "Shutdown requested during poll wait");
            break;
        }
    }

    Ok(())
}
