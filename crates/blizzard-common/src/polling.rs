//! Generic polling loop trait and runner.
//!
//! Provides a reusable polling loop pattern for both blizzard and penguin.

use async_trait::async_trait;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

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
/// 3. Wait for poll_interval or shutdown signal
/// 4. Repeat until shutdown
pub async fn run_polling_loop<P: PollingProcessor>(
    processor: &mut P,
    poll_interval: Duration,
    shutdown: CancellationToken,
) -> Result<(), P::Error> {
    let mut first_iteration = true;

    loop {
        // Race initialization against shutdown signal
        let shutdown_clone = shutdown.clone();
        let state = tokio::select! {
            biased;

            _ = shutdown_clone.cancelled() => {
                info!("Shutdown requested during initialization");
                return Ok(());
            }

            result = async {
                let cold_start = first_iteration;
                first_iteration = false;
                processor.prepare(cold_start).await
            } => result?,
        };

        let result = match state {
            Some(s) => processor.process(s).await?,
            None => {
                info!("No items to process");
                IterationResult::NoItems
            }
        };

        // Exit on shutdown, otherwise wait and poll again
        match result {
            IterationResult::Shutdown => break,
            IterationResult::NoItems => {
                info!(
                    "No new items, waiting {}s before next poll",
                    poll_interval.as_secs()
                );
            }
            IterationResult::ProcessedItems => {
                info!(
                    "Iteration complete, waiting {}s before next poll",
                    poll_interval.as_secs()
                );
            }
        }

        // Wait for poll interval or shutdown
        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Shutdown requested during poll wait");
                break;
            }
            _ = tokio::time::sleep(poll_interval) => {}
        }
    }

    Ok(())
}
