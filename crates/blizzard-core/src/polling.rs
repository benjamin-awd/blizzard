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

    /// Finalize the processor on shutdown.
    ///
    /// Called once when the polling loop exits to save any pending state.
    /// Default implementation is a no-op.
    async fn finalize(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
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
pub async fn run_polling_loop<P: PollingProcessor + Send>(
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
                break;
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

    // Finalize processor on shutdown (e.g., save checkpoints)
    processor.finalize().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// A mock error type for testing.
    #[derive(Debug)]
    struct MockError;

    impl std::fmt::Display for MockError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "mock error")
        }
    }

    impl std::error::Error for MockError {}

    /// A mock processor that tracks method calls.
    struct MockProcessor {
        prepare_count: Arc<AtomicUsize>,
        process_count: Arc<AtomicUsize>,
        finalize_called: Arc<AtomicBool>,
        /// If true, prepare() returns None (no work to do)
        no_work: bool,
    }

    impl MockProcessor {
        fn new(finalize_called: Arc<AtomicBool>) -> Self {
            Self {
                prepare_count: Arc::new(AtomicUsize::new(0)),
                process_count: Arc::new(AtomicUsize::new(0)),
                finalize_called,
                no_work: true,
            }
        }
    }

    #[async_trait]
    impl PollingProcessor for MockProcessor {
        type State = ();
        type Error = MockError;

        async fn prepare(&mut self, _cold_start: bool) -> Result<Option<Self::State>, Self::Error> {
            self.prepare_count.fetch_add(1, Ordering::SeqCst);
            if self.no_work { Ok(None) } else { Ok(Some(())) }
        }

        async fn process(&mut self, _state: Self::State) -> Result<IterationResult, Self::Error> {
            self.process_count.fetch_add(1, Ordering::SeqCst);
            Ok(IterationResult::ProcessedItems)
        }

        async fn finalize(&mut self) -> Result<(), Self::Error> {
            self.finalize_called.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    /// Test that finalize() is called when shutdown occurs during poll wait.
    #[tokio::test]
    async fn test_finalize_called_on_shutdown_during_poll_wait() {
        let finalize_called = Arc::new(AtomicBool::new(false));
        let mut processor = MockProcessor::new(finalize_called.clone());

        let shutdown = CancellationToken::new();
        let shutdown_trigger = shutdown.clone();

        // Spawn the polling loop
        let handle = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                run_polling_loop(
                    &mut processor,
                    Duration::from_secs(60), // Long poll interval
                    0,                       // No jitter
                    shutdown,
                    "test",
                    "test",
                )
                .await
            }
        });

        // Give the loop time to start and enter poll wait
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Trigger shutdown
        shutdown_trigger.cancel();

        // Wait for the loop to exit
        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("polling loop should exit within timeout")
            .expect("task should not panic");

        assert!(result.is_ok(), "polling loop should exit cleanly");
        assert!(
            finalize_called.load(Ordering::SeqCst),
            "finalize() should be called on shutdown"
        );
    }

    /// Test that finalize() is called when shutdown occurs during processing.
    #[tokio::test]
    async fn test_finalize_called_on_shutdown_during_processing() {
        let finalize_called = Arc::new(AtomicBool::new(false));
        let mut processor = MockProcessor::new(finalize_called.clone());
        processor.no_work = false; // Return work so process() is called

        let shutdown = CancellationToken::new();

        // Cancel immediately - shutdown will be detected during processing race
        shutdown.cancel();

        let result = run_polling_loop(
            &mut processor,
            Duration::from_secs(60),
            0,
            shutdown,
            "test",
            "test",
        )
        .await;

        assert!(result.is_ok(), "polling loop should exit cleanly");
        assert!(
            finalize_called.load(Ordering::SeqCst),
            "finalize() should be called on shutdown"
        );
    }
}
