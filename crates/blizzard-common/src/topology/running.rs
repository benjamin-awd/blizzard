//! Running topology management.
//!
//! Vector equivalent: RunningTopology pattern in vector/src/topology/running.rs
//!
//! Provides a common interface for managing running pipelines.

use async_trait::async_trait;

/// Trait for managing a running topology/pipeline.
///
/// This trait defines the interface for running and shutting down
/// a topology. Implementations manage the lifecycle of all components.
///
/// # Type Parameters
///
/// - `Stats`: The type of statistics returned when the topology completes
///
/// # Examples
///
/// ```ignore
/// // In penguin:
/// impl RunningTopology for RunningPipeline {
///     type Stats = MultiTableStats;
///
///     async fn run_to_completion(self) -> Self::Stats {
///         // Wait for all tables to complete or shutdown...
///     }
///
///     fn shutdown(&self) {
///         self.shutdown_token.cancel();
///     }
/// }
/// ```
#[async_trait]
pub trait RunningTopology {
    /// Statistics type returned when the topology completes.
    type Stats;

    /// Wait for the topology to complete.
    ///
    /// This method runs until all components have finished,
    /// either by completing their work or by receiving a shutdown signal.
    /// Returns statistics about the completed work.
    async fn run_to_completion(self) -> Self::Stats;

    /// Initiate graceful shutdown.
    ///
    /// This signals all components to stop accepting new work and
    /// finish their current tasks. The topology will complete after
    /// all components have stopped.
    fn shutdown(&self);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    // Simple test implementation
    struct TestTopology {
        shutdown_called: Arc<AtomicBool>,
    }

    #[derive(Debug, PartialEq)]
    struct TestStats {
        completed: usize,
    }

    #[async_trait]
    impl RunningTopology for TestTopology {
        type Stats = TestStats;

        async fn run_to_completion(self) -> Self::Stats {
            // Simulate some work
            TestStats { completed: 10 }
        }

        fn shutdown(&self) {
            self.shutdown_called.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn test_run_to_completion() {
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let topology = TestTopology {
            shutdown_called: shutdown_called.clone(),
        };

        let stats = topology.run_to_completion().await;
        assert_eq!(stats.completed, 10);
    }

    #[test]
    fn test_shutdown() {
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let topology = TestTopology {
            shutdown_called: shutdown_called.clone(),
        };

        assert!(!shutdown_called.load(Ordering::SeqCst));
        topology.shutdown();
        assert!(shutdown_called.load(Ordering::SeqCst));
    }
}
