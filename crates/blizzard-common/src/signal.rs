//! Signal handling for graceful shutdown.

use tracing::info;

/// Wait for a shutdown signal (SIGINT, SIGTERM, or SIGQUIT on Unix).
#[cfg(unix)]
pub async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set up SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set up SIGTERM handler");
    let mut sigquit = signal(SignalKind::quit()).expect("Failed to set up SIGQUIT handler");

    tokio::select! {
        _ = sigint.recv() => {
            info!(message = "Signal received.", signal = "SIGINT");
        }
        _ = sigterm.recv() => {
            info!(message = "Signal received.", signal = "SIGTERM");
        }
        _ = sigquit.recv() => {
            info!(message = "Signal received.", signal = "SIGQUIT");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_shutdown_token_is_shared() {
        let shutdown = CancellationToken::new();
        let shutdown_for_processor = shutdown.clone();

        shutdown.cancel();

        assert!(
            shutdown_for_processor.is_cancelled(),
            "Shutdown token clones should share cancellation state"
        );
    }

    #[tokio::test]
    async fn test_cancellation_propagates_through_clones() {
        let original = CancellationToken::new();
        let clone1 = original.clone();
        let clone2 = clone1.clone();
        let clone3 = clone2.clone();

        assert!(!original.is_cancelled());
        assert!(!clone1.is_cancelled());
        assert!(!clone2.is_cancelled());
        assert!(!clone3.is_cancelled());

        original.cancel();

        assert!(clone1.is_cancelled());
        assert!(clone2.is_cancelled());
        assert!(clone3.is_cancelled());
    }

    #[tokio::test]
    async fn test_separate_tokens_do_not_share_cancellation() {
        let token1 = CancellationToken::new();
        let token2 = CancellationToken::new();

        token1.cancel();

        assert!(
            !token2.is_cancelled(),
            "Separate tokens should not share cancellation"
        );
    }

    #[tokio::test]
    async fn test_shutdown_cancellation_is_immediate() {
        let shutdown = CancellationToken::new();
        let shutdown_clone = shutdown.clone();

        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_clone.cancelled() => {
                    "cancelled"
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    "timeout"
                }
            }
        });

        shutdown.cancel();

        let result = tokio::time::timeout(Duration::from_millis(100), handle)
            .await
            .expect("task should complete quickly")
            .expect("task should not panic");

        assert_eq!(result, "cancelled");
    }
}
