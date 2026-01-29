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
