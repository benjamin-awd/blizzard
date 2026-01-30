//! Prometheus metrics infrastructure with singleton-based initialization.
//!
//! This module provides a shared metrics server that can be safely initialized
//! once and shared across all pipelines/tables in a multi-component deployment.
//!
//! Vector Reference: `~/playground/vector/lib/vector-core/src/metrics/mod.rs`
//!
//! Key design decisions:
//! - `OnceLock` ensures thread-safe, one-time initialization
//! - `init_test()` handles race conditions where multiple test threads initialize
//! - Metrics are always enabled by default (no enabled/disabled flag)

use axum::{Extension, Router, routing::get};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use snafu::prelude::*;
use std::net::SocketAddr;
use std::sync::OnceLock;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::error::{AlreadyInitializedSnafu, MetricsError, NotInitializedSnafu, PrometheusInitSnafu};

/// Default metrics address.
pub const DEFAULT_METRICS_ADDR: &str = "0.0.0.0:9090";

/// Default histogram buckets for duration metrics (in seconds).
const DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Global metrics controller singleton.
static CONTROLLER: OnceLock<MetricsController> = OnceLock::new();

/// Controller for the shared metrics server.
///
/// Provides access to the Prometheus handle for rendering metrics
/// and other operations that need the metrics state.
pub struct MetricsController {
    handle: PrometheusHandle,
}

/// Initialize the metrics server for production use.
///
/// Starts a Prometheus HTTP endpoint on the given address with:
/// - `/metrics` - Prometheus metrics in text format
/// - `/health` - Health check endpoint (returns 200 OK)
///
/// Metrics are always enabled - this should be called unconditionally at startup.
///
/// # Errors
///
/// Returns an error if:
/// - The server is already initialized
/// - The Prometheus recorder fails to initialize
pub fn init_global(addr: SocketAddr) -> Result<(), MetricsError> {
    let handle = PrometheusBuilder::new()
        .set_buckets(DURATION_BUCKETS)
        .expect("valid bucket configuration")
        .install_recorder()
        .context(PrometheusInitSnafu)?;

    let controller = MetricsController { handle };

    CONTROLLER
        .set(controller)
        .map_err(|_| AlreadyInitializedSnafu.build())?;

    // Spawn the HTTP server in the background
    tokio::spawn(run_server(addr));

    info!(%addr, "Metrics server started");
    Ok(())
}

/// Initialize the metrics subsystem for tests.
///
/// Uses the same recorder setup but does NOT start an HTTP endpoint.
/// Handles the race condition where multiple test threads try to
/// initialize simultaneously by spinning until the controller is ready.
///
/// This function is safe to call multiple times from different test threads.
pub fn init_test() {
    if init_test_inner().is_err() {
        // Another thread is initializing. Wait for it to complete.
        while CONTROLLER.get().is_none() {
            std::hint::spin_loop();
        }
    }
}

fn init_test_inner() -> Result<(), MetricsError> {
    let handle = PrometheusBuilder::new()
        .set_buckets(DURATION_BUCKETS)
        .expect("valid bucket configuration")
        .install_recorder()
        .context(PrometheusInitSnafu)?;

    let controller = MetricsController { handle };

    CONTROLLER
        .set(controller)
        .map_err(|_| AlreadyInitializedSnafu.build())?;

    Ok(())
}

impl MetricsController {
    /// Get a reference to the global metrics controller.
    ///
    /// # Errors
    ///
    /// Returns an error if metrics have not been initialized.
    pub fn get() -> Result<&'static Self, MetricsError> {
        CONTROLLER.get().context(NotInitializedSnafu)
    }

    /// Render metrics in Prometheus text format.
    ///
    /// Useful for custom endpoints or debugging.
    pub fn render(&self) -> String {
        self.handle.render()
    }
}

/// Run the HTTP server for metrics and health endpoints.
async fn run_server(addr: SocketAddr) {
    // Get the handle from the controller (guaranteed to exist since we just set it)
    let controller = CONTROLLER.get().expect("controller initialized before server spawn");

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .layer(Extension(controller.handle.clone()));

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind metrics server to {}: {}", addr, e);
            return;
        }
    };

    if let Err(e) = axum::serve(listener, app).await {
        error!("Metrics server error: {}", e);
    }
}

/// Handler for `/metrics` endpoint.
async fn metrics_handler(Extension(handle): Extension<PrometheusHandle>) -> String {
    handle.render()
}

/// Handler for `/health` endpoint.
async fn health_handler() -> &'static str {
    "ok\n"
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::counter;
    use std::thread;

    #[test]
    fn test_init_test_is_idempotent() {
        // Should not panic on repeated calls
        init_test();
        init_test();
        init_test();

        // Controller should be available
        assert!(MetricsController::get().is_ok());
    }

    #[test]
    fn test_controller_render() {
        init_test();

        counter!("blizzard_test_counter").increment(42);

        let controller = MetricsController::get().unwrap();
        let output = controller.render();

        // The counter should appear in the output
        assert!(output.contains("blizzard_test_counter"));
    }

    #[test]
    fn test_concurrent_init_test() {
        let handles: Vec<_> = (0..10)
            .map(|_| {
                thread::spawn(|| {
                    init_test();
                    // All threads should see the controller
                    MetricsController::get().unwrap();
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
