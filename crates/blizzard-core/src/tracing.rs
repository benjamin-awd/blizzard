//! Tracing initialization for CLI applications.

use std::io::IsTerminal;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Initialize tracing with rate-limited output.
///
/// Uses `RUST_LOG` environment variable for filtering, defaulting to `info` level.
/// Applies rate limiting to prevent log spam from high-frequency events.
/// ANSI colors are enabled only when stdout is a terminal.
pub fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_ansi(std::io::stdout().is_terminal());

    // Use rate-limited layer to prevent log spam
    let rate_limited = tracing_limit::RateLimitedLayer::new(fmt_layer);

    tracing_subscriber::registry()
        .with(rate_limited)
        .with(env_filter)
        .init();
}
