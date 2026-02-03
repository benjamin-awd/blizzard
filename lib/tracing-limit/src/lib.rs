//! Rate-limited tracing layer.
//!
//! This module provides a tracing layer that rate-limits log events to prevent
//! log spam.
//!
//! # Behavior
//!
//! - Rate limiting is ON by default (opt-out with `internal_log_rate_limit = false`)
//! - Default window is 10 seconds (configurable via `with_default_limit`)
//! - Default burst limit is 30 events (configurable via `with_burst_limit`)
//! - Events are grouped by callsite + `component_id` field
//! - Within a time window:
//!   - First N events (up to burst limit): emitted normally
//!   - N+1+ events: silently suppressed
//! - After window expires: emits summary of suppressed count, then resets
//!
//! # Usage
//!
//! ```ignore
//! use tracing_limit::RateLimitedLayer;
//! use tracing_subscriber::layer::SubscriberExt;
//! use tracing_subscriber::util::SubscriberInitExt;
//!
//! let fmt_layer = tracing_subscriber::fmt::layer();
//! let rate_limited = RateLimitedLayer::new(fmt_layer)
//!     .with_default_limit(10)   // 10 second window
//!     .with_burst_limit(30)     // allow 30 events before limiting
//!     .with_max_entries(10000);
//!
//! tracing_subscriber::registry()
//!     .with(rate_limited)
//!     .init();
//! ```
//!
//! # Per-event configuration
//!
//! - `internal_log_rate_limit = false` - disable rate limiting for this event
//! - `internal_log_rate_secs = N` - override the window for this event

use std::fmt;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use dashmap::DashMap;
use tracing_core::callsite::Identifier;
use tracing_core::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

/// Global epoch for converting `Instant` to atomic-friendly nanoseconds.
static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Get nanoseconds since the epoch.
fn nanos_since_epoch() -> u64 {
    let epoch = EPOCH.get_or_init(Instant::now);
    u64::try_from(epoch.elapsed().as_nanos()).expect("elapsed time in nanos should fit in u64")
}

/// Default rate limit window in seconds.
const DEFAULT_LIMIT_SECS: u64 = 10;

/// Default burst limit (number of events allowed before rate limiting kicks in).
const DEFAULT_BURST_LIMIT: u64 = 30;

/// Key for rate limiting - combines callsite identifier with optional component_id.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct RateLimitKey {
    /// Callsite identifier from tracing.
    callsite: Identifier,
    /// Optional component identifier for finer-grained limiting.
    component_id: Option<String>,
}

/// State for a rate-limited event.
///
/// All fields are atomic, allowing concurrent access without `&mut self`.
struct RateLimitState {
    /// When the current window started (nanoseconds since epoch).
    window_start: AtomicU64,
    /// Number of events seen in the current window.
    count: AtomicU64,
    /// Rate limit window in nanoseconds.
    limit_nanos: u64,
}

impl RateLimitState {
    fn new(limit_secs: u64) -> Self {
        Self {
            window_start: AtomicU64::new(nanos_since_epoch()),
            count: AtomicU64::new(0),
            limit_nanos: limit_secs.saturating_mul(1_000_000_000),
        }
    }

    /// Check if the window has expired.
    fn is_expired(&self) -> bool {
        let now = nanos_since_epoch();
        let start = self.window_start.load(Ordering::Relaxed);
        now.saturating_sub(start) >= self.limit_nanos
    }

    /// Reset the window to now with count = 1.
    fn reset(&self, limit_secs: u64) {
        self.window_start
            .store(nanos_since_epoch(), Ordering::Relaxed);
        self.count.store(1, Ordering::Relaxed);
        // Note: limit_nanos is immutable after creation, but we accept the
        // parameter for API consistency. A new entry will be created if the
        // limit changes significantly.
        let _ = limit_secs;
    }

    /// Increment count and return the new value.
    fn increment(&self) -> u64 {
        self.count.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Get the current count.
    fn get_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get the window start time in nanoseconds since epoch.
    fn window_start_nanos(&self) -> u64 {
        self.window_start.load(Ordering::Relaxed)
    }
}

/// Visitor to extract rate limit configuration and message from event fields.
struct LimitVisitor {
    /// Whether rate limiting is enabled for this event.
    rate_limit_enabled: bool,
    /// Custom rate limit window in seconds.
    rate_limit_secs: Option<u64>,
    /// Component ID for grouping.
    component_id: Option<String>,
    /// The log message (for displaying in rate limit warning).
    message: Option<String>,
}

impl LimitVisitor {
    fn new() -> Self {
        Self {
            rate_limit_enabled: true,
            rate_limit_secs: None,
            component_id: None,
            message: None,
        }
    }
}

impl tracing_core::field::Visit for LimitVisitor {
    fn record_bool(&mut self, field: &tracing_core::field::Field, value: bool) {
        if field.name() == "internal_log_rate_limit" {
            self.rate_limit_enabled = value;
        }
    }

    fn record_u64(&mut self, field: &tracing_core::field::Field, value: u64) {
        if field.name() == "internal_log_rate_secs" {
            self.rate_limit_secs = Some(value);
        }
    }

    fn record_i64(&mut self, field: &tracing_core::field::Field, value: i64) {
        if field.name() == "internal_log_rate_secs" && value > 0 {
            self.rate_limit_secs = Some(value.cast_unsigned());
        }
    }

    fn record_str(&mut self, field: &tracing_core::field::Field, value: &str) {
        match field.name() {
            "component_id" => self.component_id = Some(value.to_string()),
            "message" => self.message = Some(value.to_string()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &tracing_core::field::Field, value: &dyn fmt::Debug) {
        match field.name() {
            "component_id" => self.component_id = Some(format!("{value:?}")),
            "message" => {
                // Debug formatting adds quotes around strings; strip them
                let formatted = format!("{value:?}");
                self.message = Some(
                    formatted
                        .strip_prefix('"')
                        .and_then(|s| s.strip_suffix('"'))
                        .unwrap_or(&formatted)
                        .to_string(),
                );
            }
            _ => {}
        }
    }
}

/// Action to take for a rate-limited event.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RateLimitAction {
    /// Emit the event normally.
    Emit,
    /// Emit a summary of suppressed events.
    EmitSummary { suppressed: u64 },
    /// Suppress the event.
    Suppress,
}

/// A tracing layer that rate-limits log events.
///
/// This layer wraps an inner layer and rate-limits events based on their
/// callsite and optional `component_id` field.
pub struct RateLimitedLayer<L> {
    /// The inner layer to forward events to.
    inner: L,
    /// Default rate limit window in seconds.
    default_limit_secs: u64,
    /// Burst limit - number of events allowed before rate limiting kicks in.
    burst_limit: u64,
    /// Maximum number of entries to track (for bounded memory usage).
    max_entries: Option<usize>,
    /// Rate limit state for each key.
    state: DashMap<RateLimitKey, RateLimitState>,
}

impl<L> RateLimitedLayer<L> {
    /// Create a new rate-limited layer wrapping the given inner layer.
    pub fn new(inner: L) -> Self {
        Self {
            inner,
            default_limit_secs: DEFAULT_LIMIT_SECS,
            burst_limit: DEFAULT_BURST_LIMIT,
            max_entries: None,
            state: DashMap::new(),
        }
    }

    /// Set the default rate limit window in seconds.
    pub fn with_default_limit(mut self, secs: u64) -> Self {
        self.default_limit_secs = secs;
        self
    }

    /// Set the burst limit (number of events allowed before rate limiting kicks in).
    ///
    /// Default is 30. Set higher to allow more events before suppression.
    pub fn with_burst_limit(mut self, limit: u64) -> Self {
        self.burst_limit = limit;
        self
    }

    /// Set the maximum number of entries to track.
    ///
    /// When the limit is reached, the oldest entries (by window start time)
    /// are evicted to make room for new ones. This prevents unbounded memory
    /// growth in applications with many unique callsites or component IDs.
    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = Some(max);
        self
    }

    /// Evict oldest entries if we're at capacity.
    fn maybe_evict(&self) {
        let Some(max) = self.max_entries else {
            return;
        };

        if self.state.len() < max {
            return;
        }

        // Find and remove expired entries first
        self.state.retain(|_, state| !state.is_expired());

        // If still over capacity, remove oldest entries
        if self.state.len() >= max {
            // Collect entries with their ages
            let mut entries: Vec<_> = self
                .state
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().window_start_nanos()))
                .collect();

            // Sort by window_start (oldest first)
            entries.sort_by_key(|(_, start)| *start);

            // Remove oldest entries until we're under 75% capacity
            let target = max * 3 / 4;
            let to_remove = self.state.len().saturating_sub(target);
            for (key, _) in entries.into_iter().take(to_remove) {
                self.state.remove(&key);
            }
        }
    }

    /// Determine what action to take for an event.
    fn check_rate_limit(&self, key: RateLimitKey, limit_secs: u64) -> RateLimitAction {
        let burst = self.burst_limit;

        // Check if entry exists first
        if let Some(entry) = self.state.get(&key) {
            let state = entry.value();

            // Check if window has expired
            if state.is_expired() {
                let suppressed = state.get_count().saturating_sub(burst);

                // Reset state atomically
                state.reset(limit_secs);

                if suppressed > 0 {
                    return RateLimitAction::EmitSummary { suppressed };
                }
                return RateLimitAction::Emit;
            }

            // Increment count
            let count = state.increment();

            return if count <= burst {
                RateLimitAction::Emit
            } else {
                RateLimitAction::Suppress
            };
        }

        // Entry doesn't exist, potentially evict before inserting
        self.maybe_evict();

        // Insert new entry and increment
        let entry = self
            .state
            .entry(key)
            .or_insert_with(|| RateLimitState::new(limit_secs));
        let count = entry.value().increment();

        if count <= burst {
            RateLimitAction::Emit
        } else {
            RateLimitAction::Suppress
        }
    }
}

impl<S, L> Layer<S> for RateLimitedLayer<L>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    L: Layer<S>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        // Extract rate limit configuration
        let mut limit_visitor = LimitVisitor::new();
        event.record(&mut limit_visitor);

        // If rate limiting is disabled for this event, forward it directly
        if !limit_visitor.rate_limit_enabled {
            self.inner.on_event(event, ctx);
            return;
        }

        // Build rate limit key
        let key = RateLimitKey {
            callsite: event.metadata().callsite(),
            component_id: limit_visitor.component_id,
        };

        // Determine rate limit window
        let limit_secs = limit_visitor
            .rate_limit_secs
            .unwrap_or(self.default_limit_secs);

        // Check rate limit
        match self.check_rate_limit(key, limit_secs) {
            RateLimitAction::Emit => {
                self.inner.on_event(event, ctx);
            }
            RateLimitAction::EmitSummary { suppressed } => {
                // Log summary of suppressed events with the message content
                let message = limit_visitor.message.as_deref().unwrap_or("<no message>");
                eprintln!("  Internal log [{message}] {suppressed} messages were rate limited.");

                // Then emit the new event
                self.inner.on_event(event, ctx);
            }
            RateLimitAction::Suppress => {
                // Silently suppress
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_state_expiry() {
        let state = RateLimitState::new(0); // 0 second window = immediately expired
        assert!(state.is_expired());

        let state = RateLimitState::new(3600); // 1 hour window
        assert!(!state.is_expired());
    }

    #[test]
    fn test_rate_limit_state_increment() {
        let state = RateLimitState::new(10);
        assert_eq!(state.increment(), 1);
        assert_eq!(state.increment(), 2);
        assert_eq!(state.increment(), 3);
        assert_eq!(state.get_count(), 3);
    }

    #[test]
    fn test_rate_limit_state_reset() {
        let state = RateLimitState::new(10);
        state.increment();
        state.increment();
        assert_eq!(state.get_count(), 2);

        state.reset(10);
        assert_eq!(state.get_count(), 1);
    }

    #[test]
    fn test_nanos_since_epoch() {
        let t1 = nanos_since_epoch();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t2 = nanos_since_epoch();
        assert!(t2 > t1);
    }

    #[test]
    fn test_strip_debug_quotes() {
        // Test the quote-stripping logic used in record_debug for messages
        fn strip_quotes(formatted: &str) -> String {
            formatted
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(formatted)
                .to_string()
        }

        // String debug formatting adds quotes
        assert_eq!(strip_quotes(&format!("{:?}", "hello")), "hello");

        // Non-string debug formatting doesn't have quotes to strip
        assert_eq!(strip_quotes(&format!("{:?}", 42)), "42");

        // Already unquoted string stays the same
        assert_eq!(strip_quotes("no quotes"), "no quotes");
    }
}
