//! Rate-limited tracing layer.
//!
//! This module provides a tracing layer that rate-limits log events to prevent
//! log spam.
//!
//! # Behavior
//!
//! - Rate limiting is ON by default (opt-out with `internal_log_rate_limit = false`)
//! - Default window is 10 seconds (configurable via `with_default_limit`)
//! - Events are grouped by callsite + `component_id` field
//! - Within a time window:
//!   - 1st event: emitted normally
//!   - 2nd event: shows "rate limit exceeded" warning
//!   - 3rd+ events: silenced
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
//!     .with_default_limit(10);
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tracing_core::span::{Attributes, Id, Record};
use tracing_core::{Event, Interest, Metadata, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, Filter};
use tracing_subscriber::registry::LookupSpan;

/// Default rate limit window in seconds.
const DEFAULT_LIMIT_SECS: u64 = 10;

/// Key for rate limiting - combines callsite identifier with optional component_id.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct RateLimitKey {
    /// Callsite identifier (file:line or metadata address).
    callsite: CallsiteId,
    /// Optional component identifier for finer-grained limiting.
    component_id: Option<String>,
}

/// Callsite identifier.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct CallsiteId(u64);

impl CallsiteId {
    fn from_metadata(meta: &Metadata<'_>) -> Self {
        // Use pointer address of metadata as unique identifier
        Self(meta as *const _ as u64)
    }
}

/// State for a rate-limited event.
struct RateLimitState {
    /// When the current window started.
    window_start: Instant,
    /// Number of events seen in the current window.
    count: AtomicU64,
    /// Rate limit window for this event.
    limit_secs: u64,
}

impl RateLimitState {
    fn new(limit_secs: u64) -> Self {
        Self {
            window_start: Instant::now(),
            count: AtomicU64::new(0),
            limit_secs,
        }
    }

    /// Check if the window has expired.
    fn is_expired(&self) -> bool {
        self.window_start.elapsed() >= Duration::from_secs(self.limit_secs)
    }

    /// Increment count and return the new value.
    fn increment(&self) -> u64 {
        self.count.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Get the current count.
    fn get_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
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
            self.rate_limit_secs = Some(value as u64);
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
        if field.name() == "component_id" {
            self.component_id = Some(format!("{:?}", value));
        }
        // Note: message is handled by record_str; using Debug would add quotes
    }
}

/// Action to take for a rate-limited event.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RateLimitAction {
    /// Emit the event normally.
    Emit,
    /// Emit a "rate limit exceeded" warning.
    EmitWarning,
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
    /// Rate limit state for each key.
    state: DashMap<RateLimitKey, RateLimitState>,
}

impl<L> RateLimitedLayer<L> {
    /// Create a new rate-limited layer wrapping the given inner layer.
    pub fn new(inner: L) -> Self {
        Self {
            inner,
            default_limit_secs: DEFAULT_LIMIT_SECS,
            state: DashMap::new(),
        }
    }

    /// Set the default rate limit window in seconds.
    pub fn with_default_limit(mut self, secs: u64) -> Self {
        self.default_limit_secs = secs;
        self
    }

    /// Determine what action to take for an event.
    fn check_rate_limit(&self, key: RateLimitKey, limit_secs: u64) -> RateLimitAction {
        let mut entry = self
            .state
            .entry(key)
            .or_insert_with(|| RateLimitState::new(limit_secs));
        let state = entry.value_mut();

        // Check if window has expired
        if state.is_expired() {
            let suppressed = state.get_count().saturating_sub(2);

            // Reset state
            state.window_start = Instant::now();
            state.count.store(1, Ordering::Relaxed);
            state.limit_secs = limit_secs;

            if suppressed > 0 {
                return RateLimitAction::EmitSummary { suppressed };
            }
            return RateLimitAction::Emit;
        }

        // Increment count
        let count = state.increment();

        match count {
            1 => RateLimitAction::Emit,
            2 => RateLimitAction::EmitWarning,
            _ => RateLimitAction::Suppress,
        }
    }
}

impl<S, L> Layer<S> for RateLimitedLayer<L>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    L: Layer<S>,
{
    fn on_register_dispatch(&self, subscriber: &tracing_core::Dispatch) {
        self.inner.on_register_dispatch(subscriber);
    }

    fn on_layer(&mut self, subscriber: &mut S) {
        self.inner.on_layer(subscriber);
    }

    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        self.inner.register_callsite(metadata)
    }

    fn enabled(&self, metadata: &Metadata<'_>, ctx: Context<'_, S>) -> bool {
        self.inner.enabled(metadata, ctx)
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        self.inner.on_new_span(attrs, id, ctx);
    }

    fn on_record(&self, span: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        self.inner.on_record(span, values, ctx);
    }

    fn on_follows_from(&self, span: &Id, follows: &Id, ctx: Context<'_, S>) {
        self.inner.on_follows_from(span, follows, ctx);
    }

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
            callsite: CallsiteId::from_metadata(event.metadata()),
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
            RateLimitAction::EmitWarning => {
                // Don't emit the original event, just show rate limit warning
                let message = limit_visitor.message.as_deref().unwrap_or("<no message>");
                eprintln!("  Internal log [{}] is being rate limited.", message);
            }
            RateLimitAction::EmitSummary { suppressed } => {
                // Log summary of suppressed events with the message content
                let message = limit_visitor.message.as_deref().unwrap_or("<no message>");
                eprintln!(
                    "  Internal log [{}] {} messages were rate limited.",
                    message, suppressed
                );

                // Then emit the new event
                self.inner.on_event(event, ctx);
            }
            RateLimitAction::Suppress => {
                // Silently suppress
            }
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        self.inner.on_enter(id, ctx);
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        self.inner.on_exit(id, ctx);
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        self.inner.on_close(id, ctx);
    }

    fn on_id_change(&self, old: &Id, new: &Id, ctx: Context<'_, S>) {
        self.inner.on_id_change(old, new, ctx);
    }
}

/// A filter that can be used to rate-limit events without wrapping a layer.
///
/// This is useful when you want to apply rate limiting as a filter rather
/// than as a layer wrapper.
pub struct RateLimitedFilter {
    /// Default rate limit window in seconds.
    default_limit_secs: u64,
    /// Rate limit state for each key.
    state: DashMap<RateLimitKey, RateLimitState>,
}

impl RateLimitedFilter {
    /// Create a new rate-limited filter.
    pub fn new() -> Self {
        Self {
            default_limit_secs: DEFAULT_LIMIT_SECS,
            state: DashMap::new(),
        }
    }

    /// Set the default rate limit window in seconds.
    pub fn with_default_limit(mut self, secs: u64) -> Self {
        self.default_limit_secs = secs;
        self
    }

    /// Check if an event should be filtered (returns true if event should pass).
    fn should_emit(&self, key: RateLimitKey, limit_secs: u64) -> bool {
        let mut entry = self
            .state
            .entry(key)
            .or_insert_with(|| RateLimitState::new(limit_secs));
        let state = entry.value_mut();

        // Check if window has expired
        if state.is_expired() {
            // Reset state
            state.window_start = Instant::now();
            state.count.store(1, Ordering::Relaxed);
            state.limit_secs = limit_secs;
            return true;
        }

        // Increment count
        let count = state.increment();
        count <= 2 // Allow first two events
    }
}

impl Default for RateLimitedFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Filter<S> for RateLimitedFilter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        // We can't extract fields from metadata alone, so we always return true here
        // The actual filtering happens in event_enabled
        meta.is_event()
    }

    fn event_enabled(&self, event: &Event<'_>, _cx: &Context<'_, S>) -> bool {
        // Extract rate limit configuration
        let mut limit_visitor = LimitVisitor::new();
        event.record(&mut limit_visitor);

        // If rate limiting is disabled for this event, allow it
        if !limit_visitor.rate_limit_enabled {
            return true;
        }

        // Build rate limit key
        let key = RateLimitKey {
            callsite: CallsiteId::from_metadata(event.metadata()),
            component_id: limit_visitor.component_id,
        };

        // Determine rate limit window
        let limit_secs = limit_visitor
            .rate_limit_secs
            .unwrap_or(self.default_limit_secs);

        // Check rate limit
        self.should_emit(key, limit_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_key_equality() {
        let key1 = RateLimitKey {
            callsite: CallsiteId(123),
            component_id: Some("test".to_string()),
        };
        let key2 = RateLimitKey {
            callsite: CallsiteId(123),
            component_id: Some("test".to_string()),
        };
        let key3 = RateLimitKey {
            callsite: CallsiteId(123),
            component_id: Some("other".to_string()),
        };

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

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
    fn test_rate_limit_action_flow() {
        let layer: RateLimitedLayer<()> = RateLimitedLayer::new(()).with_default_limit(10);

        let key = RateLimitKey {
            callsite: CallsiteId(999),
            component_id: None,
        };

        // First event: emit
        assert_eq!(
            layer.check_rate_limit(key.clone(), 10),
            RateLimitAction::Emit
        );

        // Second event: warning
        assert_eq!(
            layer.check_rate_limit(key.clone(), 10),
            RateLimitAction::EmitWarning
        );

        // Third+ events: suppress
        assert_eq!(
            layer.check_rate_limit(key.clone(), 10),
            RateLimitAction::Suppress
        );
        assert_eq!(
            layer.check_rate_limit(key.clone(), 10),
            RateLimitAction::Suppress
        );
    }
}
