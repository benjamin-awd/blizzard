//! Metrics for penguin (Delta Lake operations).
//!
//! This module contains penguin-specific metric events for Delta Lake commits,
//! schema evolution, and table operations.

pub mod events;

pub use events::*;

/// Macro for emitting penguin metric events.
#[macro_export]
macro_rules! emit {
    ($event:expr) => {
        <_ as $crate::metrics::events::InternalEvent>::emit($event)
    };
}
