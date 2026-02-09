//! Metrics for penguin (Delta Lake operations).
//!
//! This module contains penguin-specific metric events for Delta Lake commits,
//! schema evolution, and table operations.

pub mod events;

pub use events::*;
