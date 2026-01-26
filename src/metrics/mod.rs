//! Metrics and observability infrastructure for Blizzard.
//!
//! This module groups all observability-related components:
//! - `events`: Internal event types and the `InternalEvent` trait
//! - `server`: Prometheus HTTP server and initialization
//! - `utilization`: CPU utilization tracking with EWMA smoothing

pub mod events;
pub mod server;
pub mod utilization;

// Re-export commonly used items
pub use server::init;
pub use utilization::UtilizationTimer;

///
/// This macro calls the `InternalEvent::emit()` method on the given event,
/// which records the corresponding Prometheus counter metric.
///
/// # Example
///
/// ```ignore
/// use blizzard::metrics::{events::RecordsProcessed, events::BytesWritten};
///
/// emit!(RecordsProcessed { count: 100 });
/// emit!(BytesWritten { bytes: 1024 });
/// ```
#[macro_export]
macro_rules! emit {
    ($event:expr) => {
        $crate::metrics::events::InternalEvent::emit($event)
    };
}
