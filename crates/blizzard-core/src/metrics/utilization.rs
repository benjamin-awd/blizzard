//! Utilization tracking for pipeline components.
//!
//! Tracks the ratio of working time vs. waiting time for each component
//! using EWMA smoothing for stable readings.

use metrics::gauge;
use std::time::{Duration, Instant};

const EMISSION_INTERVAL: Duration = Duration::from_secs(5);

/// EWMA calculator for smoothing utilization values.
pub struct Ewma {
    average: Option<f64>,
    alpha: f64,
}

impl Ewma {
    pub fn new(alpha: f64) -> Self {
        Self {
            average: None,
            alpha,
        }
    }

    pub fn update(&mut self, point: f64) -> f64 {
        let average = match self.average {
            None => point,
            Some(avg) => point.mul_add(self.alpha, avg * (1.0 - self.alpha)),
        };
        self.average = Some(average);
        average
    }
}

/// Timer that tracks utilization for a component.
pub struct UtilizationTimer {
    overall_start: Instant,
    span_start: Instant,
    waiting: bool,
    total_wait: Duration,
    ewma: Ewma,
    gauge: metrics::Gauge,
}

impl UtilizationTimer {
    pub fn new(target: &str) -> Self {
        let now = Instant::now();
        Self {
            overall_start: now,
            span_start: now,
            waiting: true, // Start in waiting state
            total_wait: Duration::ZERO,
            ewma: Ewma::new(0.9),
            gauge: gauge!("blizzard_utilization", "target" => target.to_owned()),
        }
    }

    /// Mark transition to waiting state.
    pub fn start_wait(&mut self) {
        if !self.waiting {
            self.waiting = true;
            self.span_start = Instant::now();
        }
    }

    /// Mark transition to working state.
    pub fn stop_wait(&mut self) {
        if self.waiting {
            self.total_wait += self.span_start.elapsed();
            self.waiting = false;
            self.span_start = Instant::now();
        }
    }

    /// Update and emit utilization metric (call periodically).
    pub fn maybe_update(&mut self) {
        if self.overall_start.elapsed() < EMISSION_INTERVAL {
            return;
        }
        self.update();
    }

    /// Force an immediate utilization update regardless of emission interval.
    ///
    /// Returns the EWMA-smoothed utilization value (0.0–1.0).
    pub fn force_update(&mut self) -> f64 {
        self.update()
    }

    fn update(&mut self) -> f64 {
        let now = Instant::now();

        // Account for current span
        let mut total_wait = self.total_wait;
        if self.waiting {
            total_wait += now.duration_since(self.span_start);
        }

        let total_duration = now.duration_since(self.overall_start);
        let wait_ratio = total_wait.as_secs_f64() / total_duration.as_secs_f64();
        let utilization = (1.0 - wait_ratio).clamp(0.0, 1.0);

        // Apply EWMA and emit
        let avg = self.ewma.update(utilization);
        let rounded = (avg * 10000.0).round() / 10000.0;
        self.gauge.set(rounded);

        // Reset for next period
        self.overall_start = now;
        self.span_start = now;
        self.total_wait = Duration::ZERO;

        rounded
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::init_test;

    #[test]
    fn test_ewma_first_point() {
        let mut ewma = Ewma::new(0.9);
        assert_eq!(ewma.update(0.5), 0.5);
    }

    #[test]
    fn test_ewma_smoothing() {
        let mut ewma = Ewma::new(0.9);
        ewma.update(1.0);
        // Second point: 0.0 * 0.9 + 1.0 * 0.1 = 0.1
        let result = ewma.update(0.0);
        assert!((result - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_persistent_timer_reports_nonzero_for_short_iterations() {
        init_test();
        let mut timer = UtilizationTimer::new("test-short");

        // Simulate multiple short iterations (work + wait cycles) where each
        // individual cycle is well under the 5s emission interval. Before the
        // fix, a timer recreated per-iteration would never call update().
        for _ in 0..5 {
            // Iteration start: transition to working
            timer.stop_wait();
            // Simulate brief work
            std::thread::sleep(Duration::from_millis(10));
            // Iteration end: transition to waiting (poll sleep)
            timer.start_wait();
            std::thread::sleep(Duration::from_millis(10));
        }

        // Force an update (simulates what happens when enough time has passed)
        let utilization = timer.force_update();

        // Timer spent roughly equal time working and waiting, so utilization
        // should be around 0.5 — the key assertion is that it's not zero.
        assert!(
            utilization > 0.0,
            "utilization should be non-zero after work/wait cycles, got {utilization}"
        );
        assert!(
            utilization <= 1.0,
            "utilization should be at most 1.0, got {utilization}"
        );
    }

    #[test]
    fn test_maybe_update_skips_before_emission_interval() {
        init_test();
        let mut timer = UtilizationTimer::new("test-skip");

        timer.stop_wait();
        std::thread::sleep(Duration::from_millis(10));
        timer.start_wait();

        // maybe_update should be a no-op since < 5s have elapsed
        timer.maybe_update();

        // force_update should still report the accumulated utilization
        let utilization = timer.force_update();
        assert!(
            utilization > 0.0,
            "force_update should report utilization even when maybe_update skips"
        );
    }

    #[test]
    fn test_fully_idle_timer_reports_zero() {
        init_test();
        let mut timer = UtilizationTimer::new("test-idle");

        // Never call stop_wait — timer stays in waiting state
        std::thread::sleep(Duration::from_millis(10));

        let utilization = timer.force_update();
        assert!(
            utilization < 0.01,
            "idle timer should report near-zero utilization, got {utilization}"
        );
    }

    #[test]
    fn test_fully_busy_timer_reports_near_one() {
        init_test();
        let mut timer = UtilizationTimer::new("test-busy");

        // Immediately transition to working and stay there
        timer.stop_wait();
        std::thread::sleep(Duration::from_millis(10));

        let utilization = timer.force_update();
        assert!(
            utilization > 0.9,
            "fully busy timer should report near-1.0 utilization, got {utilization}"
        );
    }
}
