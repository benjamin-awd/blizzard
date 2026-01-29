//! Date-based prefix generation for partition filtering.
//!
//! Generates date prefixes from strftime-style templates to enable
//! efficient listing of partitioned data in cloud storage.

use chrono::{DateTime, Duration, Utc};

/// Generates date-based prefixes for partition filtering.
///
/// Supports strftime-style templates with common date/time codes:
/// - `%Y` - 4-digit year (e.g., 2026)
/// - `%m` - 2-digit month (01-12)
/// - `%d` - 2-digit day (01-31)
/// - `%H` - 2-digit hour (00-23)
///
/// # Example
/// ```ignore
/// let generator = DatePrefixGenerator::new("date=%Y-%m-%d", 2);
/// let prefixes = generator.generate_prefixes();
/// // Returns: ["date=2026-01-26", "date=2026-01-27", "date=2026-01-28"]
/// ```
pub struct DatePrefixGenerator {
    template: String,
    lookback: u32,
}

impl DatePrefixGenerator {
    /// Create a new prefix generator.
    ///
    /// # Arguments
    /// * `template` - strftime-style template (e.g., "date=%Y-%m-%d/hour=%H")
    /// * `lookback` - number of units to look back. If the template contains `%H`,
    ///   this is interpreted as hours; otherwise as days. (0 = current unit only)
    pub fn new(template: &str, lookback: u32) -> Self {
        Self {
            template: template.to_owned(),
            lookback,
        }
    }

    /// Check if the template contains hour-level granularity (%H).
    pub fn has_hour_granularity(&self) -> bool {
        self.template.contains("%H")
    }

    /// Generate prefixes based on the template.
    ///
    /// If the template contains `%H`, generates hour-level prefixes with
    /// `lookback` interpreted as hours.
    /// Otherwise, generates day-level prefixes with `lookback` as days.
    pub fn generate_prefixes(&self) -> Vec<String> {
        if self.has_hour_granularity() {
            self.generate_prefixes_with_hours()
        } else {
            self.generate_date_only_prefixes()
        }
    }

    /// Generate day-level prefixes (no hour component).
    fn generate_date_only_prefixes(&self) -> Vec<String> {
        let now = Utc::now();
        self.generate_date_only_prefixes_from(now)
    }

    /// Generate day-level prefixes from a specific timestamp (for testing).
    fn generate_date_only_prefixes_from(&self, now: DateTime<Utc>) -> Vec<String> {
        let mut prefixes = Vec::with_capacity((self.lookback + 1) as usize);

        for days_back in 0..=self.lookback {
            let date = now - Duration::days(days_back as i64);
            let prefix = date.format(&self.template).to_string();
            prefixes.push(prefix);
        }

        // Sort chronologically (oldest first) for consistent ordering
        prefixes.reverse();
        prefixes
    }

    /// Generate hour-level prefixes.
    ///
    /// Lookback is interpreted as hours. For example, lookback=2 at 14:00
    /// generates prefixes for hours 12, 13, and 14.
    fn generate_prefixes_with_hours(&self) -> Vec<String> {
        let now = Utc::now();
        self.generate_prefixes_with_hours_from(now)
    }

    /// Generate hour-level prefixes from a specific timestamp (for testing).
    fn generate_prefixes_with_hours_from(&self, now: DateTime<Utc>) -> Vec<String> {
        let mut prefixes = Vec::with_capacity((self.lookback + 1) as usize);

        for hours_back in 0..=self.lookback {
            let datetime = now - Duration::hours(hours_back as i64);
            let prefix = datetime.format(&self.template).to_string();
            prefixes.push(prefix);
        }

        // Sort chronologically (oldest first) for consistent ordering
        prefixes.reverse();
        prefixes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_date_only_prefixes() {
        let generator = DatePrefixGenerator::new("date=%Y-%m-%d", 2);
        // Use a fixed time: 2026-01-28 14:30:00 UTC
        let now = Utc.with_ymd_and_hms(2026, 1, 28, 14, 30, 0).unwrap();
        let prefixes = generator.generate_date_only_prefixes_from(now);

        assert_eq!(prefixes.len(), 3);
        assert_eq!(prefixes[0], "date=2026-01-26");
        assert_eq!(prefixes[1], "date=2026-01-27");
        assert_eq!(prefixes[2], "date=2026-01-28");
    }

    #[test]
    fn test_date_only_prefixes_zero_lookback() {
        let generator = DatePrefixGenerator::new("date=%Y-%m-%d", 0);
        let now = Utc.with_ymd_and_hms(2026, 1, 28, 14, 30, 0).unwrap();
        let prefixes = generator.generate_date_only_prefixes_from(now);

        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], "date=2026-01-28");
    }

    #[test]
    fn test_hour_prefixes_lookback_hours() {
        let generator = DatePrefixGenerator::new("date=%Y-%m-%d/hour=%H", 2);
        // 14:30 UTC - should generate hours 12, 13, 14 (lookback=2 means 2 hours back)
        let now = Utc.with_ymd_and_hms(2026, 1, 28, 14, 30, 0).unwrap();
        let prefixes = generator.generate_prefixes_with_hours_from(now);

        assert_eq!(prefixes.len(), 3); // hours 12, 13, 14
        assert_eq!(prefixes[0], "date=2026-01-28/hour=12");
        assert_eq!(prefixes[1], "date=2026-01-28/hour=13");
        assert_eq!(prefixes[2], "date=2026-01-28/hour=14");
    }

    #[test]
    fn test_hour_prefixes_crosses_midnight() {
        let generator = DatePrefixGenerator::new("date=%Y-%m-%d/hour=%H", 3);
        // 02:00 UTC on Jan 28 - lookback=3 goes back to 23:00 on Jan 27
        let now = Utc.with_ymd_and_hms(2026, 1, 28, 2, 0, 0).unwrap();
        let prefixes = generator.generate_prefixes_with_hours_from(now);

        assert_eq!(prefixes.len(), 4);
        assert_eq!(prefixes[0], "date=2026-01-27/hour=23");
        assert_eq!(prefixes[1], "date=2026-01-28/hour=00");
        assert_eq!(prefixes[2], "date=2026-01-28/hour=01");
        assert_eq!(prefixes[3], "date=2026-01-28/hour=02");
    }

    #[test]
    fn test_hour_prefixes_zero_lookback() {
        let generator = DatePrefixGenerator::new("date=%Y-%m-%d/hour=%H", 0);
        let now = Utc.with_ymd_and_hms(2026, 1, 28, 14, 30, 0).unwrap();
        let prefixes = generator.generate_prefixes_with_hours_from(now);

        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], "date=2026-01-28/hour=14");
    }

    #[test]
    fn test_has_hour_granularity() {
        let generator_with_hour = DatePrefixGenerator::new("date=%Y-%m-%d/hour=%H", 1);
        assert!(generator_with_hour.has_hour_granularity());

        let generator_date_only = DatePrefixGenerator::new("date=%Y-%m-%d", 1);
        assert!(!generator_date_only.has_hour_granularity());
    }

    #[test]
    fn test_generate_prefixes_auto_detects_granularity() {
        let now = Utc.with_ymd_and_hms(2026, 1, 28, 14, 30, 0).unwrap();

        // Date-only template - lookback=1 means 1 day back
        let generator = DatePrefixGenerator::new("date=%Y-%m-%d", 1);
        let prefixes = generator.generate_date_only_prefixes_from(now);
        assert_eq!(prefixes.len(), 2);
        assert_eq!(prefixes[0], "date=2026-01-27");
        assert_eq!(prefixes[1], "date=2026-01-28");

        // Hour template - lookback=1 means 1 hour back
        let generator = DatePrefixGenerator::new("date=%Y-%m-%d/hour=%H", 1);
        let prefixes = generator.generate_prefixes_with_hours_from(now);
        assert_eq!(prefixes.len(), 2);
        assert_eq!(prefixes[0], "date=2026-01-28/hour=13");
        assert_eq!(prefixes[1], "date=2026-01-28/hour=14");
    }

    #[test]
    fn test_custom_template_format() {
        let generator = DatePrefixGenerator::new("year=%Y/month=%m/day=%d", 1);
        let now = Utc.with_ymd_and_hms(2026, 1, 28, 14, 30, 0).unwrap();
        let prefixes = generator.generate_date_only_prefixes_from(now);

        assert_eq!(prefixes.len(), 2);
        assert_eq!(prefixes[0], "year=2026/month=01/day=27");
        assert_eq!(prefixes[1], "year=2026/month=01/day=28");
    }

    #[test]
    fn test_month_boundary_crossing() {
        let generator = DatePrefixGenerator::new("date=%Y-%m-%d", 2);
        // First day of February
        let now = Utc.with_ymd_and_hms(2026, 2, 1, 10, 0, 0).unwrap();
        let prefixes = generator.generate_date_only_prefixes_from(now);

        assert_eq!(prefixes.len(), 3);
        assert_eq!(prefixes[0], "date=2026-01-30");
        assert_eq!(prefixes[1], "date=2026-01-31");
        assert_eq!(prefixes[2], "date=2026-02-01");
    }
}
