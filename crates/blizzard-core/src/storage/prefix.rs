//! Date-based prefix generation for partition filtering.
//!
//! Generates date prefixes from strftime-style templates to enable
//! efficient listing of partitioned data in cloud storage.

use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use crate::config::StringOrVec;

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

/// Expand `{key}` placeholders in prefixes using include filter values.
///
/// Walks each prefix segment-by-segment. For consecutive `{key}` segments that
/// have matching include entries, substitutes with all values (cartesian product).
/// Stops at the first `{key}` without a matching include entry.
///
/// Returns `(expanded_prefixes, remaining_filters)` where remaining filters are
/// include entries for keys after a gap or keys not present as `{key}` in the
/// template at all.
pub fn expand_include_prefixes(
    prefixes: &[String],
    include: &HashMap<String, StringOrVec>,
) -> (Vec<String>, HashMap<String, Vec<String>>) {
    if include.is_empty() {
        return (prefixes.to_vec(), HashMap::new());
    }

    let mut result = Vec::new();
    let mut remaining: HashMap<String, Vec<String>> = HashMap::new();

    for prefix in prefixes {
        let segments: Vec<&str> = prefix.split('/').collect();

        // Find how far we can extend: walk segments, substituting {key} with
        // include values until we hit a {key} without a match.
        // Accumulates the cartesian product of all substituted segments so far.
        // Each entry is a vector of segments.
        let mut combos: Vec<Vec<String>> = vec![vec![]];
        let mut hit_gap = false;

        for (i, seg) in segments.iter().enumerate() {
            if let Some(key) = seg.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
                if !hit_gap {
                    if let Some(values) = include.get(key) {
                        // Expand cartesian product
                        let vals = values.values();
                        let mut new_combos = Vec::with_capacity(combos.len() * vals.len());
                        for combo in &combos {
                            for val in vals {
                                let mut c = combo.clone();
                                c.push(val.clone());
                                new_combos.push(c);
                            }
                        }
                        combos = new_combos;
                        continue;
                    } else {
                        // Gap: this {key} has no include entry.
                        hit_gap = true;
                    }
                }
                // After gap: any remaining {key} segments with include entries
                // become client-side filters.
                if let Some(values) = include.get(key) {
                    remaining
                        .entry(key.to_string())
                        .or_insert_with(|| values.values().to_vec());
                }
            } else if !hit_gap {
                // Literal segment — append to all combos
                for combo in &mut combos {
                    combo.push(seg.to_string());
                }
            } else {
                // Literal segment after gap: stop extending, but we already
                // capture remaining {key} filters above. Nothing to do for
                // literal segments after the gap.
                let _ = i; // satisfy unused variable
            }
        }

        // Build the final expanded prefixes from combos
        for combo in combos {
            result.push(combo.join("/"));
        }
    }

    // Also add include keys that don't appear as {key} in the template at all
    // to remaining filters. Only need to check one prefix (all share the same
    // template structure).
    if let Some(prefix) = prefixes.first() {
        for (key, values) in include {
            let placeholder = format!("{{{key}}}");
            if !prefix.contains(&placeholder) {
                remaining
                    .entry(key.clone())
                    .or_insert_with(|| values.values().to_vec());
            }
        }
    }

    (result, remaining)
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
    fn test_has_hour_granularity() {
        let generator_with_hour = DatePrefixGenerator::new("date=%Y-%m-%d/hour=%H", 1);
        assert!(generator_with_hour.has_hour_granularity());

        let generator_date_only = DatePrefixGenerator::new("date=%Y-%m-%d", 1);
        assert!(!generator_date_only.has_hour_granularity());
    }

    // Tests for expand_include_prefixes

    fn include_map(entries: &[(&str, &[&str])]) -> HashMap<String, StringOrVec> {
        entries
            .iter()
            .map(|(k, v)| {
                let values = if v.len() == 1 {
                    StringOrVec::Single(v[0].to_string())
                } else {
                    StringOrVec::Multiple(v.iter().map(|s| s.to_string()).collect())
                };
                (k.to_string(), values)
            })
            .collect()
    }

    #[test]
    fn test_expand_include_full_match() {
        let prefixes = vec!["2026/02/03/{host}/{exchange}".to_string()];
        let include = include_map(&[("host", &["a", "b"]), ("exchange", &["X"])]);

        let (expanded, remaining) = expand_include_prefixes(&prefixes, &include);

        assert_eq!(expanded.len(), 2);
        assert!(expanded.contains(&"2026/02/03/a/X".to_string()));
        assert!(expanded.contains(&"2026/02/03/b/X".to_string()));
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_expand_include_gap() {
        // {host} has include, {exchange} does not, {symbol} has include
        // Should stop at {exchange}, symbol becomes remaining filter
        let prefixes = vec!["2026/02/03/{host}/{exchange}/{symbol}".to_string()];
        let include = include_map(&[("host", &["a"]), ("symbol", &["S"])]);

        let (expanded, remaining) = expand_include_prefixes(&prefixes, &include);

        assert_eq!(expanded, vec!["2026/02/03/a"]);
        assert_eq!(remaining.get("symbol").unwrap(), &vec!["S".to_string()]);
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_expand_include_cartesian_product() {
        let prefixes = vec!["2026/02/03/{host}/{exchange}/{symbol}".to_string()];
        let include = include_map(&[
            ("host", &["h1", "h2"]),
            ("exchange", &["BINANCE"]),
            ("symbol", &["BTC", "ETH"]),
        ]);

        let (mut expanded, remaining) = expand_include_prefixes(&prefixes, &include);
        expanded.sort();

        assert_eq!(expanded.len(), 4); // 2 hosts × 1 exchange × 2 symbols
        assert_eq!(
            expanded,
            vec![
                "2026/02/03/h1/BINANCE/BTC",
                "2026/02/03/h1/BINANCE/ETH",
                "2026/02/03/h2/BINANCE/BTC",
                "2026/02/03/h2/BINANCE/ETH",
            ]
        );
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_expand_include_empty_include() {
        let prefixes = vec!["2026/02/03/{host}".to_string()];
        let include = HashMap::new();

        let (expanded, remaining) = expand_include_prefixes(&prefixes, &include);

        assert_eq!(expanded, vec!["2026/02/03/{host}"]);
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_expand_include_no_placeholders() {
        let prefixes = vec!["2026/02/03".to_string()];
        let include = include_map(&[("host", &["a"])]);

        let (expanded, remaining) = expand_include_prefixes(&prefixes, &include);

        assert_eq!(expanded, vec!["2026/02/03"]);
        assert_eq!(remaining.get("host").unwrap(), &vec!["a".to_string()]);
    }

    #[test]
    fn test_expand_include_key_not_in_template() {
        // Include has "region" but template only has {host}
        let prefixes = vec!["2026/02/03/{host}".to_string()];
        let include = include_map(&[("host", &["a"]), ("region", &["us-east-1"])]);

        let (expanded, remaining) = expand_include_prefixes(&prefixes, &include);

        assert_eq!(expanded, vec!["2026/02/03/a"]);
        assert_eq!(
            remaining.get("region").unwrap(),
            &vec!["us-east-1".to_string()]
        );
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_expand_include_multiple_date_prefixes() {
        let prefixes = vec![
            "2026/02/02/{host}".to_string(),
            "2026/02/03/{host}".to_string(),
        ];
        let include = include_map(&[("host", &["a", "b"])]);

        let (mut expanded, remaining) = expand_include_prefixes(&prefixes, &include);
        expanded.sort();

        assert_eq!(expanded.len(), 4);
        assert_eq!(
            expanded,
            vec![
                "2026/02/02/a",
                "2026/02/02/b",
                "2026/02/03/a",
                "2026/02/03/b",
            ]
        );
        assert!(remaining.is_empty());
    }

    #[test]
    fn test_expand_include_first_placeholder_has_no_match() {
        // {host} has no include entry — gap immediately, everything becomes remaining
        let prefixes = vec!["2026/02/03/{host}/{exchange}".to_string()];
        let include = include_map(&[("exchange", &["BINANCE"])]);

        let (expanded, remaining) = expand_include_prefixes(&prefixes, &include);

        assert_eq!(expanded, vec!["2026/02/03"]);
        assert_eq!(
            remaining.get("exchange").unwrap(),
            &vec!["BINANCE".to_string()]
        );
    }
}
