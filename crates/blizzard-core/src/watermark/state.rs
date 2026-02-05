//! Watermark state tracking for operational visibility.
//!
//! Provides state tracking for watermark-based file processing,
//! distinguishing between initial startup, active processing, and idle states.

use serde::{Deserialize, Serialize};

/// Watermark state for operational visibility.
///
/// Tracks not just the position but also the activity state:
/// - `Initial`: Cold start, no watermark yet
/// - `Active`: Actively processing files at a specific position
/// - `Idle`: No new files found above the current watermark
///
/// # Serialization
///
/// Serializes as a tagged enum for clear JSON representation:
/// ```json
/// {"state": "Active", "value": "date=2026-01-28/file.parquet"}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "state", content = "value")]
pub enum WatermarkState {
    /// Cold start - no watermark yet.
    #[default]
    Initial,
    /// Active processing - at specific file path.
    Active(String),
    /// Idle - no new files found above watermark.
    Idle(String),
}

impl WatermarkState {
    /// Create a new Active watermark state.
    pub fn active(path: impl Into<String>) -> Self {
        Self::Active(path.into())
    }

    /// Create a new Idle watermark state.
    pub fn idle(path: impl Into<String>) -> Self {
        Self::Idle(path.into())
    }

    /// Extract the path from Active or Idle states.
    pub fn path(&self) -> Option<&str> {
        match self {
            WatermarkState::Initial => None,
            WatermarkState::Active(path) | WatermarkState::Idle(path) => Some(path),
        }
    }

    /// Check if this is the Initial state.
    pub fn is_initial(&self) -> bool {
        matches!(self, WatermarkState::Initial)
    }

    /// Check if this is the Active state.
    pub fn is_active(&self) -> bool {
        matches!(self, WatermarkState::Active(_))
    }

    /// Check if this is the Idle state.
    pub fn is_idle(&self) -> bool {
        matches!(self, WatermarkState::Idle(_))
    }

    /// Transition to Idle state if currently Active.
    /// Returns true if the state was changed.
    pub fn mark_idle(&mut self) -> bool {
        match self {
            WatermarkState::Active(path) => {
                let path = std::mem::take(path);
                *self = WatermarkState::Idle(path);
                true
            }
            _ => false,
        }
    }

    /// Transition to Active state with a new path.
    ///
    /// This can be called from any state (Initial, Active, or Idle).
    /// Returns true if the path was different (state actually changed).
    pub fn set_active(&mut self, new_path: impl Into<String>) -> bool {
        let new_path = new_path.into();
        let changed = self.path() != Some(&new_path);
        *self = WatermarkState::Active(new_path);
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_state_default() {
        let state = WatermarkState::default();
        assert!(state.is_initial());
        assert!(state.path().is_none());
    }

    #[test]
    fn test_watermark_state_active() {
        let state = WatermarkState::active("date=2026-01-28/file.parquet");
        assert!(state.is_active());
        assert_eq!(state.path(), Some("date=2026-01-28/file.parquet"));
    }

    #[test]
    fn test_watermark_state_idle() {
        let state = WatermarkState::idle("date=2026-01-28/file.parquet");
        assert!(state.is_idle());
        assert_eq!(state.path(), Some("date=2026-01-28/file.parquet"));
    }

    #[test]
    fn test_mark_idle_from_active() {
        let mut state = WatermarkState::active("file.parquet");
        assert!(state.mark_idle());
        assert!(state.is_idle());
        assert_eq!(state.path(), Some("file.parquet"));
    }

    #[test]
    fn test_mark_idle_from_idle_noop() {
        let mut state = WatermarkState::idle("file.parquet");
        assert!(!state.mark_idle());
        assert!(state.is_idle());
    }

    #[test]
    fn test_mark_idle_from_initial_noop() {
        let mut state = WatermarkState::Initial;
        assert!(!state.mark_idle());
        assert!(state.is_initial());
    }

    #[test]
    fn test_set_active_from_initial() {
        let mut state = WatermarkState::Initial;
        assert!(state.set_active("file.parquet"));
        assert!(state.is_active());
        assert_eq!(state.path(), Some("file.parquet"));
    }

    #[test]
    fn test_set_active_from_idle() {
        let mut state = WatermarkState::idle("old.parquet");
        assert!(state.set_active("new.parquet"));
        assert!(state.is_active());
        assert_eq!(state.path(), Some("new.parquet"));
    }

    #[test]
    fn test_set_active_same_path() {
        let mut state = WatermarkState::active("file.parquet");
        assert!(!state.set_active("file.parquet"));
        assert!(state.is_active());
    }

    #[test]
    fn test_serialization() {
        let state = WatermarkState::active("date=2026-01-28/file.parquet");
        let json = serde_json::to_string(&state).unwrap();
        assert!(json.contains("\"state\":\"Active\""));
        assert!(json.contains("\"value\":\"date=2026-01-28/file.parquet\""));

        let restored: WatermarkState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, restored);
    }

    #[test]
    fn test_serialization_initial() {
        let state = WatermarkState::Initial;
        let json = serde_json::to_string(&state).unwrap();
        assert!(json.contains("\"state\":\"Initial\""));

        let restored: WatermarkState = serde_json::from_str(&json).unwrap();
        assert_eq!(state, restored);
    }
}
