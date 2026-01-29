//! Task wrapper for component execution.
//!
//!
//! Provides a unified way to represent async tasks with component
//! identification and error handling.

use std::fmt;
use std::future::Future;
use std::pin::Pin;

/// A boxed future that produces a TaskResult.
pub type BoxFuture<'a> = Pin<Box<dyn Future<Output = TaskResult> + Send + 'a>>;

/// Result type for task execution.
pub type TaskResult = Result<TaskOutput, TaskError>;

/// Successful output from a task.
///
/// Tasks can optionally return statistics about their execution.
#[derive(Debug, Default)]
pub struct TaskOutput {
    /// Optional message describing what the task accomplished.
    pub message: Option<String>,
}

impl TaskOutput {
    /// Create an empty task output.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Create a task output with a message.
    pub fn with_message(message: impl Into<String>) -> Self {
        Self {
            message: Some(message.into()),
        }
    }
}

/// Error type for task execution.
#[derive(Debug)]
pub enum TaskError {
    /// Task was cancelled via shutdown signal.
    Cancelled,
    /// Task panicked during execution.
    Panicked(String),
    /// Task failed with an error.
    Failed(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => write!(f, "task cancelled"),
            Self::Panicked(msg) => write!(f, "task panicked: {}", msg),
            Self::Failed(err) => write!(f, "task failed: {}", err),
        }
    }
}

impl std::error::Error for TaskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Failed(err) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl TaskError {
    /// Create a Failed error from any error type.
    pub fn failed<E>(err: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Failed(Box::new(err))
    }

    /// Check if this is a cancellation error.
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    /// Check if this is a panic error.
    pub fn is_panicked(&self) -> bool {
        matches!(self, Self::Panicked(_))
    }
}

/// A task representing a component's async execution.
///
/// Tasks wrap futures with component identification for better
/// error reporting and observability.
///
/// # Type Parameters
///
/// - `K`: The key type used to identify components (e.g., `TableKey`, `PipelineKey`)
pub struct Task<K> {
    /// The component key that owns this task.
    pub key: K,
    /// The async future to execute.
    pub future: BoxFuture<'static>,
    /// A type tag for logging/metrics (e.g., "table", "pipeline").
    pub typetag: &'static str,
}

impl<K> Task<K> {
    /// Create a new task.
    pub fn new<F>(key: K, typetag: &'static str, future: F) -> Self
    where
        F: Future<Output = TaskResult> + Send + 'static,
    {
        Self {
            key,
            future: Box::pin(future),
            typetag,
        }
    }
}

impl<K: fmt::Debug> fmt::Debug for Task<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task")
            .field("key", &self.key)
            .field("typetag", &self.typetag)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_output_empty() {
        let output = TaskOutput::empty();
        assert!(output.message.is_none());
    }

    #[test]
    fn test_task_output_with_message() {
        let output = TaskOutput::with_message("processed 100 files");
        assert_eq!(output.message.as_deref(), Some("processed 100 files"));
    }

    #[test]
    fn test_task_error_display() {
        assert_eq!(format!("{}", TaskError::Cancelled), "task cancelled");
        assert_eq!(
            format!("{}", TaskError::Panicked("oops".to_string())),
            "task panicked: oops"
        );

        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = TaskError::failed(io_err);
        assert!(format!("{}", err).contains("task failed"));
    }

    #[test]
    fn test_task_error_is_cancelled() {
        assert!(TaskError::Cancelled.is_cancelled());
        assert!(!TaskError::Panicked("x".to_string()).is_cancelled());
    }

    #[test]
    fn test_task_error_is_panicked() {
        assert!(TaskError::Panicked("x".to_string()).is_panicked());
        assert!(!TaskError::Cancelled.is_panicked());
    }

    #[test]
    fn test_task_creation() {
        let task = Task::new("my-key", "test", async { Ok(TaskOutput::empty()) });
        assert_eq!(task.key, "my-key");
        assert_eq!(task.typetag, "test");
    }

    #[test]
    fn test_task_debug() {
        let task = Task::new("my-key", "test", async { Ok(TaskOutput::empty()) });
        let debug = format!("{:?}", task);
        assert!(debug.contains("my-key"));
        assert!(debug.contains("test"));
    }
}
