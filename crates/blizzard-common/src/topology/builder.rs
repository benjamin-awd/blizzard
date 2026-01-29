//! Builder trait for topology construction.
//!
//!
//! Provides a common interface for building pipeline pieces from configuration.

use async_trait::async_trait;

/// Trait for building pipeline pieces from configuration.
///
/// This trait defines the interface for constructing all the components
/// needed to run a pipeline. Implementations take configuration and
/// produce "pieces" that can then be assembled into a running topology.
///
/// # Type Parameters
///
/// - `Pieces`: The type of pieces produced (e.g., processors, tasks)
/// - `Error`: The error type that can occur during building
///
/// # Examples
///
/// ```ignore
/// // In penguin:
/// impl PiecesBuilder for PipelinePiecesBuilder<'_> {
///     type Pieces = PipelinePieces;
///     type Error = PipelineError;
///
///     async fn build(self) -> Result<Self::Pieces, Self::Error> {
///         // Build processors for each table...
///     }
/// }
/// ```
#[async_trait]
pub trait PiecesBuilder {
    /// The type of pieces produced by this builder.
    type Pieces;
    /// The error type that can occur during building.
    type Error;

    /// Build pipeline pieces from the configuration.
    ///
    /// This method consumes the builder and produces the pieces
    /// needed to start the pipeline. If building any component fails,
    /// the entire build fails.
    async fn build(self) -> Result<Self::Pieces, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Simple test implementation
    struct TestBuilder {
        should_fail: bool,
    }

    struct TestPieces {
        count: usize,
    }

    #[derive(Debug)]
    struct TestError;

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "test error")
        }
    }

    impl std::error::Error for TestError {}

    #[async_trait]
    impl PiecesBuilder for TestBuilder {
        type Pieces = TestPieces;
        type Error = TestError;

        async fn build(self) -> Result<Self::Pieces, Self::Error> {
            if self.should_fail {
                Err(TestError)
            } else {
                Ok(TestPieces { count: 42 })
            }
        }
    }

    #[tokio::test]
    async fn test_builder_success() {
        let builder = TestBuilder { should_fail: false };
        let pieces = builder.build().await.unwrap();
        assert_eq!(pieces.count, 42);
    }

    #[tokio::test]
    async fn test_builder_failure() {
        let builder = TestBuilder { should_fail: true };
        let result = builder.build().await;
        assert!(result.is_err());
    }
}
