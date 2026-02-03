//! Shared orchestration primitives for multi-component pipelines.
//!
//!
//! This module provides common abstractions for building and running
//! pipelines with multiple components. It includes:
//!
//! - [`Task`] - A wrapper around an async task with component identification
//! - [`PiecesBuilder`] - A trait for building pipeline pieces from configuration
//! - [`RunningTopology`] - A trait for managing running pipelines
//! - [`Pipeline`] - A trait for self-contained pipeline units
//! - [`PipelineContext`] - Shared resources for pipeline execution
//! - [`PipelineRunner`] - Orchestration for multiple pipelines
//!
//! These primitives are used by both blizzard and penguin to implement
//! their respective multi-pipeline and multi-table orchestration.

mod builder;
mod pipeline;
mod running;
mod task;

pub use builder::PiecesBuilder;
pub use pipeline::{Pipeline, PipelineContext, PipelineRunner, random_jitter, run_pipelines};
pub use running::RunningTopology;
pub use task::{Task, TaskError, TaskOutput, TaskResult};
