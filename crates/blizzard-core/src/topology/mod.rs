//! Shared orchestration primitives for multi-component pipelines.
//!
//!
//! This module provides common abstractions for building and running
//! pipelines with multiple components. It includes:
//!
//! - [`Pipeline`] - A trait for self-contained pipeline units
//! - [`PipelineContext`] - Shared resources for pipeline execution
//! - [`PipelineRunner`] - Orchestration for multiple pipelines
//!
//! These primitives are used by both blizzard and penguin to implement
//! their respective multi-pipeline and multi-table orchestration.

mod pipeline;

pub use pipeline::{Pipeline, PipelineContext, PipelineRunner, random_jitter, run_pipelines};
