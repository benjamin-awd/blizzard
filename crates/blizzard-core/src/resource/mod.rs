//! Resource management for multi-pipeline deployments.
//!
//! This module provides shared resource management across multiple pipelines
//! running in a single process:
//!
//! - `StoragePool`: Connection pooling for storage providers
//!
//! These resources help prevent resource exhaustion and improve efficiency
//! when running many pipelines concurrently.

mod pool;

pub use pool::{StoragePool, StoragePoolRef};
