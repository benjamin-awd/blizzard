//! Schema utilities for Penguin.
//!
//! This module provides schema inference from parquet files and schema evolution
//! support for handling schema changes in incoming data.

pub mod evolution;
pub mod inference;
pub mod manager;

pub use evolution::{SchemaComparison, SchemaEvolutionMode, compare_schemas, merge_schemas};
pub use inference::{infer_schema_from_first_file, infer_schema_from_parquet_bytes};
pub use manager::SchemaManager;
