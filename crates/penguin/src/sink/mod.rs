//! Delta Lake sink for committing Parquet files.

pub mod delta;
mod traits;

pub use delta::DeltaSink;
pub use traits::TableSink;
