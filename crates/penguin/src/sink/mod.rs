//! Delta Lake sink for committing Parquet files.

mod delta;
mod traits;

pub use delta::DeltaSink;
pub use traits::TableSink;
