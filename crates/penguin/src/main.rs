//! Penguin CLI: Delta Lake checkpointer that discovers parquet files and commits to Delta Lake.

use std::process::ExitCode;

use penguin::{Application, Config};

fn main() -> ExitCode {
    Application::<Config>::run()
}
