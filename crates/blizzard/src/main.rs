//! Blizzard CLI: File loader for streaming NDJSON.gz files to Parquet.

use std::process::ExitCode;

use blizzard::{Application, Config};

fn main() -> ExitCode {
    Application::<Config>::run()
}
