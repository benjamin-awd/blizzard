//! Penguin CLI: Delta Lake checkpointer that discovers parquet files and commits to Delta Lake.

use std::process::ExitCode;

use clap::Parser;
use tracing::info;

use penguin::{CliArgs, Config, Pipeline, init_tracing, run_pipelines};

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();

    let args = CliArgs::parse();

    let paths = args.config_paths();
    if paths.is_empty() {
        eprintln!("Error: no config files or directories specified");
        return ExitCode::FAILURE;
    }

    info!("Loading config from {} source(s)", paths.len());

    let config = match Config::from_paths(&paths) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config: {e}");
            return ExitCode::FAILURE;
        }
    };

    let table_count = config.table_count();
    info!(
        "Starting penguin delta checkpointer with {} table(s)",
        table_count
    );

    for (table_key, table_config) in config.tables() {
        info!("  Table: {} ({})", table_key, table_config.table_uri);
    }

    let result = run_pipelines(
        &config.metrics.address,
        &config.global,
        "table",
        |context| Pipeline::from_config(&config, context),
    )
    .await;

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Pipeline failed: {e}");
            ExitCode::FAILURE
        }
    }
}
