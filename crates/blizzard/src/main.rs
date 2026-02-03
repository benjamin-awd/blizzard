//! Blizzard CLI: File loader for streaming NDJSON.gz files to Parquet.

use std::process::ExitCode;

use clap::Parser;
use tracing::info;

use blizzard::{CliArgs, Config, Pipeline, init_tracing, run_pipelines};

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

    let pipeline_count = config.pipeline_count();
    info!(
        "Starting blizzard file loader with {} pipeline(s)",
        pipeline_count
    );

    for (pipeline_key, pipeline_config) in config.pipelines() {
        info!(
            "  Pipeline: {} ({} -> {})",
            pipeline_key, pipeline_config.source.path, pipeline_config.sink.table_uri
        );
    }

    let result = run_pipelines(
        &config.metrics.address,
        &config.global,
        "pipeline",
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
