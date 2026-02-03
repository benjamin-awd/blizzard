//! Blizzard CLI: File loader for streaming NDJSON.gz files to Parquet.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use tracing::info;

use blizzard::config::ConfigPath;
use blizzard::{BlizzardPipeline, Config, init_tracing, run_pipelines};

/// Blizzard - NDJSON.gz file loader
#[derive(Parser, Debug)]
#[command(name = "blizzard")]
#[command(about = "Streams NDJSON.gz files to Parquet staging for Delta Lake ingestion")]
struct Args {
    /// Path to configuration file (can be specified multiple times)
    #[arg(short, long)]
    config: Vec<PathBuf>,

    /// Path to configuration directory (can be specified multiple times)
    #[arg(short = 'C', long = "config-dir")]
    config_dirs: Vec<PathBuf>,
}

impl Args {
    fn config_paths(&self) -> Vec<ConfigPath> {
        self.config
            .iter()
            .map(ConfigPath::file)
            .chain(self.config_dirs.iter().map(ConfigPath::dir))
            .collect()
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();

    let args = Args::parse();

    let paths = args.config_paths();
    if paths.is_empty() {
        eprintln!("Error: no config files or directories specified");
        return ExitCode::FAILURE;
    }

    info!("Loading config from {} source(s)", paths.len());

    let config = match Config::from_paths(&paths) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config: {}", e);
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
        |context| BlizzardPipeline::from_config(&config, context),
    )
    .await;

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Pipeline failed: {}", e);
            ExitCode::FAILURE
        }
    }
}
