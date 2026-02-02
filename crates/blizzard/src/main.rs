//! Blizzard CLI: File loader for streaming NDJSON.gz files to Parquet.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use blizzard::Config;
use blizzard::config::ConfigPath;
use blizzard::run_pipeline;

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

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);

    // Use rate-limited layer to prevent log spam
    let rate_limited = tracing_limit::RateLimitedLayer::new(fmt_layer);

    tracing_subscriber::registry()
        .with(rate_limited)
        .with(env_filter)
        .init();
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

    run_pipelines(&paths).await
}

async fn run_pipelines(paths: &[ConfigPath]) -> ExitCode {
    info!("Loading config from {} source(s)", paths.len());

    let config = match Config::from_paths(paths) {
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

    match run_pipeline(config).await {
        Ok(stats) => {
            info!(
                "All pipelines completed: {} files processed, {} records, {} bytes written, {} parquet files",
                stats.total_files_processed(),
                stats.total_records_processed(),
                stats.total_bytes_written(),
                stats.total_parquet_files_written()
            );

            // Exit code logic based on failure behavior defined in plan
            if stats.pipelines.is_empty() && !stats.errors.is_empty() {
                // All pipelines failed to start
                error!("All {} pipeline(s) failed", stats.errors.len());
                for (key, err) in &stats.errors {
                    error!("  {}: {}", key, err);
                }
                ExitCode::FAILURE
            } else if stats.errors.is_empty() {
                // All pipelines succeeded
                info!(
                    "All {} pipeline(s) completed successfully",
                    stats.pipelines.len()
                );
                ExitCode::SUCCESS
            } else {
                // Partial success: some pipelines worked, some failed
                let total = stats.pipelines.len() + stats.errors.len();
                warn!("{}/{} pipeline(s) failed", stats.errors.len(), total);
                for (key, err) in &stats.errors {
                    warn!("  {}: {}", key, err);
                }
                ExitCode::from(2) // Distinguish from total failure
            }
        }
        Err(e) => {
            eprintln!("Pipeline failed: {}", e);
            ExitCode::FAILURE
        }
    }
}
