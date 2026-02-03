//! Penguin CLI: Delta Lake checkpointer that discovers parquet files and commits to Delta Lake.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use penguin::config::ConfigPath;
use penguin::{Config, PenguinPipeline, run_pipelines};

/// Penguin - Delta Lake checkpointer
#[derive(Parser, Debug)]
#[command(name = "penguin")]
#[command(about = "Discovers Parquet files and commits them to Delta Lake")]
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

    info!("Loading config from {} source(s)", paths.len());

    let config = match Config::from_paths(&paths) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config: {}", e);
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
        |context| PenguinPipeline::from_config(&config, context),
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
