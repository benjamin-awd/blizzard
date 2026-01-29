//! Penguin CLI: Delta Lake checkpointer that watches staging and commits to Delta Lake.

use clap::Parser;
use std::process::ExitCode;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use penguin::Config;
use penguin::run_pipeline;

/// Penguin - Delta Lake checkpointer
#[derive(Parser, Debug)]
#[command(name = "penguin")]
#[command(about = "Watches staging directory and commits Parquet files to Delta Lake")]
struct Args {
    /// Path to the configuration file
    #[arg(short, long)]
    config: String,
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_filter(env_filter);

    // Use rate-limited layer to prevent log spam
    let rate_limited = tracing_limit::RateLimitedLayer::new(fmt_layer);

    tracing_subscriber::registry().with(rate_limited).init();
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();

    let args = Args::parse();

    info!("Loading config from {}", args.config);

    let config = match Config::from_file(&args.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config: {}", e);
            return ExitCode::FAILURE;
        }
    };

    info!("Starting penguin delta checkpointer");
    info!("  Table URI: {}", config.sink.table_uri);

    match run_pipeline(config).await {
        Ok(stats) => {
            info!("Pipeline completed successfully");
            info!(
                "  Files committed: {}, Records: {}",
                stats.files_committed, stats.records_committed
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("Pipeline failed: {}", e);
            ExitCode::FAILURE
        }
    }
}
