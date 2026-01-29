//! Blizzard CLI: File loader for streaming NDJSON.gz files to Parquet staging.

use clap::Parser;
use std::process::ExitCode;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use blizzard::Config;
use blizzard::run_pipeline;

/// Blizzard - NDJSON.gz file loader
#[derive(Parser, Debug)]
#[command(name = "blizzard")]
#[command(about = "Streams NDJSON.gz files to Parquet staging for Delta Lake ingestion")]
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

    info!("Starting blizzard file loader");
    info!("  Source: {}", config.source.path);
    info!("  Sink: {}", config.sink.table_uri);

    match run_pipeline(config).await {
        Ok(stats) => {
            info!("Pipeline completed successfully");
            info!(
                "  Files processed: {}, Records: {}, Bytes written: {}, Staging files: {}",
                stats.files_processed,
                stats.records_processed,
                stats.bytes_written,
                stats.staging_files_written
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("Pipeline failed: {}", e);
            ExitCode::FAILURE
        }
    }
}
