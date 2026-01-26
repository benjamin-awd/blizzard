//! blizzard: A standalone tool for streaming NDJSON.gz files to Delta Lake.
//!
//! This tool reads compressed NDJSON files from various storage backends (S3, GCS,
//! Azure, local filesystem) and writes them to Delta Lake tables with exactly-once
//! semantics using checkpoint-based recovery.

mod checkpoint;
mod config;
mod dlq;
mod error;
mod metrics;
mod pipeline;
mod sink;
mod source;
mod storage;

use clap::Parser;
use snafu::prelude::*;
use std::path::PathBuf;
use tracing::{debug, info};
use tracing_limit::RateLimitedLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

use config::Config;
use error::{AddressParseSnafu, ConfigSnafu, MetricsSnafu, PipelineError};
use pipeline::run_pipeline;

/// NDJSON.gz to Delta Lake streaming tool.
#[derive(Parser, Debug)]
#[command(name = "blizzard")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the configuration file.
    #[arg(short, long)]
    config: PathBuf,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Dry run - validate configuration without processing.
    #[arg(long)]
    dry_run: bool,

    /// Internal log rate limit window in seconds (0 to disable).
    #[arg(long, default_value = "10")]
    internal_log_rate_secs: u64,
}

#[snafu::report]
#[tokio::main]
async fn main() -> Result<(), PipelineError> {
    let args = Args::parse();

    // Initialize logging
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));

    if args.internal_log_rate_secs > 0 {
        let fmt_layer = tracing_subscriber::fmt::layer().with_target(false);
        let rate_limited =
            RateLimitedLayer::new(fmt_layer).with_default_limit(args.internal_log_rate_secs);

        tracing_subscriber::registry()
            .with(rate_limited.with_filter(filter))
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .init();
    }

    info!("blizzard starting");

    // Load or build configuration
    let config = build_config(&args)?;

    // Initialize metrics if enabled
    if config.metrics.enabled {
        let addr = config.metrics.address.parse().context(AddressParseSnafu)?;
        metrics::init(addr).context(MetricsSnafu)?;
        debug!(
            "Metrics endpoint listening on http://{}/metrics",
            config.metrics.address
        );
    }

    if args.dry_run {
        info!("Dry run mode - validating configuration");
        info!("Source: {}", config.source.path);
        info!("Sink: {}", config.sink.path);
        info!("Schema fields: {}", config.schema.fields.len());
        for field in &config.schema.fields {
            info!("  - {}: {:?}", field.name, field.field_type);
        }
        info!("Configuration is valid");
        return Ok(());
    }

    // Run the pipeline
    let stats = run_pipeline(config).await?;

    info!("Pipeline completed successfully");
    info!("  Files processed: {}", stats.files_processed);
    info!("  Records processed: {}", stats.records_processed);
    info!("  Parquet files written: {}", stats.parquet_files_written);
    info!("  Bytes written: {}", stats.bytes_written);
    info!("  Delta commits: {}", stats.delta_commits);
    info!("  Checkpoints saved: {}", stats.checkpoints_saved);

    Ok(())
}

/// Build configuration from arguments.
fn build_config(args: &Args) -> Result<Config, PipelineError> {
    Config::from_file(&args.config).context(ConfigSnafu)
}
