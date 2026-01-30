//! Penguin CLI: Delta Lake checkpointer that watches staging and commits to Delta Lake.

use clap::Parser;
use std::process::ExitCode;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

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

    info!("Loading config from {}", args.config);

    let config = match Config::from_file(&args.config) {
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

    match run_pipeline(config).await {
        Ok(stats) => {
            info!(
                "Pipeline completed: {} files committed, {} records",
                stats.total_files_committed(),
                stats.total_records_committed()
            );

            // Exit code logic based on failure behavior defined in plan
            if stats.tables.is_empty() && !stats.errors.is_empty() {
                // All tables failed to start
                error!("All {} table(s) failed", stats.errors.len());
                for (key, err) in &stats.errors {
                    error!("  {}: {}", key, err);
                }
                ExitCode::FAILURE
            } else if stats.errors.is_empty() {
                // All tables succeeded
                info!("All {} table(s) completed successfully", stats.tables.len());
                ExitCode::SUCCESS
            } else {
                // Partial success: some tables worked, some failed
                let total = stats.tables.len() + stats.errors.len();
                warn!("{}/{} table(s) failed", stats.errors.len(), total);
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
