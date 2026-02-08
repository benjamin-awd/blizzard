//! Application abstraction for reducing main.rs boilerplate.
//!
//! This module provides the `AppConfig` trait and `Application` struct
//! to standardize startup logic across blizzard and penguin applications.

use std::fs;
use std::process::ExitCode;

use clap::Parser;
use tracing::{info, warn};

use crate::config::{CliArgs, ConfigPath, Mergeable};
use crate::error::ConfigError;
use crate::topology::{Pipeline, PipelineContext, run_pipelines};
use crate::tracing::init_tracing;

/// Detect the CPU limit from Linux cgroup files.
///
/// Returns the number of whole CPUs available, or `None` if not running
/// in a cgroup-limited environment (e.g., local development).
fn detect_cpu_limit() -> Option<usize> {
    // cgroups v2: /sys/fs/cgroup/cpu.max contains "$MAX $PERIOD" (e.g., "300000 100000" = 3 CPUs)
    if let Some(cpus) = detect_cgroupv2() {
        return Some(cpus);
    }
    // cgroups v1: separate quota and period files
    if let Some(cpus) = detect_cgroupv1() {
        return Some(cpus);
    }
    None
}

fn detect_cgroupv2() -> Option<usize> {
    let content = fs::read_to_string("/sys/fs/cgroup/cpu.max").ok()?;
    let mut parts = content.split_whitespace();
    let max = parts.next()?;
    if max == "max" {
        return None; // unlimited
    }
    let max: usize = max.parse().ok()?;
    let period: usize = parts.next()?.parse().ok()?;
    let cpus = max / period;
    if cpus > 0 { Some(cpus) } else { Some(1) }
}

fn detect_cgroupv1() -> Option<usize> {
    let quota: isize = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    if quota < 0 {
        return None; // unlimited
    }
    let period: usize = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let cpus = quota.cast_unsigned() / period;
    if cpus > 0 { Some(cpus) } else { Some(1) }
}

/// Trait for application configurations that can be loaded and run.
///
/// Implement this trait on your configuration struct to enable use with
/// `Application<C>::run()` for standardized startup behavior.
///
/// Note: `Mergeable` already provides `metrics()` and `global()` methods,
/// which are used by the `Application` runner.
pub trait AppConfig: Mergeable + Sized {
    /// The pipeline type this config produces.
    type Pipeline: Pipeline;

    /// Human-readable name for components (e.g., "pipeline", "table").
    const COMPONENT_NAME: &'static str;

    /// Load config from paths with validation.
    fn from_paths(paths: &[ConfigPath]) -> Result<Self, ConfigError>;

    /// Create pipelines from this config.
    fn create_pipelines(&self, context: PipelineContext) -> Vec<Self::Pipeline>;

    /// Log startup info (component count and details).
    fn log_startup_info(&self);
}

/// Application runner that handles the full startup lifecycle.
///
/// Provides a standardized entry point for both blizzard and penguin,
/// reducing main.rs to a simple `Application::<Config>::run()` call.
pub struct Application<C: AppConfig> {
    config: C,
}

impl<C: AppConfig> Application<C> {
    /// Full application lifecycle: parse args, load config, run pipelines.
    ///
    /// This is the main entry point for applications. It handles:
    /// 1. Initialize tracing
    /// 2. Parse CLI arguments
    /// 3. Validate config paths
    /// 4. Load and validate configuration
    /// 5. Log startup info
    /// 6. Run pipelines
    pub fn run() -> ExitCode {
        init_tracing();

        let args = CliArgs::parse();
        let paths = args.config_paths();

        if paths.is_empty() {
            eprintln!("Error: no config files or directories specified");
            return ExitCode::FAILURE;
        }

        let source_count = paths.len();
        info!("Loading config from {source_count} source(s)");

        match Self::from_paths(&paths) {
            Ok(app) => app.execute(),
            Err(e) => {
                eprintln!("Failed to load config: {e}");
                ExitCode::FAILURE
            }
        }
    }

    /// Load config from paths (useful for testing).
    pub fn from_paths(paths: &[ConfigPath]) -> Result<Self, ConfigError> {
        let config = C::from_paths(paths)?;
        Ok(Self { config })
    }

    /// Execute the application (after config is loaded).
    fn execute(self) -> ExitCode {
        self.config.log_startup_info();

        let global = Mergeable::global(&self.config);
        let worker_threads = global.runtime_worker_threads.or_else(detect_cpu_limit);
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.enable_all();
        match (global.runtime_worker_threads, worker_threads) {
            (Some(threads), _) => {
                info!("Tokio runtime worker threads: {threads} (from config)");
                builder.worker_threads(threads);
            }
            (None, Some(threads)) => {
                info!("Tokio runtime worker threads: {threads} (detected from cgroup CPU limit)");
                builder.worker_threads(threads);
            }
            (None, None) => {
                warn!(
                    "No cgroup CPU limit detected, using Tokio default (host core count). \
                     Consider setting global.runtime_worker_threads in containerized environments."
                );
            }
        }
        let runtime = match builder.build() {
            Ok(rt) => rt,
            Err(e) => {
                eprintln!("Failed to create tokio runtime: {e}");
                return ExitCode::FAILURE;
            }
        };
        let result = runtime.block_on(run_pipelines(
            &Mergeable::metrics(&self.config).address,
            Mergeable::global(&self.config),
            C::COMPONENT_NAME,
            |context| self.config.create_pipelines(context),
        ));

        match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("{} failed: {e}", C::COMPONENT_NAME);
                ExitCode::FAILURE
            }
        }
    }
}
