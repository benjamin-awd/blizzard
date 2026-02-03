//! Application abstraction for reducing main.rs boilerplate.
//!
//! This module provides the `AppConfig` trait and `Application` struct
//! to standardize startup logic across blizzard and penguin applications.

use std::process::ExitCode;

use clap::Parser;
use tracing::info;

use crate::config::{CliArgs, ConfigPath, Mergeable};
use crate::error::ConfigError;
use crate::topology::{Pipeline, PipelineContext, run_pipelines};
use crate::tracing::init_tracing;

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

        let runtime = tokio::runtime::Runtime::new().unwrap();
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
