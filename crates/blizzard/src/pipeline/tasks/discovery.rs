//! Background file discovery task.
//!
//! Lists pending files from all sources and streams them through a channel,
//! enabling downloads to start while discovery continues.

use indexmap::IndexMap;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::error::PipelineError;
use crate::pipeline::tracker::{DiscoverySource, SourcedFile};

/// Handle to the background file discovery task.
pub(in crate::pipeline) struct DiscoveryTask {
    /// Receiver for discovered files.
    pub rx: mpsc::Receiver<SourcedFile>,
    /// Handle to the spawned task. Resolves to the total number of files discovered.
    pub handle: JoinHandle<Result<usize, PipelineError>>,
}

impl DiscoveryTask {
    /// Spawn a discovery task that lists files from all sources.
    ///
    /// Files are sent through the channel as they're discovered, enabling
    /// downloads to start before all sources have been listed.
    ///
    /// # Arguments
    /// * `sources` - Per-source discovery data (storage, snapshot, prefixes)
    /// * `shutdown` - Cancellation token for graceful shutdown
    /// * `pipeline_key` - Pipeline identifier for logging
    pub fn spawn(
        sources: IndexMap<String, DiscoverySource>,
        shutdown: CancellationToken,
        pipeline_key: String,
    ) -> Self {
        let (tx, rx) = mpsc::channel(64);

        let handle = tokio::spawn(Self::run(sources, tx, shutdown, pipeline_key));

        Self { rx, handle }
    }

    async fn run(
        sources: IndexMap<String, DiscoverySource>,
        tx: mpsc::Sender<SourcedFile>,
        shutdown: CancellationToken,
        pipeline_key: String,
    ) -> Result<usize, PipelineError> {
        let mut total = 0;

        for (source_name, source) in sources {
            if shutdown.is_cancelled() {
                debug!(
                    target = %pipeline_key,
                    "Discovery stopped by shutdown"
                );
                break;
            }

            let files = source
                .snapshot
                .list_pending(&source.storage, source.prefixes.as_deref(), &pipeline_key)
                .await?;

            debug!(
                target = %pipeline_key,
                source = %source_name,
                count = files.len(),
                "Discovered files from source"
            );

            for path in files {
                total += 1;
                let sourced_file = SourcedFile {
                    source_name: source_name.clone(),
                    path,
                };
                if tx.send(sourced_file).await.is_err() {
                    // Consumer dropped — stop discovery
                    debug!(
                        target = %pipeline_key,
                        "Discovery stopped: consumer closed"
                    );
                    return Ok(total);
                }
            }
        }

        debug!(
            target = %pipeline_key,
            total,
            "Discovery complete"
        );
        Ok(total)
    }
}
