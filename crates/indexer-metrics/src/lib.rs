pub mod config;
pub mod counters;
pub mod gauges;
pub mod histograms;
pub mod server;

pub use config::MetricsConfig;
pub use server::MetricsServer;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum MetricsError {
    #[error("Server error: {0}")]
    Server(String),

    #[error("Recorder error: {0}")]
    Recorder(String),
}

pub type Result<T> = std::result::Result<T, MetricsError>;

/// Initialize the metrics recorder
pub fn init() -> Result<()> {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder
        .install()
        .map_err(|e| MetricsError::Recorder(e.to_string()))?;

    // Initialize all metrics
    counters::init();
    gauges::init();
    histograms::init();

    Ok(())
}
