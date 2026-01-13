use crate::config::MetricsConfig;
use axum::{routing::get, Router};
use metrics_exporter_prometheus::PrometheusHandle;
use tracing::info;

/// Metrics HTTP Server
pub struct MetricsServer {
    config: MetricsConfig,
}

impl MetricsServer {
    pub fn new(config: MetricsConfig) -> Self {
        Self { config }
    }

    /// Start the metrics server
    pub async fn run(self) -> crate::Result<()> {
        let addr = self.config.address();

        // Get the prometheus handle for rendering metrics
        let handle = metrics_exporter_prometheus::PrometheusBuilder::new()
            .install_recorder()
            .map_err(|e| crate::MetricsError::Recorder(e.to_string()))?;

        let app = Router::new()
            .route("/metrics", get(move || metrics_handler(handle.clone())))
            .route("/health", get(health_handler));

        info!(address = %addr, "Starting metrics server");

        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| crate::MetricsError::Server(e.to_string()))?;

        axum::serve(listener, app)
            .await
            .map_err(|e| crate::MetricsError::Server(e.to_string()))?;

        Ok(())
    }
}

/// Metrics endpoint handler
async fn metrics_handler(handle: PrometheusHandle) -> String {
    handle.render()
}

/// Health check endpoint
async fn health_handler() -> &'static str {
    "OK"
}
