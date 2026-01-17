use alloy::network::Ethereum;
use alloy::providers::{Provider, ProviderBuilder};
use indexer_core::{IndexerError, Result};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// Boxed provider trait for HTTP connections
pub type BoxedProvider = Arc<dyn Provider<Ethereum> + Send + Sync>;

/// Manages RPC providers for HTTP connections
pub struct ProviderManager {
    http: BoxedProvider,
    ws_url: String,
}

impl ProviderManager {
    /// Create a new provider manager with HTTP connection
    pub async fn new(http_url: &str, ws_url: &str) -> Result<Self> {
        // Debug: Print exact URL details
        info!(
            http_url = %http_url,
            http_url_len = http_url.len(),
            http_url_bytes = ?http_url.as_bytes(),
            "Creating provider with HTTP URL"
        );

        // Parse and validate the URL
        let parsed_url: reqwest::Url = http_url
            .parse()
            .map_err(|e| {
                error!(
                    http_url = %http_url,
                    error = %e,
                    "Failed to parse HTTP URL"
                );
                IndexerError::Rpc(format!("Invalid HTTP URL '{}': {}", http_url, e))
            })?;

        // Log parsed URL components
        info!(
            scheme = %parsed_url.scheme(),
            host = ?parsed_url.host_str(),
            port = ?parsed_url.port(),
            path = %parsed_url.path(),
            "Parsed HTTP URL components"
        );

        // Test DNS resolution manually (for debugging)
        if let Some(host) = parsed_url.host_str() {
            debug!(host = %host, "Attempting DNS lookup for host");
            match tokio::net::lookup_host(format!("{}:{}", host, parsed_url.port().unwrap_or(443))).await {
                Ok(addrs) => {
                    let addrs: Vec<_> = addrs.collect();
                    info!(host = %host, addresses = ?addrs, "DNS lookup successful");
                }
                Err(e) => {
                    error!(
                        host = %host,
                        error = %e,
                        "DNS lookup failed - this may indicate network/DNS issues"
                    );
                }
            }
        }

        // Configure HTTP client with connection pooling settings to prevent DNS issues under load
        // Default reqwest pool settings can cause DNS resolution failures with high concurrency
        let pool_max_idle_per_host: usize = env::var("HTTP_POOL_MAX_IDLE_PER_HOST")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(32); // Higher than default to handle concurrent batches

        let pool_idle_timeout_secs: u64 = env::var("HTTP_POOL_IDLE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(90);

        let connect_timeout_secs: u64 = env::var("HTTP_CONNECT_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        let request_timeout_secs: u64 = env::var("HTTP_REQUEST_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);

        info!(
            pool_max_idle_per_host = pool_max_idle_per_host,
            pool_idle_timeout_secs = pool_idle_timeout_secs,
            connect_timeout_secs = connect_timeout_secs,
            request_timeout_secs = request_timeout_secs,
            "Configuring HTTP client pool"
        );

        let client = reqwest::ClientBuilder::new()
            .pool_max_idle_per_host(pool_max_idle_per_host)
            .pool_idle_timeout(Duration::from_secs(pool_idle_timeout_secs))
            .connect_timeout(Duration::from_secs(connect_timeout_secs))
            .timeout(Duration::from_secs(request_timeout_secs))
            .tcp_keepalive(Duration::from_secs(60))
            .tcp_nodelay(true)
            .build()
            .map_err(|e| {
                error!(error = %e, "Failed to build HTTP client");
                IndexerError::Rpc(format!("Failed to build HTTP client: {}", e))
            })?;

        let http = ProviderBuilder::new()
            .connect_reqwest(client, parsed_url);

        info!("HTTP provider created successfully with custom client pool");

        Ok(Self {
            http: Arc::new(http),
            ws_url: ws_url.to_string(),
        })
    }

    /// Get HTTP provider reference
    pub fn http(&self) -> &BoxedProvider {
        &self.http
    }

    /// Get WebSocket URL for future WebSocket connections
    pub fn ws_url(&self) -> &str {
        &self.ws_url
    }
}
