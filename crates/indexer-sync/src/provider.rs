use alloy::network::Ethereum;
use alloy::providers::{Provider, ProviderBuilder};
use indexer_core::{IndexerError, Result};
use std::sync::Arc;

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
        let http_url: reqwest::Url = http_url
            .parse()
            .map_err(|e| IndexerError::Rpc(format!("Invalid HTTP URL: {}", e)))?;

        let http = ProviderBuilder::new().connect_http(http_url);

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
