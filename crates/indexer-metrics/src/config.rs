use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    /// Metrics server host
    #[serde(default = "default_host")]
    pub host: String,

    /// Metrics server port
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    9090
}

impl MetricsConfig {
    pub fn from_env() -> Self {
        Self {
            host: std::env::var("METRICS_HOST").unwrap_or_else(|_| default_host()),
            port: std::env::var("METRICS_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_port),
        }
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
