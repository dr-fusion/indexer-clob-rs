use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    /// Server host
    #[serde(default = "default_host")]
    pub host: String,

    /// Server port
    #[serde(default = "default_port")]
    pub port: u16,

    /// Enable CORS
    #[serde(default = "default_cors")]
    pub cors_enabled: bool,

    /// CORS allowed origins (comma-separated)
    #[serde(default)]
    pub cors_origins: Option<String>,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    4000
}

fn default_cors() -> bool {
    true
}

impl ApiConfig {
    pub fn from_env() -> Self {
        Self {
            host: std::env::var("API_HOST").unwrap_or_else(|_| default_host()),
            port: std::env::var("API_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_port),
            cors_enabled: std::env::var("API_CORS_ENABLED")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_cors),
            cors_origins: std::env::var("API_CORS_ORIGINS").ok(),
        }
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
