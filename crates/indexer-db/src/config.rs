use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    /// PostgreSQL connection URL
    pub url: String,

    /// Maximum number of connections in the pool
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Minimum number of connections to maintain
    #[serde(default = "default_min_connections")]
    pub min_connections: u32,

    /// Connection timeout in seconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,

    /// Idle timeout in seconds
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
}

fn default_max_connections() -> u32 {
    10
}

fn default_min_connections() -> u32 {
    2
}

fn default_connect_timeout() -> u64 {
    30
}

fn default_idle_timeout() -> u64 {
    600
}

impl DatabaseConfig {
    pub fn from_env() -> Self {
        Self {
            url: std::env::var("DATABASE_URL")
                .expect("DATABASE_URL must be set"),
            max_connections: std::env::var("DATABASE_MAX_CONNECTIONS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_max_connections),
            min_connections: std::env::var("DATABASE_MIN_CONNECTIONS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_min_connections),
            connect_timeout_secs: std::env::var("DATABASE_CONNECT_TIMEOUT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_connect_timeout),
            idle_timeout_secs: std::env::var("DATABASE_IDLE_TIMEOUT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_idle_timeout),
        }
    }
}

/// Configuration for the BatchWriter
#[derive(Debug, Clone)]
pub struct BatchWriterConfig {
    /// Maximum items before flush trigger (default: 1000)
    pub batch_size: usize,

    /// Flush interval in milliseconds (default: 1000)
    pub flush_interval_ms: u64,

    /// Channel capacity for write operations (default: 50000)
    pub channel_capacity: usize,
}

impl Default for BatchWriterConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            flush_interval_ms: 1000,
            channel_capacity: 50000,
        }
    }
}

impl BatchWriterConfig {
    pub fn from_env() -> Self {
        Self {
            batch_size: std::env::var("DB_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000),
            flush_interval_ms: std::env::var("DB_FLUSH_INTERVAL_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000),
            channel_capacity: std::env::var("DB_CHANNEL_CAPACITY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(50000),
        }
    }
}
