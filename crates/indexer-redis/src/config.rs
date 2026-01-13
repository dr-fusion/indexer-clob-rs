use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    /// Redis connection URL
    pub url: String,

    /// Stream key for events
    #[serde(default = "default_stream_key")]
    pub stream_key: String,

    /// Maximum stream length (MAXLEN for XADD)
    #[serde(default = "default_max_len")]
    pub max_len: usize,

    /// Enable async/fire-and-forget mode
    #[serde(default = "default_async")]
    pub async_mode: bool,

    /// Async queue capacity (number of messages to buffer)
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,

    /// Batch size for async publishing (messages per pipeline)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Debounce interval in milliseconds for stats updates
    #[serde(default = "default_debounce_ms")]
    pub stats_debounce_ms: u64,

    /// Maximum debounce delay in milliseconds
    #[serde(default = "default_max_debounce_ms")]
    pub stats_max_debounce_ms: u64,
}

fn default_stream_key() -> String {
    "events:stream".to_string()
}

fn default_max_len() -> usize {
    100000
}

fn default_async() -> bool {
    true
}

fn default_queue_capacity() -> usize {
    100000 // 100k messages
}

fn default_batch_size() -> usize {
    100 // 100 messages per pipeline batch
}

fn default_debounce_ms() -> u64 {
    100
}

fn default_max_debounce_ms() -> u64 {
    500
}

impl RedisConfig {
    pub fn from_env() -> Self {
        Self {
            url: std::env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
            stream_key: std::env::var("REDIS_STREAM_KEY")
                .unwrap_or_else(|_| default_stream_key()),
            max_len: std::env::var("REDIS_MAX_LEN")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_max_len),
            async_mode: std::env::var("REDIS_ASYNC")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_async),
            queue_capacity: std::env::var("REDIS_QUEUE_CAPACITY")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_queue_capacity),
            batch_size: std::env::var("REDIS_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_batch_size),
            stats_debounce_ms: std::env::var("STATS_DEBOUNCE_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_debounce_ms),
            stats_max_debounce_ms: std::env::var("STATS_MAX_DEBOUNCE_MS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(default_max_debounce_ms),
        }
    }
}
