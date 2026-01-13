use crate::config::RedisConfig;
use crate::{RedisError, Result};
use redis::aio::ConnectionManager;
use redis::Client;
use tracing::info;

/// Redis connection wrapper with connection manager
#[derive(Clone)]
pub struct RedisConnection {
    manager: ConnectionManager,
    config: RedisConfig,
}

impl RedisConnection {
    /// Create a new Redis connection
    pub async fn new(config: RedisConfig) -> Result<Self> {
        info!(url = %config.url, "Connecting to Redis");

        let client = Client::open(config.url.as_str())
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        let manager = ConnectionManager::new(client)
            .await
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        info!("Redis connection established");

        Ok(Self { manager, config })
    }

    /// Get a connection from the manager
    pub fn get_connection(&self) -> ConnectionManager {
        self.manager.clone()
    }

    /// Get the config
    pub fn config(&self) -> &RedisConfig {
        &self.config
    }

    /// Health check
    pub async fn health_check(&self) -> Result<()> {
        let mut conn = self.manager.clone();
        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| RedisError::Connection(e.to_string()))?;
        Ok(())
    }
}
