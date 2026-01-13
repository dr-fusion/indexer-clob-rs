use crate::{config::DatabaseConfig, DatabaseError, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use tracing::info;

/// Database connection pool wrapper
#[derive(Clone)]
pub struct DatabasePool {
    pool: PgPool,
}

impl DatabasePool {
    /// Create a new database pool from config
    pub async fn new(config: &DatabaseConfig) -> Result<Self> {
        info!(
            max_connections = config.max_connections,
            min_connections = config.min_connections,
            "Connecting to database"
        );

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.connect_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .connect(&config.url)
            .await
            .map_err(|e| DatabaseError::Connection(e.to_string()))?;

        info!("Database connection pool established");

        Ok(Self { pool })
    }

    /// Run migrations
    pub async fn migrate(&self) -> Result<()> {
        info!("Running database migrations");
        sqlx::migrate!("./src/migrations")
            .run(&self.pool)
            .await?;
        info!("Migrations completed");
        Ok(())
    }

    /// Get the inner pool reference
    pub fn inner(&self) -> &PgPool {
        &self.pool
    }

    /// Check if the connection is healthy
    pub async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::Connection(e.to_string()))?;
        Ok(())
    }

    /// Close the pool gracefully
    pub async fn close(&self) {
        info!("Closing database connection pool");
        self.pool.close().await;
    }
}
