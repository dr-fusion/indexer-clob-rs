pub mod config;
pub mod models;
pub mod pool;
pub mod repositories;
pub mod writer;

pub use config::{BatchWriterConfig, DatabaseConfig};
pub use pool::DatabasePool;
pub use writer::BatchWriter;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Database connection error: {0}")]
    Connection(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Migration error: {0}")]
    Migration(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<sqlx::Error> for DatabaseError {
    fn from(err: sqlx::Error) -> Self {
        DatabaseError::Query(err.to_string())
    }
}

impl From<sqlx::migrate::MigrateError> for DatabaseError {
    fn from(err: sqlx::migrate::MigrateError) -> Self {
        DatabaseError::Migration(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, DatabaseError>;
