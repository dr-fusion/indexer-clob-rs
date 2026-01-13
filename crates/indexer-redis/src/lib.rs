pub mod channels;
pub mod config;
pub mod connection;
pub mod debouncer;
pub mod messages;
pub mod publisher;

pub use config::RedisConfig;
pub use connection::RedisConnection;
pub use debouncer::DebouncedEmitter;
pub use publisher::RedisPublisher;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RedisError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Publish error: {0}")]
    Publish(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<redis::RedisError> for RedisError {
    fn from(err: redis::RedisError) -> Self {
        RedisError::Connection(err.to_string())
    }
}

impl From<serde_json::Error> for RedisError {
    fn from(err: serde_json::Error) -> Self {
        RedisError::Serialization(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, RedisError>;
