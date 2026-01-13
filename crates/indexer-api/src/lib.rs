pub mod config;
pub mod schema;
pub mod server;

pub use config::ApiConfig;
pub use server::ApiServer;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Server error: {0}")]
    Server(String),

    #[error("Database error: {0}")]
    Database(String),

    #[error("Not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, ApiError>;
