use alloy_primitives::{Address, FixedBytes};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexerError {
    #[error("Missing required environment variable: {0}")]
    MissingEnvVar(String),

    #[error("Deployment file not found: {0}")]
    DeploymentFileNotFound(String),

    #[error("Failed to parse deployment file: {0}")]
    DeploymentParseError(String),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("WebSocket error: {0}")]
    WebSocket(String),

    #[error("Event decode error: {0}")]
    EventDecode(String),

    #[error("Unknown OrderBook address: {0}")]
    UnknownOrderBook(Address),

    #[error("Order not found: pool={0}, order_id={1}")]
    OrderNotFound(FixedBytes<32>, u64),

    #[error("Pool not found: {0}")]
    PoolNotFound(FixedBytes<32>),

    #[error("Gap detected: expected block {expected}, got {actual}")]
    GapDetected { expected: u64, actual: u64 },

    #[error("WebSocket disconnected")]
    WebSocketDisconnected,

    #[error("Sync error: {0}")]
    Sync(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(String),
}

pub type Result<T> = std::result::Result<T, IndexerError>;
