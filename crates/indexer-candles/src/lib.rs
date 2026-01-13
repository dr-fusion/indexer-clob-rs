pub mod aggregator;
pub mod bucket;
pub mod cache;
pub mod flusher;
pub mod interval;

pub use aggregator::{CandleAggregator, CandleAggregatorBuilder, TradeData};
pub use bucket::CandleBucket;
pub use cache::CandleCache;
pub use interval::CandleInterval;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum CandleError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Redis error: {0}")]
    Redis(String),

    #[error("Invalid interval: {0}")]
    InvalidInterval(String),
}

pub type Result<T> = std::result::Result<T, CandleError>;
