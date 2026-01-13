use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Database model for trades table (per-order trades)
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbTrade {
    /// Primary key: txHash_logIndex
    pub id: String,
    /// Chain ID
    pub chain_id: i64,
    /// Transaction hash
    pub transaction_id: Option<String>,
    /// Pool ID (hex)
    pub pool_id: String,
    /// Order ID that was filled
    pub order_id: Option<String>,
    /// Execution price
    pub price: Option<i64>,
    /// Executed quantity
    pub quantity: Option<i64>,
    /// Timestamp
    pub timestamp: Option<i32>,
    /// Log index within transaction
    pub log_index: Option<i32>,
}

impl DbTrade {
    /// Create ID from tx_hash and log_index
    pub fn make_id(tx_hash: &str, log_index: u64) -> String {
        format!("{}_{}", tx_hash, log_index)
    }
}

/// Database model for order_book_trades table (market-level trades)
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbOrderBookTrade {
    /// Primary key: txHash_logIndex
    pub id: String,
    /// Chain ID
    pub chain_id: i64,
    /// Execution price (maker's price)
    pub price: Option<i64>,
    /// Taker's limit price
    pub taker_limit_price: Option<i64>,
    /// Executed quantity
    pub quantity: Option<i64>,
    /// Timestamp
    pub timestamp: Option<i32>,
    /// Log index
    pub log_index: Option<i32>,
    /// Transaction hash
    pub transaction_id: Option<String>,
    /// Taker side: BUY or SELL
    pub side: Option<String>,
    /// Pool ID (hex)
    pub pool_id: String,
}

impl DbOrderBookTrade {
    /// Create ID from tx_hash and log_index
    pub fn make_id(tx_hash: &str, log_index: u64) -> String {
        format!("{}_{}", tx_hash, log_index)
    }
}
