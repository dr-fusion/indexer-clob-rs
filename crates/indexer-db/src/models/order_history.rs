use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Database model for order_history table
/// Records state changes for orders
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbOrderHistory {
    /// Primary key: txHash_logIndex or unique ID
    pub id: String,
    /// Chain ID
    pub chain_id: i64,
    /// Pool ID (hex)
    pub pool_id: String,
    /// Order ID reference
    pub order_id: Option<String>,
    /// Transaction hash
    pub transaction_id: Option<String>,
    /// Timestamp of this state change
    pub timestamp: Option<i32>,
    /// Filled amount at this point
    pub filled: Option<i64>,
    /// Status at this point: OPEN, FILLED, CANCELLED, PARTIALLY_FILLED
    pub status: Option<String>,
}

impl DbOrderHistory {
    /// Create ID from tx_hash and log_index
    pub fn make_id(tx_hash: &str, log_index: u64) -> String {
        format!("{}_{}", tx_hash, log_index)
    }
}
