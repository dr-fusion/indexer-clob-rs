use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Database model for orders table
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbOrder {
    /// Primary key: chainId_poolId_orderId
    pub id: String,
    /// Chain ID
    pub chain_id: i64,
    /// Pool ID (hex)
    pub pool_id: String,
    /// Order ID within the pool
    pub order_id: i64,
    /// Transaction hash
    pub transaction_id: Option<String>,
    /// User address
    pub user: Option<String>,
    /// Order side: BUY or SELL
    pub side: Option<String>,
    /// Creation timestamp
    pub timestamp: Option<i32>,
    /// Limit price
    pub price: Option<i64>,
    /// Order quantity
    pub quantity: Option<i64>,
    /// Filled quantity
    pub filled: Option<i64>,
    /// Total filled value (for average price calculation)
    pub total_filled_value: Option<i64>,
    /// Order type: LIMIT or MARKET
    #[sqlx(rename = "type")]
    pub order_type: Option<String>,
    /// Status: OPEN, FILLED, CANCELLED
    pub status: Option<String>,
    /// Expiry timestamp
    pub expiry: Option<i32>,
}

impl DbOrder {
    /// Create composite ID
    pub fn make_id(chain_id: i64, pool_id: &str, order_id: u64) -> String {
        format!("{}_{}{}", chain_id, pool_id, order_id)
    }
}
