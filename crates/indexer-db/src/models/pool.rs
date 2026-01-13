use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Database model for pools table
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbPool {
    /// Hex pool_id (primary key)
    pub id: String,
    /// Chain ID
    pub chain_id: i64,
    /// Coin name (optional)
    pub coin: Option<String>,
    /// Orderbook contract address
    pub order_book: String,
    /// Base currency address
    pub base_currency: String,
    /// Quote currency address
    pub quote_currency: String,
    /// Base currency decimals
    pub base_decimals: Option<i16>,
    /// Quote currency decimals
    pub quote_decimals: Option<i16>,
    /// Creation timestamp
    pub timestamp: Option<i32>,
    /// Token0 price (for display)
    pub token0_price: Option<f32>,
    /// Token1 price (for display)
    pub token1_price: Option<f32>,
}

impl DbPool {
    /// Create ID from pool_id bytes
    pub fn make_id(pool_id: &[u8; 32]) -> String {
        format!("0x{}", hex::encode(pool_id))
    }
}
