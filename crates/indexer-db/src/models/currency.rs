use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Database model for currencies table
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbCurrency {
    /// Primary key: chainId_address
    pub id: String,
    /// Chain ID
    pub chain_id: i64,
    /// Token contract address
    pub address: String,
    /// Token name
    pub name: Option<String>,
    /// Token symbol
    pub symbol: Option<String>,
    /// Token decimals
    pub decimals: Option<i16>,
    /// USD price (cached)
    pub price_usd: Option<f32>,
}

impl DbCurrency {
    /// Create composite ID
    pub fn make_id(chain_id: i64, address: &str) -> String {
        format!("{}_{}", chain_id, address)
    }
}
