use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Database model for balances table
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbBalance {
    /// Primary key: chainId_user_currency
    pub id: String,
    /// Chain ID
    pub chain_id: i64,
    /// User address
    pub user: Option<String>,
    /// Currency/token address
    pub currency: Option<String>,
    /// Available balance
    pub amount: Option<i64>,
    /// Locked balance (in open orders)
    pub locked_amount: Option<i64>,
}

impl DbBalance {
    /// Create composite ID
    pub fn make_id(chain_id: i64, user: &str, currency: &str) -> String {
        format!("{}_{}_{}", chain_id, user, currency)
    }
}
