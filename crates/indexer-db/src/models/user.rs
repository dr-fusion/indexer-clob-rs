use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Database model for users table
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbUser {
    /// User address (primary key)
    pub user: String,
    /// Chain ID
    pub chain_id: i64,
    /// First seen timestamp
    pub created_at: Option<i32>,
}
