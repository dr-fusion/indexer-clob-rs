use async_graphql::SimpleObject;
use indexer_db::models::DbPool;

/// GraphQL Pool type
#[derive(Debug, Clone, SimpleObject)]
pub struct GqlPool {
    pub id: String,
    pub chain_id: i64,
    pub coin: Option<String>,
    pub order_book: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub base_decimals: Option<i32>,
    pub quote_decimals: Option<i32>,
    pub timestamp: Option<i32>,
    pub token0_price: Option<f64>,
    pub token1_price: Option<f64>,
}

impl From<DbPool> for GqlPool {
    fn from(pool: DbPool) -> Self {
        Self {
            id: pool.id,
            chain_id: pool.chain_id,
            coin: pool.coin,
            order_book: pool.order_book,
            base_currency: pool.base_currency,
            quote_currency: pool.quote_currency,
            base_decimals: pool.base_decimals.map(|d| d as i32),
            quote_decimals: pool.quote_decimals.map(|d| d as i32),
            timestamp: pool.timestamp,
            token0_price: pool.token0_price.map(|p| p as f64),
            token1_price: pool.token1_price.map(|p| p as f64),
        }
    }
}
