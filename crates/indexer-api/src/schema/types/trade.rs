use async_graphql::SimpleObject;
use indexer_db::models::DbOrderBookTrade;

/// GraphQL Trade type
#[derive(Debug, Clone, SimpleObject)]
pub struct GqlTrade {
    pub id: String,
    pub chain_id: i64,
    pub pool_id: String,
    pub price: Option<String>,
    pub taker_limit_price: Option<String>,
    pub quantity: Option<String>,
    pub timestamp: Option<i32>,
    pub log_index: Option<i32>,
    pub transaction_id: Option<String>,
    pub side: Option<String>,
}

impl From<DbOrderBookTrade> for GqlTrade {
    fn from(trade: DbOrderBookTrade) -> Self {
        Self {
            id: trade.id,
            chain_id: trade.chain_id,
            pool_id: trade.pool_id,
            price: trade.price.map(|p| p.to_string()),
            taker_limit_price: trade.taker_limit_price.map(|p| p.to_string()),
            quantity: trade.quantity.map(|q| q.to_string()),
            timestamp: trade.timestamp,
            log_index: trade.log_index,
            transaction_id: trade.transaction_id,
            side: trade.side,
        }
    }
}
