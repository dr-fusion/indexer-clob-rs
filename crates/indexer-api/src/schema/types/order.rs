use async_graphql::SimpleObject;
use indexer_db::models::DbOrder;

/// GraphQL Order type
#[derive(Debug, Clone, SimpleObject)]
pub struct GqlOrder {
    pub id: String,
    pub chain_id: i64,
    pub pool_id: String,
    pub order_id: i64,
    pub transaction_id: Option<String>,
    pub user: Option<String>,
    pub side: Option<String>,
    pub timestamp: Option<i32>,
    pub price: Option<String>,
    pub quantity: Option<String>,
    pub filled: Option<String>,
    pub order_type: Option<String>,
    pub status: Option<String>,
    pub expiry: Option<i32>,
}

impl From<DbOrder> for GqlOrder {
    fn from(order: DbOrder) -> Self {
        Self {
            id: order.id,
            chain_id: order.chain_id,
            pool_id: order.pool_id,
            order_id: order.order_id,
            transaction_id: order.transaction_id,
            user: order.user,
            side: order.side,
            timestamp: order.timestamp,
            price: order.price.map(|p| p.to_string()),
            quantity: order.quantity.map(|q| q.to_string()),
            filled: order.filled.map(|f| f.to_string()),
            order_type: order.order_type,
            status: order.status,
            expiry: order.expiry,
        }
    }
}
