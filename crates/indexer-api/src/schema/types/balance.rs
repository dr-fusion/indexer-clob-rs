use async_graphql::SimpleObject;
use indexer_db::models::DbBalance;

/// GraphQL Balance type
#[derive(Debug, Clone, SimpleObject)]
pub struct GqlBalance {
    pub id: String,
    pub chain_id: i64,
    pub user: Option<String>,
    pub currency: Option<String>,
    pub amount: Option<String>,
    pub locked_amount: Option<String>,
}

impl From<DbBalance> for GqlBalance {
    fn from(balance: DbBalance) -> Self {
        Self {
            id: balance.id,
            chain_id: balance.chain_id,
            user: balance.user,
            currency: balance.currency,
            amount: balance.amount.map(|a| a.to_string()),
            locked_amount: balance.locked_amount.map(|a| a.to_string()),
        }
    }
}
