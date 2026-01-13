use async_graphql::SimpleObject;
use indexer_db::models::DbUser;

/// GraphQL User type
#[derive(Debug, Clone, SimpleObject)]
pub struct GqlUser {
    pub user: String,
    pub chain_id: i64,
    pub created_at: Option<i32>,
}

impl From<DbUser> for GqlUser {
    fn from(user: DbUser) -> Self {
        Self {
            user: user.user,
            chain_id: user.chain_id,
            created_at: user.created_at,
        }
    }
}
