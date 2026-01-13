pub mod query;
pub mod types;

use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use indexer_db::DatabasePool;
use indexer_store::IndexerStore;
use std::sync::Arc;

pub use query::QueryRoot;

/// GraphQL Schema type
pub type ApiSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// Context shared across GraphQL resolvers
pub struct ApiContext {
    pub db_pool: Arc<DatabasePool>,
    pub store: Arc<IndexerStore>,
}

impl ApiContext {
    pub fn new(db_pool: Arc<DatabasePool>, store: Arc<IndexerStore>) -> Self {
        Self { db_pool, store }
    }
}

/// Build the GraphQL schema
pub fn build_schema(ctx: ApiContext) -> ApiSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(ctx.db_pool)
        .data(ctx.store)
        .finish()
}
