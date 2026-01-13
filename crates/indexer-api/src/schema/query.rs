use async_graphql::{Context, Object, Result};
use indexer_db::repositories::{
    BalanceRepository, CandleRepository, OrderRepository, PoolRepository, TradeRepository,
    UserRepository,
};
use indexer_db::DatabasePool;
use indexer_store::IndexerStore;
use std::sync::Arc;

use super::types::{
    CandleIntervalInput, GqlBalance, GqlCandle, GqlOrder, GqlPool, GqlTrade, GqlUser,
};

/// Root query type
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Get a pool by ID
    async fn pool(&self, ctx: &Context<'_>, id: String) -> Result<Option<GqlPool>> {
        let db = ctx.data::<Arc<DatabasePool>>()?;
        let pool = PoolRepository::get_by_id(db.inner(), &id).await?;
        Ok(pool.map(GqlPool::from))
    }

    /// Get all pools
    async fn pools(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 100)] limit: i32,
        #[graphql(default = 0)] offset: i32,
    ) -> Result<Vec<GqlPool>> {
        let db = ctx.data::<Arc<DatabasePool>>()?;
        let pools = PoolRepository::get_all(db.inner(), limit as i64, offset as i64).await?;
        Ok(pools.into_iter().map(GqlPool::from).collect())
    }

    /// Get pool by orderbook address
    async fn pool_by_orderbook(
        &self,
        ctx: &Context<'_>,
        orderbook: String,
    ) -> Result<Option<GqlPool>> {
        let db = ctx.data::<Arc<DatabasePool>>()?;
        let pool = PoolRepository::get_by_orderbook(db.inner(), &orderbook).await?;
        Ok(pool.map(GqlPool::from))
    }

    /// Get an order by ID
    async fn order(&self, ctx: &Context<'_>, id: String) -> Result<Option<GqlOrder>> {
        let db = ctx.data::<Arc<DatabasePool>>()?;
        let order = OrderRepository::get_by_id(db.inner(), &id).await?;
        Ok(order.map(GqlOrder::from))
    }

    /// Get orders for a pool
    async fn orders(
        &self,
        ctx: &Context<'_>,
        pool_id: String,
        status: Option<String>,
        #[graphql(default = 100)] limit: i32,
    ) -> Result<Vec<GqlOrder>> {
        let db = ctx.data::<Arc<DatabasePool>>()?;
        let orders = OrderRepository::get_by_pool(
            db.inner(),
            &pool_id,
            status.as_deref(),
            limit as i64,
        )
        .await?;
        Ok(orders.into_iter().map(GqlOrder::from).collect())
    }

    /// Get orders for a user
    async fn user_orders(
        &self,
        ctx: &Context<'_>,
        user: String,
        status: Option<String>,
        #[graphql(default = 100)] limit: i32,
    ) -> Result<Vec<GqlOrder>> {
        let db = ctx.data::<Arc<DatabasePool>>()?;
        let orders =
            OrderRepository::get_by_user(db.inner(), &user, status.as_deref(), limit as i64)
                .await?;
        Ok(orders.into_iter().map(GqlOrder::from).collect())
    }

    /// Get orderbook (open orders by side)
    async fn orderbook(
        &self,
        ctx: &Context<'_>,
        pool_id: String,
        side: String,
        #[graphql(default = 50)] limit: i32,
    ) -> Result<Vec<GqlOrder>> {
        let db = ctx.data::<Arc<DatabasePool>>()?;
        let orders =
            OrderRepository::get_open_orders_by_side(db.inner(), &pool_id, &side, limit as i64)
                .await?;
        Ok(orders.into_iter().map(GqlOrder::from).collect())
    }

    /// Get recent trades for a pool
    async fn trades(
        &self,
        ctx: &Context<'_>,
        pool_id: String,
        #[graphql(default = 100)] limit: i32,
    ) -> Result<Vec<GqlTrade>> {
        let db = ctx.data::<Arc<DatabasePool>>()?;
        let trades = TradeRepository::get_orderbook_trades(db.inner(), &pool_id, limit as i64).await?;
        Ok(trades.into_iter().map(GqlTrade::from).collect())
    }

    /// Get candlestick data
    async fn candles(
        &self,
        ctx: &Context<'_>,
        pool_id: String,
        interval: CandleIntervalInput,
        from: Option<i32>,
        to: Option<i32>,
        #[graphql(default = 100)] limit: i32,
    ) -> Result<Vec<GqlCandle>> {
        let db = ctx.data::<Arc<DatabasePool>>()?;
        let db_interval = interval.to_db_interval();

        let candles = if let (Some(from), Some(to)) = (from, to) {
            CandleRepository::get_by_pool_range(
                db.inner(),
                db_interval,
                &pool_id,
                from,
                to,
                limit as i64,
            )
            .await?
        } else {
            CandleRepository::get_recent(db.inner(), db_interval, &pool_id, limit as i64).await?
        };

        Ok(candles.into_iter().map(GqlCandle::from).collect())
    }

    /// Get user information
    async fn user(&self, ctx: &Context<'_>, address: String) -> Result<Option<GqlUser>> {
        let db = ctx.data::<Arc<DatabasePool>>()?;
        let user = UserRepository::get_by_address(db.inner(), &address).await?;
        Ok(user.map(GqlUser::from))
    }

    /// Get user balances
    async fn balances(&self, ctx: &Context<'_>, user: String) -> Result<Vec<GqlBalance>> {
        let db = ctx.data::<Arc<DatabasePool>>()?;
        let balances = BalanceRepository::get_by_user(db.inner(), &user).await?;
        Ok(balances.into_iter().map(GqlBalance::from).collect())
    }

    /// Get indexer stats (from in-memory store)
    async fn stats(&self, ctx: &Context<'_>) -> Result<IndexerStats> {
        let store = ctx.data::<Arc<IndexerStore>>()?;
        let state = store.sync_state.read().await;

        Ok(IndexerStats {
            last_synced_block: state.last_synced_block(),
            total_pools: store.pools.count() as i64,
            total_orders: store.orders.count() as i64,
            total_trades: store.trades.count() as i64,
            is_syncing: state.is_syncing(),
        })
    }

    /// Health check
    async fn health(&self) -> Result<bool> {
        Ok(true)
    }
}

/// Indexer statistics
#[derive(Debug, Clone, async_graphql::SimpleObject)]
pub struct IndexerStats {
    pub last_synced_block: u64,
    pub total_pools: i64,
    pub total_orders: i64,
    pub total_trades: i64,
    pub is_syncing: bool,
}
