use crate::models::{DbOrderBookTrade, DbTrade};
use crate::Result;
use sqlx::PgPool;

pub struct TradeRepository;

impl TradeRepository {
    /// Insert trade (idempotent - ignores duplicates)
    pub async fn insert(pool: &PgPool, trade: &DbTrade) -> Result<bool> {
        let result = sqlx::query(
            r#"
            INSERT INTO trades (id, chain_id, transaction_id, pool_id, order_id, price, quantity, timestamp, log_index)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&trade.id)
        .bind(trade.chain_id)
        .bind(&trade.transaction_id)
        .bind(&trade.pool_id)
        .bind(&trade.order_id)
        .bind(trade.price)
        .bind(trade.quantity)
        .bind(trade.timestamp)
        .bind(trade.log_index)
        .execute(pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Insert order book trade (market-level)
    pub async fn insert_orderbook_trade(pool: &PgPool, trade: &DbOrderBookTrade) -> Result<bool> {
        let result = sqlx::query(
            r#"
            INSERT INTO order_book_trades (id, chain_id, price, taker_limit_price, quantity,
                                          timestamp, log_index, transaction_id, side, pool_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&trade.id)
        .bind(trade.chain_id)
        .bind(trade.price)
        .bind(trade.taker_limit_price)
        .bind(trade.quantity)
        .bind(trade.timestamp)
        .bind(trade.log_index)
        .bind(&trade.transaction_id)
        .bind(&trade.side)
        .bind(&trade.pool_id)
        .execute(pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Get trade by ID
    pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Option<DbTrade>> {
        let result = sqlx::query_as::<_, DbTrade>("SELECT * FROM trades WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
        Ok(result)
    }

    /// Get recent trades for a pool
    pub async fn get_by_pool(pool: &PgPool, pool_id: &str, limit: i64) -> Result<Vec<DbTrade>> {
        let results = sqlx::query_as::<_, DbTrade>(
            "SELECT * FROM trades WHERE pool_id = $1 ORDER BY timestamp DESC, log_index DESC LIMIT $2",
        )
        .bind(pool_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;
        Ok(results)
    }

    /// Get recent orderbook trades for a pool
    pub async fn get_orderbook_trades(
        pool: &PgPool,
        pool_id: &str,
        limit: i64,
    ) -> Result<Vec<DbOrderBookTrade>> {
        let results = sqlx::query_as::<_, DbOrderBookTrade>(
            "SELECT * FROM order_book_trades WHERE pool_id = $1 ORDER BY timestamp DESC, log_index DESC LIMIT $2",
        )
        .bind(pool_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;
        Ok(results)
    }

    /// Get trades in time range for a pool (for candlestick aggregation)
    pub async fn get_by_pool_in_range(
        pool: &PgPool,
        pool_id: &str,
        from_timestamp: i32,
        to_timestamp: i32,
    ) -> Result<Vec<DbOrderBookTrade>> {
        let results = sqlx::query_as::<_, DbOrderBookTrade>(
            r#"
            SELECT * FROM order_book_trades
            WHERE pool_id = $1 AND timestamp >= $2 AND timestamp < $3
            ORDER BY timestamp ASC, log_index ASC
            "#,
        )
        .bind(pool_id)
        .bind(from_timestamp)
        .bind(to_timestamp)
        .fetch_all(pool)
        .await?;
        Ok(results)
    }

    /// Count total trades
    pub async fn count(pool: &PgPool) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM trades")
            .fetch_one(pool)
            .await?;
        Ok(count)
    }

    /// Count orderbook trades
    pub async fn count_orderbook_trades(pool: &PgPool) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM order_book_trades")
            .fetch_one(pool)
            .await?;
        Ok(count)
    }

    /// Bulk insert trades (more efficient than individual inserts)
    pub async fn bulk_insert(pool: &PgPool, trades: &[DbTrade]) -> Result<usize> {
        if trades.is_empty() {
            return Ok(0);
        }

        let ids: Vec<&str> = trades.iter().map(|t| t.id.as_str()).collect();
        let chain_ids: Vec<i64> = trades.iter().map(|t| t.chain_id).collect();
        let transaction_ids: Vec<Option<&str>> = trades
            .iter()
            .map(|t| t.transaction_id.as_deref())
            .collect();
        let pool_ids: Vec<&str> = trades.iter().map(|t| t.pool_id.as_str()).collect();
        let order_ids: Vec<Option<&str>> = trades.iter().map(|t| t.order_id.as_deref()).collect();
        let prices: Vec<Option<i64>> = trades.iter().map(|t| t.price).collect();
        let quantities: Vec<Option<i64>> = trades.iter().map(|t| t.quantity).collect();
        let timestamps: Vec<Option<i32>> = trades.iter().map(|t| t.timestamp).collect();
        let log_indices: Vec<Option<i32>> = trades.iter().map(|t| t.log_index).collect();

        sqlx::query(
            r#"
            INSERT INTO trades (id, chain_id, transaction_id, pool_id, order_id, price, quantity, timestamp, log_index)
            SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::text[], $4::text[], $5::text[], $6::bigint[], $7::bigint[], $8::int[], $9::int[])
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&ids)
        .bind(&chain_ids)
        .bind(&transaction_ids)
        .bind(&pool_ids)
        .bind(&order_ids)
        .bind(&prices)
        .bind(&quantities)
        .bind(&timestamps)
        .bind(&log_indices)
        .execute(pool)
        .await?;

        Ok(trades.len())
    }

    /// Bulk insert orderbook trades (more efficient than individual inserts)
    pub async fn bulk_insert_orderbook_trades(
        pool: &PgPool,
        trades: &[DbOrderBookTrade],
    ) -> Result<usize> {
        if trades.is_empty() {
            return Ok(0);
        }

        let ids: Vec<&str> = trades.iter().map(|t| t.id.as_str()).collect();
        let chain_ids: Vec<i64> = trades.iter().map(|t| t.chain_id).collect();
        let prices: Vec<Option<i64>> = trades.iter().map(|t| t.price).collect();
        let taker_limit_prices: Vec<Option<i64>> = trades.iter().map(|t| t.taker_limit_price).collect();
        let quantities: Vec<Option<i64>> = trades.iter().map(|t| t.quantity).collect();
        let timestamps: Vec<Option<i32>> = trades.iter().map(|t| t.timestamp).collect();
        let log_indices: Vec<Option<i32>> = trades.iter().map(|t| t.log_index).collect();
        let transaction_ids: Vec<Option<&str>> = trades
            .iter()
            .map(|t| t.transaction_id.as_deref())
            .collect();
        let sides: Vec<Option<&str>> = trades.iter().map(|t| t.side.as_deref()).collect();
        let pool_ids: Vec<&str> = trades.iter().map(|t| t.pool_id.as_str()).collect();

        sqlx::query(
            r#"
            INSERT INTO order_book_trades (id, chain_id, price, taker_limit_price, quantity, timestamp, log_index, transaction_id, side, pool_id)
            SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::int[], $7::int[], $8::text[], $9::text[], $10::text[])
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&ids)
        .bind(&chain_ids)
        .bind(&prices)
        .bind(&taker_limit_prices)
        .bind(&quantities)
        .bind(&timestamps)
        .bind(&log_indices)
        .bind(&transaction_ids)
        .bind(&sides)
        .bind(&pool_ids)
        .execute(pool)
        .await?;

        Ok(trades.len())
    }
}
