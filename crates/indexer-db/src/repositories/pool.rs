use crate::models::DbPool;
use crate::Result;
use sqlx::PgPool;

pub struct PoolRepository;

impl PoolRepository {
    /// Insert or update a pool (upsert)
    pub async fn upsert(pool: &PgPool, db_pool: &DbPool) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO pools (id, chain_id, coin, order_book, base_currency, quote_currency,
                              base_decimals, quote_decimals, timestamp, token0_price, token1_price)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (id) DO UPDATE SET
                coin = EXCLUDED.coin,
                token0_price = EXCLUDED.token0_price,
                token1_price = EXCLUDED.token1_price
            "#,
        )
        .bind(&db_pool.id)
        .bind(db_pool.chain_id)
        .bind(&db_pool.coin)
        .bind(&db_pool.order_book)
        .bind(&db_pool.base_currency)
        .bind(&db_pool.quote_currency)
        .bind(db_pool.base_decimals)
        .bind(db_pool.quote_decimals)
        .bind(db_pool.timestamp)
        .bind(db_pool.token0_price)
        .bind(db_pool.token1_price)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Get pool by ID
    pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Option<DbPool>> {
        let result = sqlx::query_as::<_, DbPool>("SELECT * FROM pools WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
        Ok(result)
    }

    /// Get pool by orderbook address
    pub async fn get_by_orderbook(pool: &PgPool, orderbook: &str) -> Result<Option<DbPool>> {
        let result = sqlx::query_as::<_, DbPool>("SELECT * FROM pools WHERE order_book = $1")
            .bind(orderbook)
            .fetch_optional(pool)
            .await?;
        Ok(result)
    }

    /// Get all pools for a chain
    pub async fn get_all_by_chain(pool: &PgPool, chain_id: i64) -> Result<Vec<DbPool>> {
        let results = sqlx::query_as::<_, DbPool>(
            "SELECT * FROM pools WHERE chain_id = $1 ORDER BY timestamp DESC",
        )
        .bind(chain_id)
        .fetch_all(pool)
        .await?;
        Ok(results)
    }

    /// Get all pools with pagination
    pub async fn get_all(
        pool: &PgPool,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<DbPool>> {
        let results = sqlx::query_as::<_, DbPool>(
            "SELECT * FROM pools ORDER BY timestamp DESC LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;
        Ok(results)
    }

    /// Count total pools
    pub async fn count(pool: &PgPool) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM pools")
            .fetch_one(pool)
            .await?;
        Ok(count)
    }

    /// Bulk upsert pools (more efficient than individual upserts)
    pub async fn bulk_upsert(pool: &PgPool, pools: &[DbPool]) -> Result<usize> {
        if pools.is_empty() {
            return Ok(0);
        }

        let ids: Vec<&str> = pools.iter().map(|p| p.id.as_str()).collect();
        let chain_ids: Vec<i64> = pools.iter().map(|p| p.chain_id).collect();
        let coins: Vec<Option<&str>> = pools.iter().map(|p| p.coin.as_deref()).collect();
        let order_books: Vec<&str> = pools.iter().map(|p| p.order_book.as_str()).collect();
        let base_currencies: Vec<&str> = pools.iter().map(|p| p.base_currency.as_str()).collect();
        let quote_currencies: Vec<&str> = pools.iter().map(|p| p.quote_currency.as_str()).collect();
        let base_decimals: Vec<Option<i16>> = pools.iter().map(|p| p.base_decimals).collect();
        let quote_decimals: Vec<Option<i16>> = pools.iter().map(|p| p.quote_decimals).collect();
        let timestamps: Vec<Option<i32>> = pools.iter().map(|p| p.timestamp).collect();
        let token0_prices: Vec<Option<f32>> = pools.iter().map(|p| p.token0_price).collect();
        let token1_prices: Vec<Option<f32>> = pools.iter().map(|p| p.token1_price).collect();

        sqlx::query(
            r#"
            INSERT INTO pools (id, chain_id, coin, order_book, base_currency, quote_currency,
                              base_decimals, quote_decimals, timestamp, token0_price, token1_price)
            SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::text[], $4::text[], $5::text[], $6::text[],
                                 $7::smallint[], $8::smallint[], $9::int[], $10::real[], $11::real[])
            ON CONFLICT (id) DO UPDATE SET
                coin = EXCLUDED.coin,
                token0_price = EXCLUDED.token0_price,
                token1_price = EXCLUDED.token1_price
            "#,
        )
        .bind(&ids)
        .bind(&chain_ids)
        .bind(&coins)
        .bind(&order_books)
        .bind(&base_currencies)
        .bind(&quote_currencies)
        .bind(&base_decimals)
        .bind(&quote_decimals)
        .bind(&timestamps)
        .bind(&token0_prices)
        .bind(&token1_prices)
        .execute(pool)
        .await?;

        Ok(pools.len())
    }
}
