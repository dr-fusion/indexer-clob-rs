use crate::models::{CandleInterval, DbCandle};
use crate::Result;
use sqlx::PgPool;

pub struct CandleRepository;

impl CandleRepository {
    /// Insert or update a candle (upsert)
    pub async fn upsert(pool: &PgPool, interval: CandleInterval, candle: &DbCandle) -> Result<()> {
        let table = interval.table_name();
        let query = format!(
            r#"
            INSERT INTO {} (id, chain_id, open_time, close_time, open, high, low, close,
                           volume, quote_volume, volume_usd, count,
                           taker_buy_base_volume, taker_buy_quote_volume, average, pool_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (id) DO UPDATE SET
                high = GREATEST({}.high, EXCLUDED.high),
                low = LEAST({}.low, EXCLUDED.low),
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                quote_volume = EXCLUDED.quote_volume,
                volume_usd = EXCLUDED.volume_usd,
                count = EXCLUDED.count,
                taker_buy_base_volume = EXCLUDED.taker_buy_base_volume,
                taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume,
                average = EXCLUDED.average
            "#,
            table, table, table
        );

        sqlx::query(&query)
            .bind(&candle.id)
            .bind(candle.chain_id)
            .bind(candle.open_time)
            .bind(candle.close_time)
            .bind(candle.open)
            .bind(candle.high)
            .bind(candle.low)
            .bind(candle.close)
            .bind(candle.volume)
            .bind(candle.quote_volume)
            .bind(candle.volume_usd)
            .bind(candle.count)
            .bind(candle.taker_buy_base_volume)
            .bind(candle.taker_buy_quote_volume)
            .bind(candle.average)
            .bind(&candle.pool_id)
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Get candle by ID
    pub async fn get_by_id(
        pool: &PgPool,
        interval: CandleInterval,
        id: &str,
    ) -> Result<Option<DbCandle>> {
        let table = interval.table_name();
        let query = format!("SELECT * FROM {} WHERE id = $1", table);
        let result = sqlx::query_as::<_, DbCandle>(&query)
            .bind(id)
            .fetch_optional(pool)
            .await?;
        Ok(result)
    }

    /// Get candles for a pool in time range
    pub async fn get_by_pool_range(
        pool: &PgPool,
        interval: CandleInterval,
        pool_id: &str,
        from_time: i32,
        to_time: i32,
        limit: i64,
    ) -> Result<Vec<DbCandle>> {
        let table = interval.table_name();
        let query = format!(
            r#"
            SELECT * FROM {}
            WHERE pool_id = $1 AND open_time >= $2 AND open_time < $3
            ORDER BY open_time ASC
            LIMIT $4
            "#,
            table
        );
        let results = sqlx::query_as::<_, DbCandle>(&query)
            .bind(pool_id)
            .bind(from_time)
            .bind(to_time)
            .bind(limit)
            .fetch_all(pool)
            .await?;
        Ok(results)
    }

    /// Get recent candles for a pool
    pub async fn get_recent(
        pool: &PgPool,
        interval: CandleInterval,
        pool_id: &str,
        limit: i64,
    ) -> Result<Vec<DbCandle>> {
        let table = interval.table_name();
        let query = format!(
            "SELECT * FROM {} WHERE pool_id = $1 ORDER BY open_time DESC LIMIT $2",
            table
        );
        let results = sqlx::query_as::<_, DbCandle>(&query)
            .bind(pool_id)
            .bind(limit)
            .fetch_all(pool)
            .await?;
        Ok(results)
    }

    /// Get the latest candle for a pool
    pub async fn get_latest(
        pool: &PgPool,
        interval: CandleInterval,
        pool_id: &str,
    ) -> Result<Option<DbCandle>> {
        let table = interval.table_name();
        let query = format!(
            "SELECT * FROM {} WHERE pool_id = $1 ORDER BY open_time DESC LIMIT 1",
            table
        );
        let result = sqlx::query_as::<_, DbCandle>(&query)
            .bind(pool_id)
            .fetch_optional(pool)
            .await?;
        Ok(result)
    }

    /// Count candles for an interval
    pub async fn count(pool: &PgPool, interval: CandleInterval) -> Result<i64> {
        let table = interval.table_name();
        let query = format!("SELECT COUNT(*) FROM {}", table);
        let (count,): (i64,) = sqlx::query_as(&query).fetch_one(pool).await?;
        Ok(count)
    }

    /// Batch upsert candles (for efficiency)
    pub async fn batch_upsert(
        pool: &PgPool,
        interval: CandleInterval,
        candles: &[DbCandle],
    ) -> Result<()> {
        if candles.is_empty() {
            return Ok(());
        }

        // Use transaction for batch operations
        let mut tx = pool.begin().await?;

        for candle in candles {
            let table = interval.table_name();
            let query = format!(
                r#"
                INSERT INTO {} (id, chain_id, open_time, close_time, open, high, low, close,
                               volume, quote_volume, volume_usd, count,
                               taker_buy_base_volume, taker_buy_quote_volume, average, pool_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                ON CONFLICT (id) DO UPDATE SET
                    high = GREATEST({}.high, EXCLUDED.high),
                    low = LEAST({}.low, EXCLUDED.low),
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    quote_volume = EXCLUDED.quote_volume,
                    volume_usd = EXCLUDED.volume_usd,
                    count = EXCLUDED.count,
                    taker_buy_base_volume = EXCLUDED.taker_buy_base_volume,
                    taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume,
                    average = EXCLUDED.average
                "#,
                table, table, table
            );

            sqlx::query(&query)
                .bind(&candle.id)
                .bind(candle.chain_id)
                .bind(candle.open_time)
                .bind(candle.close_time)
                .bind(candle.open)
                .bind(candle.high)
                .bind(candle.low)
                .bind(candle.close)
                .bind(candle.volume)
                .bind(candle.quote_volume)
                .bind(candle.volume_usd)
                .bind(candle.count)
                .bind(candle.taker_buy_base_volume)
                .bind(candle.taker_buy_quote_volume)
                .bind(candle.average)
                .bind(&candle.pool_id)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
