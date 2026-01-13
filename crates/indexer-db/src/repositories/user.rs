use crate::models::{DbCurrency, DbUser};
use crate::Result;
use sqlx::PgPool;

pub struct UserRepository;

impl UserRepository {
    /// Insert or ignore user (upsert without update)
    pub async fn insert_if_not_exists(pool: &PgPool, user: &DbUser) -> Result<bool> {
        let result = sqlx::query(
            r#"
            INSERT INTO users ("user", chain_id, created_at)
            VALUES ($1, $2, $3)
            ON CONFLICT ("user") DO NOTHING
            "#,
        )
        .bind(&user.user)
        .bind(user.chain_id)
        .bind(user.created_at)
        .execute(pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Get user by address
    pub async fn get_by_address(pool: &PgPool, address: &str) -> Result<Option<DbUser>> {
        let result = sqlx::query_as::<_, DbUser>(r#"SELECT * FROM users WHERE "user" = $1"#)
            .bind(address)
            .fetch_optional(pool)
            .await?;
        Ok(result)
    }

    /// Get all users with pagination
    pub async fn get_all(pool: &PgPool, limit: i64, offset: i64) -> Result<Vec<DbUser>> {
        let results = sqlx::query_as::<_, DbUser>(
            r#"SELECT * FROM users ORDER BY created_at DESC LIMIT $1 OFFSET $2"#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;
        Ok(results)
    }

    /// Count total users
    pub async fn count(pool: &PgPool) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
            .fetch_one(pool)
            .await?;
        Ok(count)
    }

    /// Insert or update currency
    pub async fn upsert_currency(pool: &PgPool, currency: &DbCurrency) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO currencies (id, chain_id, address, name, symbol, decimals, price_usd)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                symbol = EXCLUDED.symbol,
                decimals = EXCLUDED.decimals,
                price_usd = EXCLUDED.price_usd
            "#,
        )
        .bind(&currency.id)
        .bind(currency.chain_id)
        .bind(&currency.address)
        .bind(&currency.name)
        .bind(&currency.symbol)
        .bind(currency.decimals)
        .bind(currency.price_usd)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Get currency by ID
    pub async fn get_currency(pool: &PgPool, id: &str) -> Result<Option<DbCurrency>> {
        let result = sqlx::query_as::<_, DbCurrency>("SELECT * FROM currencies WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
        Ok(result)
    }

    /// Get all currencies
    pub async fn get_all_currencies(pool: &PgPool) -> Result<Vec<DbCurrency>> {
        let results = sqlx::query_as::<_, DbCurrency>("SELECT * FROM currencies")
            .fetch_all(pool)
            .await?;
        Ok(results)
    }

    /// Bulk insert users (ignores duplicates)
    pub async fn bulk_insert_ignore(pool: &PgPool, users: &[DbUser]) -> Result<usize> {
        if users.is_empty() {
            return Ok(0);
        }

        let user_addrs: Vec<&str> = users.iter().map(|u| u.user.as_str()).collect();
        let chain_ids: Vec<i64> = users.iter().map(|u| u.chain_id).collect();
        let created_ats: Vec<Option<i32>> = users.iter().map(|u| u.created_at).collect();

        sqlx::query(
            r#"
            INSERT INTO users ("user", chain_id, created_at)
            SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::int[])
            ON CONFLICT ("user") DO NOTHING
            "#,
        )
        .bind(&user_addrs)
        .bind(&chain_ids)
        .bind(&created_ats)
        .execute(pool)
        .await?;

        Ok(users.len())
    }

    /// Bulk upsert currencies
    pub async fn bulk_upsert_currencies(pool: &PgPool, currencies: &[DbCurrency]) -> Result<usize> {
        if currencies.is_empty() {
            return Ok(0);
        }

        let ids: Vec<&str> = currencies.iter().map(|c| c.id.as_str()).collect();
        let chain_ids: Vec<i64> = currencies.iter().map(|c| c.chain_id).collect();
        let addresses: Vec<&str> = currencies.iter().map(|c| c.address.as_str()).collect();
        let names: Vec<Option<&str>> = currencies.iter().map(|c| c.name.as_deref()).collect();
        let symbols: Vec<Option<&str>> = currencies.iter().map(|c| c.symbol.as_deref()).collect();
        let decimals_vec: Vec<Option<i16>> = currencies.iter().map(|c| c.decimals).collect();
        let prices: Vec<Option<f32>> = currencies.iter().map(|c| c.price_usd).collect();

        sqlx::query(
            r#"
            INSERT INTO currencies (id, chain_id, address, name, symbol, decimals, price_usd)
            SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::text[], $4::text[], $5::text[], $6::smallint[], $7::real[])
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                symbol = EXCLUDED.symbol,
                decimals = EXCLUDED.decimals,
                price_usd = EXCLUDED.price_usd
            "#,
        )
        .bind(&ids)
        .bind(&chain_ids)
        .bind(&addresses)
        .bind(&names)
        .bind(&symbols)
        .bind(&decimals_vec)
        .bind(&prices)
        .execute(pool)
        .await?;

        Ok(currencies.len())
    }
}
