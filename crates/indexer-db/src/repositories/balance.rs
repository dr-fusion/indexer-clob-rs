use crate::models::DbBalance;
use crate::Result;
use sqlx::PgPool;

pub struct BalanceRepository;

impl BalanceRepository {
    /// Insert or update a balance (upsert)
    pub async fn upsert(pool: &PgPool, balance: &DbBalance) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO balances (id, chain_id, "user", currency, amount, locked_amount)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
                amount = EXCLUDED.amount,
                locked_amount = EXCLUDED.locked_amount
            "#,
        )
        .bind(&balance.id)
        .bind(balance.chain_id)
        .bind(&balance.user)
        .bind(&balance.currency)
        .bind(balance.amount)
        .bind(balance.locked_amount)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Get balance by ID
    pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Option<DbBalance>> {
        let result = sqlx::query_as::<_, DbBalance>("SELECT * FROM balances WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
        Ok(result)
    }

    /// Get all balances for a user
    pub async fn get_by_user(pool: &PgPool, user: &str) -> Result<Vec<DbBalance>> {
        let results = sqlx::query_as::<_, DbBalance>(
            r#"SELECT * FROM balances WHERE "user" = $1"#,
        )
        .bind(user)
        .fetch_all(pool)
        .await?;
        Ok(results)
    }

    /// Get balance for user and currency
    pub async fn get_by_user_currency(
        pool: &PgPool,
        user: &str,
        currency: &str,
    ) -> Result<Option<DbBalance>> {
        let result = sqlx::query_as::<_, DbBalance>(
            r#"SELECT * FROM balances WHERE "user" = $1 AND currency = $2"#,
        )
        .bind(user)
        .bind(currency)
        .fetch_optional(pool)
        .await?;
        Ok(result)
    }

    /// Update balance amount (atomic add)
    pub async fn add_amount(pool: &PgPool, id: &str, delta: i64) -> Result<()> {
        sqlx::query("UPDATE balances SET amount = amount + $1 WHERE id = $2")
            .bind(delta)
            .bind(id)
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Update locked amount (atomic add)
    pub async fn add_locked_amount(pool: &PgPool, id: &str, delta: i64) -> Result<()> {
        sqlx::query("UPDATE balances SET locked_amount = locked_amount + $1 WHERE id = $2")
            .bind(delta)
            .bind(id)
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Count total balance records
    pub async fn count(pool: &PgPool) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM balances")
            .fetch_one(pool)
            .await?;
        Ok(count)
    }

    /// Bulk upsert balances (more efficient than individual upserts)
    pub async fn bulk_upsert(pool: &PgPool, balances: &[DbBalance]) -> Result<usize> {
        if balances.is_empty() {
            return Ok(0);
        }

        let ids: Vec<&str> = balances.iter().map(|b| b.id.as_str()).collect();
        let chain_ids: Vec<i64> = balances.iter().map(|b| b.chain_id).collect();
        let users: Vec<Option<&str>> = balances.iter().map(|b| b.user.as_deref()).collect();
        let currencies: Vec<Option<&str>> = balances.iter().map(|b| b.currency.as_deref()).collect();
        let amounts: Vec<Option<i64>> = balances.iter().map(|b| b.amount).collect();
        let locked_amounts: Vec<Option<i64>> = balances.iter().map(|b| b.locked_amount).collect();

        sqlx::query(
            r#"
            INSERT INTO balances (id, chain_id, "user", currency, amount, locked_amount)
            SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::text[], $4::text[], $5::bigint[], $6::bigint[])
            ON CONFLICT (id) DO UPDATE SET
                amount = EXCLUDED.amount,
                locked_amount = EXCLUDED.locked_amount
            "#,
        )
        .bind(&ids)
        .bind(&chain_ids)
        .bind(&users)
        .bind(&currencies)
        .bind(&amounts)
        .bind(&locked_amounts)
        .execute(pool)
        .await?;

        Ok(balances.len())
    }
}
