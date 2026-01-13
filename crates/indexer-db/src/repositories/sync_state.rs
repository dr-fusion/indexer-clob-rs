use crate::Result;
use sqlx::PgPool;

pub struct SyncStateRepository;

impl SyncStateRepository {
    /// Get the last synced block from DB (returns None if not found or 0)
    pub async fn get_last_synced_block(pool: &PgPool) -> Result<Option<u64>> {
        let result: Option<(i64,)> = sqlx::query_as(
            "SELECT last_synced_block FROM sync_state WHERE id = 'main'",
        )
        .fetch_optional(pool)
        .await?;

        match result {
            Some((block,)) if block > 0 => Ok(Some(block as u64)),
            _ => Ok(None),
        }
    }

    /// Update the last synced block
    pub async fn set_last_synced_block(pool: &PgPool, block: u64) -> Result<()> {
        sqlx::query(
            "UPDATE sync_state SET last_synced_block = $1, updated_at = NOW() WHERE id = 'main'",
        )
        .bind(block as i64)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Get full sync state (block and timestamp)
    pub async fn get_sync_state(pool: &PgPool) -> Result<Option<(u64, Option<i64>)>> {
        let result: Option<(i64, Option<i64>)> = sqlx::query_as(
            "SELECT last_synced_block, last_synced_timestamp FROM sync_state WHERE id = 'main'",
        )
        .fetch_optional(pool)
        .await?;

        match result {
            Some((block, timestamp)) if block > 0 => Ok(Some((block as u64, timestamp))),
            _ => Ok(None),
        }
    }

    /// Update full sync state
    pub async fn set_sync_state(pool: &PgPool, block: u64, timestamp: Option<i64>) -> Result<()> {
        sqlx::query(
            "UPDATE sync_state SET last_synced_block = $1, last_synced_timestamp = $2, updated_at = NOW() WHERE id = 'main'",
        )
        .bind(block as i64)
        .bind(timestamp)
        .execute(pool)
        .await?;
        Ok(())
    }
}
