use crate::models::{DbOrder, DbOrderHistory};
use crate::Result;
use sqlx::PgPool;

pub struct OrderRepository;

impl OrderRepository {
    /// Insert or update an order (upsert)
    pub async fn upsert(pool: &PgPool, order: &DbOrder) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO orders (id, chain_id, pool_id, order_id, transaction_id, "user",
                               side, timestamp, price, quantity, filled, total_filled_value,
                               type, status, expiry)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (id) DO UPDATE SET
                filled = EXCLUDED.filled,
                total_filled_value = EXCLUDED.total_filled_value,
                status = EXCLUDED.status
            "#,
        )
        .bind(&order.id)
        .bind(order.chain_id)
        .bind(&order.pool_id)
        .bind(order.order_id)
        .bind(&order.transaction_id)
        .bind(&order.user)
        .bind(&order.side)
        .bind(order.timestamp)
        .bind(order.price)
        .bind(order.quantity)
        .bind(order.filled)
        .bind(order.total_filled_value)
        .bind(&order.order_type)
        .bind(&order.status)
        .bind(order.expiry)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Get order by ID
    pub async fn get_by_id(pool: &PgPool, id: &str) -> Result<Option<DbOrder>> {
        let result = sqlx::query_as::<_, DbOrder>("SELECT * FROM orders WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
        Ok(result)
    }

    /// Get orders by pool
    pub async fn get_by_pool(
        pool: &PgPool,
        pool_id: &str,
        status: Option<&str>,
        limit: i64,
    ) -> Result<Vec<DbOrder>> {
        let results = if let Some(status) = status {
            sqlx::query_as::<_, DbOrder>(
                "SELECT * FROM orders WHERE pool_id = $1 AND status = $2 ORDER BY timestamp DESC LIMIT $3",
            )
            .bind(pool_id)
            .bind(status)
            .bind(limit)
            .fetch_all(pool)
            .await?
        } else {
            sqlx::query_as::<_, DbOrder>(
                "SELECT * FROM orders WHERE pool_id = $1 ORDER BY timestamp DESC LIMIT $2",
            )
            .bind(pool_id)
            .bind(limit)
            .fetch_all(pool)
            .await?
        };
        Ok(results)
    }

    /// Get orders by user
    pub async fn get_by_user(
        pool: &PgPool,
        user: &str,
        status: Option<&str>,
        limit: i64,
    ) -> Result<Vec<DbOrder>> {
        let results = if let Some(status) = status {
            sqlx::query_as::<_, DbOrder>(
                r#"SELECT * FROM orders WHERE "user" = $1 AND status = $2 ORDER BY timestamp DESC LIMIT $3"#,
            )
            .bind(user)
            .bind(status)
            .bind(limit)
            .fetch_all(pool)
            .await?
        } else {
            sqlx::query_as::<_, DbOrder>(
                r#"SELECT * FROM orders WHERE "user" = $1 ORDER BY timestamp DESC LIMIT $2"#,
            )
            .bind(user)
            .bind(limit)
            .fetch_all(pool)
            .await?
        };
        Ok(results)
    }

    /// Get open orders for orderbook (bids/asks)
    pub async fn get_open_orders_by_side(
        pool: &PgPool,
        pool_id: &str,
        side: &str,
        limit: i64,
    ) -> Result<Vec<DbOrder>> {
        let order_by = if side == "BUY" {
            "price DESC"
        } else {
            "price ASC"
        };
        let query = format!(
            "SELECT * FROM orders WHERE pool_id = $1 AND side = $2 AND status = 'OPEN' ORDER BY {} LIMIT $3",
            order_by
        );
        let results = sqlx::query_as::<_, DbOrder>(&query)
            .bind(pool_id)
            .bind(side)
            .bind(limit)
            .fetch_all(pool)
            .await?;
        Ok(results)
    }

    /// Insert order history record
    pub async fn insert_history(pool: &PgPool, history: &DbOrderHistory) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO order_history (id, chain_id, pool_id, order_id, transaction_id, timestamp, filled, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&history.id)
        .bind(history.chain_id)
        .bind(&history.pool_id)
        .bind(&history.order_id)
        .bind(&history.transaction_id)
        .bind(history.timestamp)
        .bind(history.filled)
        .bind(&history.status)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Get order history
    pub async fn get_history(
        pool: &PgPool,
        order_id: &str,
        limit: i64,
    ) -> Result<Vec<DbOrderHistory>> {
        let results = sqlx::query_as::<_, DbOrderHistory>(
            "SELECT * FROM order_history WHERE order_id = $1 ORDER BY timestamp DESC LIMIT $2",
        )
        .bind(order_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;
        Ok(results)
    }

    /// Count total orders
    pub async fn count(pool: &PgPool) -> Result<i64> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM orders")
            .fetch_one(pool)
            .await?;
        Ok(count)
    }

    /// Bulk upsert orders (more efficient than individual upserts)
    pub async fn bulk_upsert(pool: &PgPool, orders: &[DbOrder]) -> Result<usize> {
        if orders.is_empty() {
            return Ok(0);
        }

        // Extract columns as arrays
        let ids: Vec<&str> = orders.iter().map(|o| o.id.as_str()).collect();
        let chain_ids: Vec<i64> = orders.iter().map(|o| o.chain_id).collect();
        let pool_ids: Vec<&str> = orders.iter().map(|o| o.pool_id.as_str()).collect();
        let order_ids: Vec<i64> = orders.iter().map(|o| o.order_id).collect();
        let transaction_ids: Vec<Option<&str>> = orders
            .iter()
            .map(|o| o.transaction_id.as_deref())
            .collect();
        let users: Vec<Option<&str>> = orders.iter().map(|o| o.user.as_deref()).collect();
        let sides: Vec<Option<&str>> = orders.iter().map(|o| o.side.as_deref()).collect();
        let timestamps: Vec<Option<i32>> = orders.iter().map(|o| o.timestamp).collect();
        let prices: Vec<Option<i64>> = orders.iter().map(|o| o.price).collect();
        let quantities: Vec<Option<i64>> = orders.iter().map(|o| o.quantity).collect();
        let filleds: Vec<Option<i64>> = orders.iter().map(|o| o.filled).collect();
        let total_filled_values: Vec<Option<i64>> =
            orders.iter().map(|o| o.total_filled_value).collect();
        let order_types: Vec<Option<&str>> = orders.iter().map(|o| o.order_type.as_deref()).collect();
        let statuses: Vec<Option<&str>> = orders.iter().map(|o| o.status.as_deref()).collect();
        let expiries: Vec<Option<i32>> = orders.iter().map(|o| o.expiry).collect();

        sqlx::query(
            r#"
            INSERT INTO orders (id, chain_id, pool_id, order_id, transaction_id, "user",
                               side, timestamp, price, quantity, filled, total_filled_value,
                               type, status, expiry)
            SELECT * FROM UNNEST(
                $1::text[], $2::bigint[], $3::text[], $4::bigint[], $5::text[],
                $6::text[], $7::text[], $8::int[], $9::bigint[], $10::bigint[],
                $11::bigint[], $12::bigint[], $13::text[], $14::text[], $15::int[]
            )
            ON CONFLICT (id) DO UPDATE SET
                filled = EXCLUDED.filled,
                total_filled_value = EXCLUDED.total_filled_value,
                status = EXCLUDED.status
            "#,
        )
        .bind(&ids)
        .bind(&chain_ids)
        .bind(&pool_ids)
        .bind(&order_ids)
        .bind(&transaction_ids)
        .bind(&users)
        .bind(&sides)
        .bind(&timestamps)
        .bind(&prices)
        .bind(&quantities)
        .bind(&filleds)
        .bind(&total_filled_values)
        .bind(&order_types)
        .bind(&statuses)
        .bind(&expiries)
        .execute(pool)
        .await?;

        Ok(orders.len())
    }

    /// Bulk insert order history records
    pub async fn bulk_insert_history(pool: &PgPool, histories: &[DbOrderHistory]) -> Result<usize> {
        if histories.is_empty() {
            return Ok(0);
        }

        let ids: Vec<&str> = histories.iter().map(|h| h.id.as_str()).collect();
        let chain_ids: Vec<i64> = histories.iter().map(|h| h.chain_id).collect();
        let pool_ids: Vec<&str> = histories.iter().map(|h| h.pool_id.as_str()).collect();
        let order_ids: Vec<Option<&str>> = histories.iter().map(|h| h.order_id.as_deref()).collect();
        let transaction_ids: Vec<Option<&str>> = histories
            .iter()
            .map(|h| h.transaction_id.as_deref())
            .collect();
        let timestamps: Vec<Option<i32>> = histories.iter().map(|h| h.timestamp).collect();
        let filleds: Vec<Option<i64>> = histories.iter().map(|h| h.filled).collect();
        let statuses: Vec<Option<&str>> = histories.iter().map(|h| h.status.as_deref()).collect();

        sqlx::query(
            r#"
            INSERT INTO order_history (id, chain_id, pool_id, order_id, transaction_id, timestamp, filled, status)
            SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::text[], $4::text[], $5::text[], $6::int[], $7::bigint[], $8::text[])
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(&ids)
        .bind(&chain_ids)
        .bind(&pool_ids)
        .bind(&order_ids)
        .bind(&transaction_ids)
        .bind(&timestamps)
        .bind(&filleds)
        .bind(&statuses)
        .execute(pool)
        .await?;

        Ok(histories.len())
    }
}
