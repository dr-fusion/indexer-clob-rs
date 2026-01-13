use crate::config::BatchWriterConfig;
use crate::models::{
    CandleInterval, DbBalance, DbCandle, DbCurrency, DbOrder, DbOrderBookTrade, DbOrderHistory,
    DbPool, DbTrade, DbUser,
};
use crate::pool::DatabasePool;
use crate::repositories::{
    BalanceRepository, CandleRepository, OrderRepository, PoolRepository, TradeRepository,
    UserRepository,
};
use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Types of write operations that can be batched
#[derive(Debug)]
pub enum WriteOperation {
    Pool(DbPool),
    Order(DbOrder),
    OrderHistory(DbOrderHistory),
    Trade(DbTrade),
    OrderBookTrade(DbOrderBookTrade),
    Balance(DbBalance),
    User(DbUser),
    Currency(DbCurrency),
    Candle(CandleInterval, DbCandle),
    Flush,
}

/// Batch writer for efficient database operations
pub struct BatchWriter {
    sender: mpsc::Sender<WriteOperation>,
}

impl BatchWriter {
    /// Create a new batch writer with background processing
    pub fn new(db_pool: Arc<DatabasePool>, batch_size: usize, flush_interval_ms: u64) -> Self {
        let config = BatchWriterConfig {
            batch_size,
            flush_interval_ms,
            channel_capacity: 10000,
        };
        Self::with_config(db_pool, config)
    }

    /// Create a new batch writer with configuration
    pub fn with_config(db_pool: Arc<DatabasePool>, config: BatchWriterConfig) -> Self {
        let (sender, receiver) = mpsc::channel(config.channel_capacity);

        // Spawn background writer task
        tokio::spawn(Self::writer_loop(
            db_pool,
            receiver,
            config.batch_size,
            config.flush_interval_ms,
        ));

        Self { sender }
    }

    /// Queue a pool for writing
    pub async fn write_pool(&self, pool: DbPool) -> Result<()> {
        self.sender
            .send(WriteOperation::Pool(pool))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue an order for writing
    pub async fn write_order(&self, order: DbOrder) -> Result<()> {
        self.sender
            .send(WriteOperation::Order(order))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue an order history record
    pub async fn write_order_history(&self, history: DbOrderHistory) -> Result<()> {
        self.sender
            .send(WriteOperation::OrderHistory(history))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue a trade for writing
    pub async fn write_trade(&self, trade: DbTrade) -> Result<()> {
        self.sender
            .send(WriteOperation::Trade(trade))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue an orderbook trade for writing
    pub async fn write_orderbook_trade(&self, trade: DbOrderBookTrade) -> Result<()> {
        self.sender
            .send(WriteOperation::OrderBookTrade(trade))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue a balance update
    pub async fn write_balance(&self, balance: DbBalance) -> Result<()> {
        self.sender
            .send(WriteOperation::Balance(balance))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue a user for writing
    pub async fn write_user(&self, user: DbUser) -> Result<()> {
        self.sender
            .send(WriteOperation::User(user))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue a currency for writing
    pub async fn write_currency(&self, currency: DbCurrency) -> Result<()> {
        self.sender
            .send(WriteOperation::Currency(currency))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue a candle for writing
    pub async fn write_candle(&self, interval: CandleInterval, candle: DbCandle) -> Result<()> {
        self.sender
            .send(WriteOperation::Candle(interval, candle))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Force flush all pending writes
    pub async fn flush(&self) -> Result<()> {
        self.sender
            .send(WriteOperation::Flush)
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Background writer loop
    async fn writer_loop(
        db_pool: Arc<DatabasePool>,
        mut receiver: mpsc::Receiver<WriteOperation>,
        batch_size: usize,
        flush_interval_ms: u64,
    ) {
        let mut pools: Vec<DbPool> = Vec::with_capacity(batch_size);
        let mut orders: Vec<DbOrder> = Vec::with_capacity(batch_size);
        let mut order_histories: Vec<DbOrderHistory> = Vec::with_capacity(batch_size);
        let mut trades: Vec<DbTrade> = Vec::with_capacity(batch_size);
        let mut ob_trades: Vec<DbOrderBookTrade> = Vec::with_capacity(batch_size);
        let mut balances: Vec<DbBalance> = Vec::with_capacity(batch_size);
        let mut users: Vec<DbUser> = Vec::with_capacity(batch_size);
        let mut currencies: Vec<DbCurrency> = Vec::with_capacity(batch_size);
        let mut candles: Vec<(CandleInterval, DbCandle)> = Vec::with_capacity(batch_size);

        let flush_interval = tokio::time::Duration::from_millis(flush_interval_ms);
        let mut last_flush = tokio::time::Instant::now();

        loop {
            // Wait for operations with timeout
            let op = tokio::select! {
                op = receiver.recv() => op,
                _ = tokio::time::sleep(flush_interval) => {
                    // Timeout - flush if we have pending data
                    if last_flush.elapsed() >= flush_interval {
                        Self::flush_all(
                            &db_pool,
                            &mut pools,
                            &mut orders,
                            &mut order_histories,
                            &mut trades,
                            &mut ob_trades,
                            &mut balances,
                            &mut users,
                            &mut currencies,
                            &mut candles,
                        ).await;
                        last_flush = tokio::time::Instant::now();
                    }
                    continue;
                }
            };

            match op {
                Some(WriteOperation::Pool(p)) => pools.push(p),
                Some(WriteOperation::Order(o)) => orders.push(o),
                Some(WriteOperation::OrderHistory(h)) => order_histories.push(h),
                Some(WriteOperation::Trade(t)) => trades.push(t),
                Some(WriteOperation::OrderBookTrade(t)) => ob_trades.push(t),
                Some(WriteOperation::Balance(b)) => balances.push(b),
                Some(WriteOperation::User(u)) => users.push(u),
                Some(WriteOperation::Currency(c)) => currencies.push(c),
                Some(WriteOperation::Candle(interval, candle)) => candles.push((interval, candle)),
                Some(WriteOperation::Flush) | None => {
                    // Flush everything
                    Self::flush_all(
                        &db_pool,
                        &mut pools,
                        &mut orders,
                        &mut order_histories,
                        &mut trades,
                        &mut ob_trades,
                        &mut balances,
                        &mut users,
                        &mut currencies,
                        &mut candles,
                    )
                    .await;
                    last_flush = tokio::time::Instant::now();

                    if op.is_none() {
                        info!("Writer channel closed, shutting down");
                        break;
                    }
                }
            }

            // Check if we need to flush due to batch size
            let total_pending = pools.len()
                + orders.len()
                + order_histories.len()
                + trades.len()
                + ob_trades.len()
                + balances.len()
                + users.len()
                + currencies.len()
                + candles.len();

            if total_pending >= batch_size {
                Self::flush_all(
                    &db_pool,
                    &mut pools,
                    &mut orders,
                    &mut order_histories,
                    &mut trades,
                    &mut ob_trades,
                    &mut balances,
                    &mut users,
                    &mut currencies,
                    &mut candles,
                )
                .await;
                last_flush = tokio::time::Instant::now();
            }
        }
    }

    /// Deduplicate a vector by key, keeping the last occurrence (most recent)
    fn dedup_by_key<T, K, F>(items: &mut Vec<T>, key_fn: F) -> Vec<T>
    where
        K: std::hash::Hash + Eq,
        F: Fn(&T) -> K,
    {
        let mut seen: HashMap<K, usize> = HashMap::new();
        let mut result: Vec<T> = Vec::with_capacity(items.len());

        // First pass: record last index for each key
        for (idx, item) in items.iter().enumerate() {
            seen.insert(key_fn(item), idx);
        }

        // Second pass: collect only the last occurrence of each key
        for (idx, item) in items.drain(..).enumerate() {
            if seen.get(&key_fn(&item)) == Some(&idx) {
                result.push(item);
            }
        }

        result
    }

    /// Flush all pending writes to database using bulk operations
    async fn flush_all(
        db_pool: &DatabasePool,
        pools: &mut Vec<DbPool>,
        orders: &mut Vec<DbOrder>,
        order_histories: &mut Vec<DbOrderHistory>,
        trades: &mut Vec<DbTrade>,
        ob_trades: &mut Vec<DbOrderBookTrade>,
        balances: &mut Vec<DbBalance>,
        users: &mut Vec<DbUser>,
        currencies: &mut Vec<DbCurrency>,
        candles: &mut Vec<(CandleInterval, DbCandle)>,
    ) {
        let pool = db_pool.inner();

        // Flush pools (bulk) - deduplicate by id
        if !pools.is_empty() {
            let deduped = Self::dedup_by_key(pools, |p| p.id.clone());
            let count = deduped.len();
            debug!(count, "Flushing pools (bulk)");
            match PoolRepository::bulk_upsert(pool, &deduped).await {
                Ok(n) => debug!(inserted = n, "Bulk upserted pools"),
                Err(e) => error!(error = %e, "Failed to bulk upsert pools"),
            }
        }

        // Flush orders (bulk) - deduplicate by order_id
        if !orders.is_empty() {
            let deduped = Self::dedup_by_key(orders, |o| o.order_id.clone());
            let count = deduped.len();
            debug!(count, "Flushing orders (bulk)");
            match OrderRepository::bulk_upsert(pool, &deduped).await {
                Ok(n) => debug!(inserted = n, "Bulk upserted orders"),
                Err(e) => error!(error = %e, "Failed to bulk upsert orders"),
            }
        }

        // Flush order histories (bulk) - no dedup needed, each is unique
        if !order_histories.is_empty() {
            let count = order_histories.len();
            debug!(count, "Flushing order histories (bulk)");
            match OrderRepository::bulk_insert_history(pool, order_histories).await {
                Ok(n) => debug!(inserted = n, "Bulk inserted order histories"),
                Err(e) => error!(error = %e, "Failed to bulk insert order histories"),
            }
            order_histories.clear();
        }

        // Flush trades (bulk) - no dedup needed, each trade is unique
        if !trades.is_empty() {
            let count = trades.len();
            debug!(count, "Flushing trades (bulk)");
            match TradeRepository::bulk_insert(pool, trades).await {
                Ok(n) => debug!(inserted = n, "Bulk inserted trades"),
                Err(e) => error!(error = %e, "Failed to bulk insert trades"),
            }
            trades.clear();
        }

        // Flush orderbook trades (bulk) - no dedup needed
        if !ob_trades.is_empty() {
            let count = ob_trades.len();
            debug!(count, "Flushing orderbook trades (bulk)");
            match TradeRepository::bulk_insert_orderbook_trades(pool, ob_trades).await {
                Ok(n) => debug!(inserted = n, "Bulk inserted orderbook trades"),
                Err(e) => error!(error = %e, "Failed to bulk insert orderbook trades"),
            }
            ob_trades.clear();
        }

        // Flush balances (bulk) - deduplicate by id
        if !balances.is_empty() {
            let deduped = Self::dedup_by_key(balances, |b| b.id.clone());
            let count = deduped.len();
            debug!(count, "Flushing balances (bulk)");
            match BalanceRepository::bulk_upsert(pool, &deduped).await {
                Ok(n) => debug!(inserted = n, "Bulk upserted balances"),
                Err(e) => error!(error = %e, "Failed to bulk upsert balances"),
            }
        }

        // Flush users (bulk) - deduplicate by user
        if !users.is_empty() {
            let deduped = Self::dedup_by_key(users, |u| u.user.clone());
            let count = deduped.len();
            debug!(count, "Flushing users (bulk)");
            match UserRepository::bulk_insert_ignore(pool, &deduped).await {
                Ok(n) => debug!(inserted = n, "Bulk inserted users"),
                Err(e) => error!(error = %e, "Failed to bulk insert users"),
            }
        }

        // Flush currencies (bulk) - deduplicate by (chain_id, address)
        if !currencies.is_empty() {
            let deduped = Self::dedup_by_key(currencies, |c| (c.chain_id, c.address.clone()));
            let count = deduped.len();
            debug!(count, "Flushing currencies (bulk)");
            match UserRepository::bulk_upsert_currencies(pool, &deduped).await {
                Ok(n) => debug!(inserted = n, "Bulk upserted currencies"),
                Err(e) => error!(error = %e, "Failed to bulk upsert currencies"),
            }
        }

        // Flush candles by interval (batch per interval) - deduplicate by (interval, pool_id, open_time)
        if !candles.is_empty() {
            debug!(count = candles.len(), "Flushing candles");
            // First deduplicate
            let deduped = Self::dedup_by_key(candles, |(interval, c)| {
                (*interval, c.pool_id.clone(), c.open_time)
            });
            // Group candles by interval for efficient batch operations
            let mut by_interval: HashMap<CandleInterval, Vec<DbCandle>> = HashMap::new();
            for (interval, candle) in deduped {
                by_interval.entry(interval).or_default().push(candle);
            }
            for (interval, interval_candles) in by_interval {
                match CandleRepository::batch_upsert(pool, interval, &interval_candles).await {
                    Ok(()) => debug!(interval = ?interval, count = interval_candles.len(), "Batch upserted candles"),
                    Err(e) => error!(error = %e, interval = ?interval, "Failed to batch upsert candles"),
                }
            }
        }
    }
}

impl Clone for BatchWriter {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}
