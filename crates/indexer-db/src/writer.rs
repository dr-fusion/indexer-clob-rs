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
use tokio::sync::{mpsc, Mutex};
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

/// Internal state for BatchWriter that needs interior mutability
struct BatchWriterInner {
    sender: Option<mpsc::Sender<WriteOperation>>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Batch writer for efficient database operations
pub struct BatchWriter {
    inner: Mutex<BatchWriterInner>,
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
        let handle = tokio::spawn(Self::writer_loop(
            db_pool,
            receiver,
            config.batch_size,
            config.flush_interval_ms,
        ));

        Self {
            inner: Mutex::new(BatchWriterInner {
                sender: Some(sender),
                task_handle: Some(handle),
            }),
        }
    }

    /// Shutdown the writer and wait for the final flush to complete
    pub async fn shutdown(&self) {
        let handle = {
            let mut inner = self.inner.lock().await;
            // Drop the sender to close the channel, triggering final flush
            inner.sender.take();
            // Take the handle to wait on it
            inner.task_handle.take()
        };

        // Wait for the writer task to complete (outside the lock)
        if let Some(handle) = handle {
            match handle.await {
                Ok(()) => {
                    info!("BatchWriter task completed successfully");
                }
                Err(e) => {
                    error!(error = %e, "BatchWriter task panicked");
                }
            }
        }
    }

    /// Get a clone of the sender for sending operations
    async fn get_sender(&self) -> Result<mpsc::Sender<WriteOperation>> {
        let inner = self.inner.lock().await;
        inner
            .sender
            .clone()
            .ok_or_else(|| crate::DatabaseError::Query("BatchWriter is shutdown".to_string()))
    }

    /// Queue a pool for writing
    pub async fn write_pool(&self, pool: DbPool) -> Result<()> {
        self.get_sender()
            .await?
            .send(WriteOperation::Pool(pool))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue an order for writing
    pub async fn write_order(&self, order: DbOrder) -> Result<()> {
        self.get_sender()
            .await?
            .send(WriteOperation::Order(order))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue an order history record
    pub async fn write_order_history(&self, history: DbOrderHistory) -> Result<()> {
        self.get_sender()
            .await?
            .send(WriteOperation::OrderHistory(history))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue a trade for writing
    pub async fn write_trade(&self, trade: DbTrade) -> Result<()> {
        self.get_sender()
            .await?
            .send(WriteOperation::Trade(trade))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue an orderbook trade for writing
    pub async fn write_orderbook_trade(&self, trade: DbOrderBookTrade) -> Result<()> {
        self.get_sender()
            .await?
            .send(WriteOperation::OrderBookTrade(trade))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue a balance update
    pub async fn write_balance(&self, balance: DbBalance) -> Result<()> {
        self.get_sender()
            .await?
            .send(WriteOperation::Balance(balance))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue a user for writing
    pub async fn write_user(&self, user: DbUser) -> Result<()> {
        self.get_sender()
            .await?
            .send(WriteOperation::User(user))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue a currency for writing
    pub async fn write_currency(&self, currency: DbCurrency) -> Result<()> {
        self.get_sender()
            .await?
            .send(WriteOperation::Currency(currency))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Queue a candle for writing
    pub async fn write_candle(&self, interval: CandleInterval, candle: DbCandle) -> Result<()> {
        self.get_sender()
            .await?
            .send(WriteOperation::Candle(interval, candle))
            .await
            .map_err(|e| crate::DatabaseError::Query(e.to_string()))?;
        Ok(())
    }

    /// Force flush all pending writes
    pub async fn flush(&self) -> Result<()> {
        self.get_sender()
            .await?
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
        use std::time::Instant;

        let flush_start = Instant::now();
        let pool = db_pool.inner();

        let total_pending = pools.len()
            + orders.len()
            + order_histories.len()
            + trades.len()
            + ob_trades.len()
            + balances.len()
            + users.len()
            + currencies.len()
            + candles.len();

        if total_pending == 0 {
            return;
        }

        let mut stats = FlushStats::default();

        // Flush pools (bulk) - deduplicate by id
        if !pools.is_empty() {
            let pool_start = Instant::now();
            let deduped = Self::dedup_by_key(pools, |p| p.id.clone());
            stats.pools = deduped.len();
            match PoolRepository::bulk_upsert(pool, &deduped).await {
                Ok(_) => {
                    debug!(
                        count = stats.pools,
                        duration_ms = pool_start.elapsed().as_millis(),
                        "DB: Bulk upserted pools"
                    );
                }
                Err(e) => error!(error = %e, count = stats.pools, "Failed to bulk upsert pools"),
            }
        }

        // Flush orders (bulk) - deduplicate by order_id
        if !orders.is_empty() {
            let order_start = Instant::now();
            let deduped = Self::dedup_by_key(orders, |o| o.order_id.clone());
            stats.orders = deduped.len();
            match OrderRepository::bulk_upsert(pool, &deduped).await {
                Ok(_) => {
                    debug!(
                        count = stats.orders,
                        duration_ms = order_start.elapsed().as_millis(),
                        "DB: Bulk upserted orders"
                    );
                }
                Err(e) => error!(error = %e, count = stats.orders, "Failed to bulk upsert orders"),
            }
        }

        // Flush order histories (bulk) - no dedup needed, each is unique
        if !order_histories.is_empty() {
            let history_start = Instant::now();
            stats.order_histories = order_histories.len();
            match OrderRepository::bulk_insert_history(pool, order_histories).await {
                Ok(_) => {
                    debug!(
                        count = stats.order_histories,
                        duration_ms = history_start.elapsed().as_millis(),
                        "DB: Bulk inserted order histories"
                    );
                }
                Err(e) => error!(error = %e, count = stats.order_histories, "Failed to bulk insert order histories"),
            }
            order_histories.clear();
        }

        // Flush trades (bulk) - no dedup needed, each trade is unique
        if !trades.is_empty() {
            let trade_start = Instant::now();
            stats.trades = trades.len();
            match TradeRepository::bulk_insert(pool, trades).await {
                Ok(_) => {
                    debug!(
                        count = stats.trades,
                        duration_ms = trade_start.elapsed().as_millis(),
                        "DB: Bulk inserted trades"
                    );
                }
                Err(e) => error!(error = %e, count = stats.trades, "Failed to bulk insert trades"),
            }
            trades.clear();
        }

        // Flush orderbook trades (bulk) - no dedup needed
        if !ob_trades.is_empty() {
            let ob_start = Instant::now();
            stats.ob_trades = ob_trades.len();
            match TradeRepository::bulk_insert_orderbook_trades(pool, ob_trades).await {
                Ok(_) => {
                    debug!(
                        count = stats.ob_trades,
                        duration_ms = ob_start.elapsed().as_millis(),
                        "DB: Bulk inserted orderbook trades"
                    );
                }
                Err(e) => error!(error = %e, count = stats.ob_trades, "Failed to bulk insert orderbook trades"),
            }
            ob_trades.clear();
        }

        // Flush balances (bulk) - deduplicate by id
        if !balances.is_empty() {
            let balance_start = Instant::now();
            let deduped = Self::dedup_by_key(balances, |b| b.id.clone());
            stats.balances = deduped.len();
            match BalanceRepository::bulk_upsert(pool, &deduped).await {
                Ok(_) => {
                    debug!(
                        count = stats.balances,
                        duration_ms = balance_start.elapsed().as_millis(),
                        "DB: Bulk upserted balances"
                    );
                }
                Err(e) => error!(error = %e, count = stats.balances, "Failed to bulk upsert balances"),
            }
        }

        // Flush users (bulk) - deduplicate by user
        if !users.is_empty() {
            let user_start = Instant::now();
            let deduped = Self::dedup_by_key(users, |u| u.user.clone());
            stats.users = deduped.len();
            match UserRepository::bulk_insert_ignore(pool, &deduped).await {
                Ok(_) => {
                    debug!(
                        count = stats.users,
                        duration_ms = user_start.elapsed().as_millis(),
                        "DB: Bulk inserted users"
                    );
                }
                Err(e) => error!(error = %e, count = stats.users, "Failed to bulk insert users"),
            }
        }

        // Flush currencies (bulk) - deduplicate by (chain_id, address)
        if !currencies.is_empty() {
            let currency_start = Instant::now();
            let deduped = Self::dedup_by_key(currencies, |c| (c.chain_id, c.address.clone()));
            stats.currencies = deduped.len();
            match UserRepository::bulk_upsert_currencies(pool, &deduped).await {
                Ok(_) => {
                    debug!(
                        count = stats.currencies,
                        duration_ms = currency_start.elapsed().as_millis(),
                        "DB: Bulk upserted currencies"
                    );
                }
                Err(e) => error!(error = %e, count = stats.currencies, "Failed to bulk upsert currencies"),
            }
        }

        // Flush candles by interval (batch per interval) - deduplicate by (interval, pool_id, open_time)
        if !candles.is_empty() {
            let candle_start = Instant::now();
            let deduped = Self::dedup_by_key(candles, |(interval, c)| {
                (*interval, c.pool_id.clone(), c.open_time)
            });
            stats.candles = deduped.len();
            let mut by_interval: HashMap<CandleInterval, Vec<DbCandle>> = HashMap::new();
            for (interval, candle) in deduped {
                by_interval.entry(interval).or_default().push(candle);
            }
            for (interval, interval_candles) in by_interval {
                let interval_start = Instant::now();
                match CandleRepository::batch_upsert(pool, interval, &interval_candles).await {
                    Ok(()) => {
                        debug!(
                            interval = ?interval,
                            count = interval_candles.len(),
                            duration_ms = interval_start.elapsed().as_millis(),
                            "DB: Batch upserted candles"
                        );
                    }
                    Err(e) => error!(error = %e, interval = ?interval, "Failed to batch upsert candles"),
                }
            }
            debug!(
                total_candles = stats.candles,
                duration_ms = candle_start.elapsed().as_millis(),
                "DB: All candles flushed"
            );
        }

        let elapsed_ms = flush_start.elapsed().as_millis();
        info!(
            pools = stats.pools,
            orders = stats.orders,
            order_histories = stats.order_histories,
            trades = stats.trades,
            ob_trades = stats.ob_trades,
            balances = stats.balances,
            users = stats.users,
            currencies = stats.currencies,
            candles = stats.candles,
            total = stats.total(),
            elapsed_ms = elapsed_ms,
            "DB flush complete"
        );
    }
}

#[derive(Default)]
struct FlushStats {
    pools: usize,
    orders: usize,
    order_histories: usize,
    trades: usize,
    ob_trades: usize,
    balances: usize,
    users: usize,
    currencies: usize,
    candles: usize,
}

impl FlushStats {
    fn total(&self) -> usize {
        self.pools
            + self.orders
            + self.order_histories
            + self.trades
            + self.ob_trades
            + self.balances
            + self.users
            + self.currencies
            + self.candles
    }
}

// Note: BatchWriter should be used with Arc<BatchWriter> for shared ownership
// Clone is not implemented as the interior Mutex state shouldn't be duplicated
