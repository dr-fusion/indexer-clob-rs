use alloy_primitives::{Address, FixedBytes};
use dashmap::DashMap;
use indexer_core::types::{Trade, TradeKey};
use std::time::Instant;
use tracing::debug;

/// Thread-safe store for trades
#[derive(Debug)]
pub struct TradeStore {
    /// TradeKey (tx_hash + log_index) -> Trade
    trades: DashMap<TradeKey, Trade>,

    /// pool_id -> Vec<TradeKey> (chronological order)
    pool_trades: DashMap<FixedBytes<32>, Vec<TradeKey>>,

    /// user -> Vec<TradeKey>
    user_trades: DashMap<Address, Vec<TradeKey>>,
}

impl TradeStore {
    pub fn new() -> Self {
        Self {
            trades: DashMap::new(),
            pool_trades: DashMap::new(),
            user_trades: DashMap::new(),
        }
    }

    /// Insert a trade (idempotent - duplicate keys are ignored)
    /// Returns true if trade was inserted, false if it already existed
    pub fn insert(&self, trade: Trade) -> bool {
        let start = Instant::now();
        let key = trade.key;
        let pool_id = trade.pool_id;
        let buy_order_id = trade.buy_order_id;
        let sell_order_id = trade.sell_order_id;
        let taker_address = trade.taker_address;
        let execution_price = trade.execution_price;

        // Check if trade already exists (idempotent)
        let dup_check_start = Instant::now();
        if self.trades.contains_key(&key) {
            let dup_check_us = dup_check_start.elapsed().as_micros();
            debug!(
                buy_order = buy_order_id,
                sell_order = sell_order_id,
                pool_id = ?pool_id,
                dup_check_us = dup_check_us,
                "Trade already exists (duplicate), skipping insert"
            );
            return false;
        }
        let dup_check_us = dup_check_start.elapsed().as_micros();

        // Index by pool
        let pool_index_start = Instant::now();
        self.pool_trades
            .entry(trade.pool_id)
            .or_insert_with(Vec::new)
            .push(key);
        let pool_index_us = pool_index_start.elapsed().as_micros();

        // Index by user (taker)
        let user_index_start = Instant::now();
        self.user_trades
            .entry(trade.taker_address)
            .or_insert_with(Vec::new)
            .push(key);
        let user_index_us = user_index_start.elapsed().as_micros();

        let primary_insert_start = Instant::now();
        self.trades.insert(key, trade);
        let primary_insert_us = primary_insert_start.elapsed().as_micros();

        let duration_us = start.elapsed().as_micros();
        debug!(
            buy_order = buy_order_id,
            sell_order = sell_order_id,
            pool_id = ?pool_id,
            taker = ?taker_address,
            price = ?execution_price,
            total_trades = self.trades.len(),
            dup_check_us = dup_check_us,
            pool_index_us = pool_index_us,
            user_index_us = user_index_us,
            primary_insert_us = primary_insert_us,
            total_us = duration_us,
            "Trade stored in memory"
        );
        true
    }

    /// Get trade by key
    pub fn get(&self, key: &TradeKey) -> Option<Trade> {
        self.trades.get(key).map(|t| t.clone())
    }

    /// Get recent trades for a pool
    pub fn get_pool_trades(&self, pool_id: &FixedBytes<32>, limit: usize) -> Vec<Trade> {
        self.pool_trades
            .get(pool_id)
            .map(|keys| {
                keys.iter()
                    .rev()
                    .take(limit)
                    .filter_map(|key| self.trades.get(key).map(|t| t.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get recent trades for a user
    pub fn get_user_trades(&self, user: &Address, limit: usize) -> Vec<Trade> {
        self.user_trades
            .get(user)
            .map(|keys| {
                keys.iter()
                    .rev()
                    .take(limit)
                    .filter_map(|key| self.trades.get(key).map(|t| t.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get total trade count
    pub fn count(&self) -> usize {
        self.trades.len()
    }

    /// Get trade count for a pool
    pub fn pool_count(&self, pool_id: &FixedBytes<32>) -> usize {
        self.pool_trades
            .get(pool_id)
            .map(|keys| keys.len())
            .unwrap_or(0)
    }
}

impl Default for TradeStore {
    fn default() -> Self {
        Self::new()
    }
}
