use alloy_primitives::{Address, FixedBytes};
use dashmap::DashMap;
use indexer_core::types::Pool;
use std::time::Instant;
use tracing::debug;

/// Thread-safe store for trading pools
#[derive(Debug)]
pub struct PoolStore {
    /// Pool ID -> Pool
    pools: DashMap<FixedBytes<32>, Pool>,
    /// OrderBook address -> Pool ID (reverse lookup)
    orderbook_index: DashMap<Address, FixedBytes<32>>,
}

impl PoolStore {
    pub fn new() -> Self {
        Self {
            pools: DashMap::new(),
            orderbook_index: DashMap::new(),
        }
    }

    /// Insert a new pool
    pub fn insert(&self, pool: Pool) {
        let start = Instant::now();
        let pool_id = pool.pool_id;
        let orderbook = pool.order_book_address;

        self.orderbook_index
            .insert(pool.order_book_address, pool.pool_id);
        self.pools.insert(pool.pool_id, pool);

        let duration_us = start.elapsed().as_micros();
        debug!(
            pool_id = ?pool_id,
            orderbook = ?orderbook,
            total_pools = self.pools.len(),
            insert_us = duration_us,
            "Pool inserted into memory store"
        );
    }

    /// Get pool by ID
    pub fn get_by_id(&self, pool_id: &FixedBytes<32>) -> Option<Pool> {
        self.pools.get(pool_id).map(|p| p.clone())
    }

    /// Get pool ID by OrderBook address
    pub fn get_pool_id_by_orderbook(&self, orderbook: &Address) -> Option<FixedBytes<32>> {
        self.orderbook_index.get(orderbook).map(|id| *id)
    }

    /// Get pool by OrderBook address
    pub fn get_by_orderbook(&self, orderbook: &Address) -> Option<Pool> {
        self.get_pool_id_by_orderbook(orderbook)
            .and_then(|id| self.get_by_id(&id))
    }

    /// Get all OrderBook addresses (for log filtering)
    pub fn get_all_orderbook_addresses(&self) -> Vec<Address> {
        self.orderbook_index.iter().map(|e| *e.key()).collect()
    }

    /// Get total number of pools
    pub fn count(&self) -> usize {
        self.pools.len()
    }

    /// Check if an OrderBook address is known
    pub fn is_known_orderbook(&self, address: &Address) -> bool {
        self.orderbook_index.contains_key(address)
    }

    /// Get all pools
    pub fn get_all(&self) -> Vec<Pool> {
        self.pools.iter().map(|e| e.value().clone()).collect()
    }

    /// Bulk insert pools (for restoring state from database)
    pub fn bulk_insert(&self, pools: impl IntoIterator<Item = Pool>) -> usize {
        let start = Instant::now();
        let mut count = 0;
        for pool in pools {
            self.orderbook_index
                .insert(pool.order_book_address, pool.pool_id);
            self.pools.insert(pool.pool_id, pool);
            count += 1;
        }
        let duration_us = start.elapsed().as_micros();
        debug!(
            inserted = count,
            total_pools = self.pools.len(),
            bulk_insert_us = duration_us,
            "Bulk inserted pools into memory store"
        );
        count
    }
}

impl Default for PoolStore {
    fn default() -> Self {
        Self::new()
    }
}
