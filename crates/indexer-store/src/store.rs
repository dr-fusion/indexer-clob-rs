use std::sync::Arc;
use tokio::sync::RwLock;

use crate::balances::BalanceStore;
use crate::orders::OrderStore;
use crate::pools::PoolStore;
use crate::sync_state::SyncState;
use crate::trades::TradeStore;

/// Thread-safe in-memory store for the indexer
#[derive(Debug)]
pub struct IndexerStore {
    pub pools: Arc<PoolStore>,
    pub orders: Arc<OrderStore>,
    pub balances: Arc<BalanceStore>,
    pub trades: Arc<TradeStore>,
    pub sync_state: Arc<RwLock<SyncState>>,
}

impl IndexerStore {
    pub fn new() -> Self {
        Self {
            pools: Arc::new(PoolStore::new()),
            orders: Arc::new(OrderStore::new()),
            balances: Arc::new(BalanceStore::new()),
            trades: Arc::new(TradeStore::new()),
            sync_state: Arc::new(RwLock::new(SyncState::default())),
        }
    }
}

impl Default for IndexerStore {
    fn default() -> Self {
        Self::new()
    }
}
