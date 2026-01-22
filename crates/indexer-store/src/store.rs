use std::sync::Arc;
use tokio::sync::RwLock;

use crate::balances::BalanceStore;
use crate::orders::OrderStore;
use crate::pools::PoolStore;
use crate::sync_state::SyncState;
use crate::trades::TradeStore;
use crate::ws_event_buffer::WebSocketEventBuffer;

/// Thread-safe in-memory store for the indexer
#[derive(Debug)]
pub struct IndexerStore {
    pub pools: Arc<PoolStore>,
    pub orders: Arc<OrderStore>,
    pub balances: Arc<BalanceStore>,
    pub trades: Arc<TradeStore>,
    pub sync_state: Arc<RwLock<SyncState>>,
    /// Buffer for WebSocket event verification
    pub ws_event_buffer: Arc<WebSocketEventBuffer>,
}

impl IndexerStore {
    pub fn new() -> Self {
        Self {
            pools: Arc::new(PoolStore::new()),
            orders: Arc::new(OrderStore::new()),
            balances: Arc::new(BalanceStore::new()),
            trades: Arc::new(TradeStore::new()),
            sync_state: Arc::new(RwLock::new(SyncState::default())),
            ws_event_buffer: Arc::new(WebSocketEventBuffer::new()),
        }
    }
}

impl Default for IndexerStore {
    fn default() -> Self {
        Self::new()
    }
}
