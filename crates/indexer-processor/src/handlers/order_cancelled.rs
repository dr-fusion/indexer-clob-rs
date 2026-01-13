use alloy::rpc::types::Log;
use alloy_sol_types::SolEvent;
use indexer_core::events::OrderCancelled;
use indexer_core::types::{OrderKey, OrderStatus};
use indexer_core::{IndexerError, Result};
use indexer_store::IndexerStore;
use std::sync::Arc;
use tracing::debug;

pub struct OrderCancelledHandler {
    store: Arc<IndexerStore>,
}

impl OrderCancelledHandler {
    pub fn new(store: Arc<IndexerStore>) -> Self {
        Self { store }
    }

    pub async fn handle(&self, log: &Log) -> Result<()> {
        let event = OrderCancelled::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;

        // Look up pool_id from orderbook address
        let orderbook_address = log.address();
        let pool_id = self
            .store
            .pools
            .get_pool_id_by_orderbook(&orderbook_address)
            .ok_or_else(|| IndexerError::UnknownOrderBook(orderbook_address))?;

        let order_id = event.orderId.to::<u64>();
        let key = OrderKey { pool_id, order_id };
        let block_number = log.block_number.unwrap_or_default();

        // Update order status to cancelled
        self.store.orders.update(&key, |order| {
            order.status = OrderStatus::Cancelled;
            order.last_updated_block = block_number;
        });

        debug!(
            order_id = order_id,
            user = ?event.user,
            "Order cancelled"
        );

        // Update stats
        {
            let mut state = self.store.sync_state.write().await;
            state.record_event();
        }

        Ok(())
    }
}
