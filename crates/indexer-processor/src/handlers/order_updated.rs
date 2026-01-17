use alloy::rpc::types::Log;
use alloy_primitives::U128;
use alloy_sol_types::SolEvent;
use indexer_core::events::UpdateOrder;
use indexer_core::types::{OrderKey, OrderStatus};
use indexer_core::{IndexerError, Result};
use indexer_store::IndexerStore;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

pub struct OrderUpdatedHandler {
    store: Arc<IndexerStore>,
}

impl OrderUpdatedHandler {
    pub fn new(store: Arc<IndexerStore>) -> Self {
        Self { store }
    }

    pub async fn handle(&self, log: &Log) -> Result<()> {
        let start = Instant::now();

        let decode_start = Instant::now();
        let event = UpdateOrder::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;
        let decode_duration_us = decode_start.elapsed().as_micros();

        // Look up pool_id from orderbook address
        let lookup_start = Instant::now();
        let orderbook_address = log.address();
        let pool_id = self
            .store
            .pools
            .get_pool_id_by_orderbook(&orderbook_address)
            .ok_or_else(|| IndexerError::UnknownOrderBook(orderbook_address))?;
        let lookup_duration_us = lookup_start.elapsed().as_micros();

        let order_id = event.orderId.to::<u64>();
        let key = OrderKey { pool_id, order_id };

        let new_filled = U128::from(event.filled);
        let new_status = OrderStatus::from(event.status as u8);
        let block_number = log.block_number.unwrap_or_default();

        // Update order in store
        let store_start = Instant::now();
        self.store.orders.update(&key, |order| {
            order.filled_quantity = new_filled;
            order.remaining_quantity = order.original_quantity.saturating_sub(new_filled);
            order.status = new_status;
            order.last_updated_block = block_number;
        });
        let store_duration_us = store_start.elapsed().as_micros();

        debug!(
            order_id = order_id,
            pool_id = ?pool_id,
            filled = ?new_filled,
            status = ?new_status,
            block = block_number,
            "Order updated"
        );

        // Update stats
        let stats_start = Instant::now();
        {
            let mut state = self.store.sync_state.write().await;
            state.record_event();
        }
        let stats_duration_us = stats_start.elapsed().as_micros();

        let total_duration_us = start.elapsed().as_micros();
        debug!(
            order_id = order_id,
            decode_us = decode_duration_us,
            lookup_us = lookup_duration_us,
            store_us = store_duration_us,
            stats_us = stats_duration_us,
            total_us = total_duration_us,
            "UpdateOrder handler timing"
        );

        Ok(())
    }
}
