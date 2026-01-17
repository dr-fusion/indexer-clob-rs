use alloy::rpc::types::Log;
use alloy_primitives::U128;
use alloy_sol_types::SolEvent;
use indexer_core::events::OrderPlaced;
use indexer_core::types::{Order, OrderSide, OrderStatus};
use indexer_core::{IndexerError, Result};
use indexer_store::IndexerStore;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

pub struct OrderPlacedHandler {
    store: Arc<IndexerStore>,
}

impl OrderPlacedHandler {
    pub fn new(store: Arc<IndexerStore>) -> Self {
        Self { store }
    }

    pub async fn handle(&self, log: &Log) -> Result<()> {
        let start = Instant::now();

        let decode_start = Instant::now();
        let event = OrderPlaced::decode_log(&log.inner)
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

        let side = OrderSide::from(event.side as u8);
        let status = OrderStatus::from(event.status as u8);

        let order = Order {
            order_id: event.orderId.to::<u64>(),
            pool_id,
            user: event.user,
            side,
            price: U128::from(event.price),
            original_quantity: U128::from(event.quantity),
            filled_quantity: U128::ZERO,
            remaining_quantity: U128::from(event.quantity),
            expiry: event.expiry.to::<u64>(),
            is_market_order: event.isMarketOrder,
            status,
            created_at_block: log.block_number.unwrap_or_default(),
            created_at_tx: log.transaction_hash.unwrap_or_default(),
            last_updated_block: log.block_number.unwrap_or_default(),
        };

        debug!(
            order_id = order.order_id,
            pool_id = ?pool_id,
            user = ?order.user,
            side = ?order.side,
            price = ?order.price,
            quantity = ?order.original_quantity,
            block = log.block_number.unwrap_or_default(),
            "Order placed"
        );

        let store_start = Instant::now();
        self.store.orders.insert(order);
        let store_duration_us = store_start.elapsed().as_micros();

        // Update stats
        let stats_start = Instant::now();
        {
            let mut state = self.store.sync_state.write().await;
            state.record_order();
            state.record_event();
        }
        let stats_duration_us = stats_start.elapsed().as_micros();

        let total_duration_us = start.elapsed().as_micros();
        debug!(
            order_id = event.orderId.to::<u64>(),
            decode_us = decode_duration_us,
            lookup_us = lookup_duration_us,
            store_us = store_duration_us,
            stats_us = stats_duration_us,
            total_us = total_duration_us,
            "OrderPlaced handler timing"
        );

        Ok(())
    }
}
