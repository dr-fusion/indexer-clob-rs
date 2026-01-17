use alloy::rpc::types::Log;
use alloy_primitives::U128;
use alloy_sol_types::SolEvent;
use indexer_core::events::OrderMatched;
use indexer_core::types::{OrderSide, Trade, TradeKey};
use indexer_core::{IndexerError, Result};
use indexer_store::IndexerStore;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

pub struct OrderMatchedHandler {
    store: Arc<IndexerStore>,
}

impl OrderMatchedHandler {
    pub fn new(store: Arc<IndexerStore>) -> Self {
        Self { store }
    }

    pub async fn handle(&self, log: &Log) -> Result<()> {
        let start = Instant::now();

        let decode_start = Instant::now();
        let event = OrderMatched::decode_log(&log.inner)
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

        let taker_side = OrderSide::from(event.side as u8);
        let tx_hash = log.transaction_hash.unwrap_or_default();
        let log_index = log.log_index.unwrap_or_default() as u64;

        let trade = Trade {
            key: TradeKey { tx_hash, log_index },
            pool_id,
            buy_order_id: event.buyOrderId.to::<u64>(),
            sell_order_id: event.sellOrderId.to::<u64>(),
            taker_address: event.user,
            taker_side,
            execution_price: U128::from(event.executionPrice),
            taker_limit_price: U128::from(event.takerLimitPrice),
            executed_quantity: U128::from(event.executedQuantity),
            timestamp: event.timestamp.to::<u64>(),
            block_number: log.block_number.unwrap_or_default(),
        };

        debug!(
            buy_order = trade.buy_order_id,
            sell_order = trade.sell_order_id,
            pool_id = ?pool_id,
            price = ?trade.execution_price,
            quantity = ?trade.executed_quantity,
            taker = ?trade.taker_address,
            block = log.block_number.unwrap_or_default(),
            "Order matched"
        );

        // Insert is idempotent - returns false if trade already exists
        let store_start = Instant::now();
        let inserted = self.store.trades.insert(trade);
        let store_duration_us = store_start.elapsed().as_micros();

        // Only update stats if this is a new trade (not a duplicate)
        let stats_start = Instant::now();
        if inserted {
            let mut state = self.store.sync_state.write().await;
            state.record_trade();
            state.record_event();
        }
        let stats_duration_us = stats_start.elapsed().as_micros();

        let total_duration_us = start.elapsed().as_micros();
        debug!(
            buy_order = event.buyOrderId.to::<u64>(),
            sell_order = event.sellOrderId.to::<u64>(),
            inserted = inserted,
            decode_us = decode_duration_us,
            lookup_us = lookup_duration_us,
            store_us = store_duration_us,
            stats_us = stats_duration_us,
            total_us = total_duration_us,
            "OrderMatched handler timing"
        );

        Ok(())
    }
}
