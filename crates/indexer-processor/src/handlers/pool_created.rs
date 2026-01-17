use alloy::rpc::types::Log;
use alloy_sol_types::SolEvent;
use indexer_core::events::PoolCreated;
use indexer_core::types::Pool;
use indexer_core::{IndexerError, Result};
use indexer_store::IndexerStore;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info};

pub struct PoolCreatedHandler {
    store: Arc<IndexerStore>,
}

impl PoolCreatedHandler {
    pub fn new(store: Arc<IndexerStore>) -> Self {
        Self { store }
    }

    pub async fn handle(&self, log: &Log) -> Result<()> {
        let start = Instant::now();

        let decode_start = Instant::now();
        let event = PoolCreated::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;
        let decode_duration_us = decode_start.elapsed().as_micros();

        let pool = Pool {
            pool_id: event.poolId,
            order_book_address: event.orderBook,
            base_currency: event.baseCurrency,
            quote_currency: event.quoteCurrency,
            created_at_block: log.block_number.unwrap_or_default(),
            created_at_tx: log.transaction_hash.unwrap_or_default(),
        };

        info!(
            pool_id = ?pool.pool_id,
            orderbook = ?pool.order_book_address,
            base = ?pool.base_currency,
            quote = ?pool.quote_currency,
            block = log.block_number.unwrap_or_default(),
            "New pool discovered"
        );

        let store_start = Instant::now();
        self.store.pools.insert(pool);
        let store_duration_us = store_start.elapsed().as_micros();

        // Update stats
        let stats_start = Instant::now();
        {
            let mut state = self.store.sync_state.write().await;
            state.record_pool();
            state.record_event();
        }
        let stats_duration_us = stats_start.elapsed().as_micros();

        let total_duration_us = start.elapsed().as_micros();
        debug!(
            decode_us = decode_duration_us,
            store_us = store_duration_us,
            stats_us = stats_duration_us,
            total_us = total_duration_us,
            "PoolCreated handler timing"
        );

        Ok(())
    }
}
