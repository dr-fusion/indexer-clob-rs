use alloy::rpc::types::Filter;
use alloy_sol_types::SolEvent;
use indexer_core::events::{
    Deposit, Lock, OrderCancelled, OrderMatched, OrderPlaced, PoolCreated, TransferFrom,
    TransferLockedFrom, Unlock, UpdateOrder, Withdrawal,
};
use indexer_core::{IndexerConfig, IndexerError, Result};
use indexer_processor::EventProcessor;
use indexer_store::SyncMode;
use std::sync::Arc;
use tracing::info;

use crate::adaptive_batch::AdaptiveBatchConfig;
use crate::provider::ProviderManager;

/// Gap detector and filler for handling missed blocks
pub struct GapDetector {
    config: IndexerConfig,
    provider: Arc<ProviderManager>,
    processor: Arc<EventProcessor>,
}

impl GapDetector {
    pub fn new(
        config: IndexerConfig,
        provider: Arc<ProviderManager>,
        processor: Arc<EventProcessor>,
    ) -> Self {
        Self {
            config,
            provider,
            processor,
        }
    }

    /// Fill a gap starting from the specified block
    pub async fn fill_gap(&self, start_block: u64) -> Result<()> {
        let current_block = self
            .provider
            .http()
            .get_block_number()
            .await
            .map_err(|e| IndexerError::Rpc(e.to_string()))?;

        let last_synced = {
            let state = self.processor.store().sync_state.read().await;
            state.last_synced_block
        };

        let gap_end = last_synced.max(current_block);

        info!(
            from = start_block,
            to = gap_end,
            "Filling gap"
        );

        // Update sync state to gap filling mode
        {
            let mut state = self.processor.store().sync_state.write().await;
            state.mode = SyncMode::GapFilling;
        }

        // Process the gap in batches (use initial batch size from adaptive config)
        let batch_size = AdaptiveBatchConfig::from_env().initial_size;
        let mut from = start_block;

        while from <= gap_end {
            let to = (from + batch_size - 1).min(gap_end);

            self.process_gap_batch(from, to).await?;

            from = to + 1;
        }

        // Mark gap as filled and return to realtime mode
        {
            let mut state = self.processor.store().sync_state.write().await;
            state.fill_gap(start_block);
            state.mode = SyncMode::Realtime;
        }

        info!(block = start_block, "Gap filled");
        Ok(())
    }

    async fn process_gap_batch(&self, from_block: u64, to_block: u64) -> Result<()> {
        // Get all orderbook addresses
        let orderbook_addresses = self.processor.store().pools.get_all_orderbook_addresses();

        // Fetch PoolManager events
        let pool_filter = Filter::new()
            .address(self.config.pool_manager)
            .event_signature(PoolCreated::SIGNATURE_HASH)
            .from_block(from_block)
            .to_block(to_block);

        let pool_logs = self
            .provider
            .http()
            .get_logs(&pool_filter)
            .await
            .map_err(|e| IndexerError::Rpc(e.to_string()))?;

        for log in pool_logs {
            self.processor.process_log(log).await?;
        }

        // Fetch OrderBook events
        if !orderbook_addresses.is_empty() {
            let orderbook_sigs = vec![
                OrderPlaced::SIGNATURE_HASH,
                OrderMatched::SIGNATURE_HASH,
                UpdateOrder::SIGNATURE_HASH,
                OrderCancelled::SIGNATURE_HASH,
            ];

            let orderbook_filter = Filter::new()
                .address(orderbook_addresses)
                .events(orderbook_sigs)
                .from_block(from_block)
                .to_block(to_block);

            let logs = self
                .provider
                .http()
                .get_logs(&orderbook_filter)
                .await
                .map_err(|e| IndexerError::Rpc(e.to_string()))?;

            for log in logs {
                self.processor.process_log(log).await?;
            }
        }

        // Fetch BalanceManager events
        let balance_sigs = vec![
            Deposit::SIGNATURE_HASH,
            Withdrawal::SIGNATURE_HASH,
            Lock::SIGNATURE_HASH,
            Unlock::SIGNATURE_HASH,
            TransferFrom::SIGNATURE_HASH,
            TransferLockedFrom::SIGNATURE_HASH,
        ];

        let balance_filter = Filter::new()
            .address(self.config.balance_manager)
            .events(balance_sigs)
            .from_block(from_block)
            .to_block(to_block);

        let logs = self
            .provider
            .http()
            .get_logs(&balance_filter)
            .await
            .map_err(|e| IndexerError::Rpc(e.to_string()))?;

        for log in logs {
            self.processor.process_log(log).await?;
        }

        Ok(())
    }

    /// Detect if there's a gap between expected and actual block
    #[allow(dead_code)]
    pub fn detect_gap(&self, expected_block: u64, actual_block: u64) -> bool {
        actual_block > expected_block + 1
    }
}
