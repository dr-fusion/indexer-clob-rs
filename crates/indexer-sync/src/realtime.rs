use alloy::rpc::types::Filter;
use alloy_sol_types::SolEvent;
use indexer_core::events::{
    Deposit, Lock, OrderCancelled, OrderMatched, OrderPlaced, PoolCreated, TransferFrom,
    TransferLockedFrom, Unlock, UpdateOrder, Withdrawal,
};
use indexer_core::{IndexerConfig, IndexerError, Result};
use indexer_processor::EventProcessor;
use serde::Deserialize;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace};

use crate::provider::ProviderManager;

/// MegaETH miniblock structure (for future WebSocket subscription)
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MiniBlock {
    pub payload_id: String,
    #[serde(deserialize_with = "deserialize_u64_from_hex")]
    pub block_number: u64,
    pub index: u32,
    pub tx_offset: u32,
    pub log_offset: u32,
    pub gas_offset: u64,
    #[serde(deserialize_with = "deserialize_u64_from_hex")]
    pub timestamp: u64,
    pub gas_used: u64,
    #[serde(default)]
    pub receipts: Vec<MiniBlockReceipt>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MiniBlockReceipt {
    #[serde(default)]
    pub logs: Vec<MiniBlockLog>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MiniBlockLog {
    pub address: alloy::primitives::Address,
    #[serde(default)]
    pub topics: Vec<alloy::primitives::B256>,
    pub data: alloy::primitives::Bytes,
    #[serde(deserialize_with = "deserialize_u64_from_hex")]
    pub block_number: u64,
    pub transaction_hash: alloy::primitives::B256,
    #[serde(deserialize_with = "deserialize_u64_from_hex")]
    pub log_index: u64,
}

fn deserialize_u64_from_hex<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    u64::from_str_radix(s.trim_start_matches("0x"), 16).map_err(serde::de::Error::custom)
}

impl From<MiniBlockLog> for alloy::primitives::Log {
    fn from(log: MiniBlockLog) -> Self {
        alloy::primitives::Log {
            address: log.address,
            data: alloy::primitives::LogData::new(log.topics, log.data).unwrap_or_default(),
        }
    }
}

/// Real-time syncer using MegaETH miniBlocks subscription
pub struct RealtimeSyncer {
    config: IndexerConfig,
    provider: Arc<ProviderManager>,
    processor: Arc<EventProcessor>,
    #[allow(dead_code)]
    last_processed_block: AtomicU64,
    #[allow(dead_code)]
    last_miniblock_index: AtomicU32,
}

impl RealtimeSyncer {
    pub fn new(
        config: IndexerConfig,
        provider: Arc<ProviderManager>,
        processor: Arc<EventProcessor>,
    ) -> Self {
        Self {
            config,
            provider,
            processor,
            last_processed_block: AtomicU64::new(0),
            last_miniblock_index: AtomicU32::new(0),
        }
    }

    /// Run the real-time syncer
    /// Returns when disconnected or error occurs
    pub async fn run(&mut self, gap_tx: mpsc::Sender<u64>) -> Result<()> {
        info!("Starting real-time sync");

        // For now, we'll use polling as a fallback since miniBlocks subscription
        // requires custom transport handling in Alloy
        self.run_polling_mode(gap_tx).await
    }

    /// Polling mode fallback - poll for new blocks periodically
    async fn run_polling_mode(&mut self, _gap_tx: mpsc::Sender<u64>) -> Result<()> {
        info!("Running in polling mode");

        let poll_interval = Duration::from_millis(100); // 100ms for MegaETH

        loop {
            match self.poll_new_blocks().await {
                Ok(processed) => {
                    if processed > 0 {
                        trace!(blocks = processed, "Processed new blocks");
                    }
                }
                Err(e) => {
                    error!(error = %e, "Error polling blocks");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn poll_new_blocks(&mut self) -> Result<usize> {
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

        if current_block <= last_synced {
            return Ok(0);
        }

        let from_block = last_synced + 1;
        let to_block = current_block.min(from_block + 10); // Process max 10 blocks at a time

        debug!(from = from_block, to = to_block, "Polling new blocks");

        // Fetch PoolManager events
        self.fetch_and_process_events(from_block, to_block).await?;

        // Update sync state
        {
            let mut state = self.processor.store().sync_state.write().await;
            state.set_last_synced_block(to_block);
        }

        Ok((to_block - from_block + 1) as usize)
    }

    async fn fetch_and_process_events(&self, from_block: u64, to_block: u64) -> Result<()> {
        // Step 1: Fetch and process PoolManager events FIRST
        // This ensures any new pools are discovered before we fetch orderbook events
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

        let new_pools = pool_logs.len();
        for log in pool_logs {
            self.processor.process_log(log).await?;
        }

        if new_pools > 0 {
            info!(
                new_pools = new_pools,
                total_pools = self.processor.store().pools.count(),
                "New pool(s) discovered in real-time"
            );
        }

        // Step 2: Get ALL orderbook addresses (now includes any newly discovered pools)
        let orderbook_addresses = self.processor.store().pools.get_all_orderbook_addresses();
        if !orderbook_addresses.is_empty() {
            let orderbook_sigs = vec![
                OrderPlaced::SIGNATURE_HASH,
                OrderMatched::SIGNATURE_HASH,
                UpdateOrder::SIGNATURE_HASH,
                OrderCancelled::SIGNATURE_HASH,
            ];

            let orderbook_filter = Filter::new()
                .address(orderbook_addresses)
                .event_signature(orderbook_sigs)
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

        // Step 3: Fetch BalanceManager events for all users
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
            .event_signature(balance_sigs)
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
}
