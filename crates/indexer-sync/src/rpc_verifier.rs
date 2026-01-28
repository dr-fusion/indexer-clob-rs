//! RPC Verifier - Periodic verification of WebSocket events against RPC
//!
//! Runs in background at configurable intervals to catch any events that
//! WebSocket might have missed. Fetches events from two RPCs and compares
//! with the WebSocket event buffer.

use alloy::providers::ProviderBuilder;
use alloy::rpc::types::{Filter, Log};
use alloy_sol_types::SolEvent;
use indexer_core::events::{
    Deposit, Lock, OrderCancelled, OrderMatched, OrderPlaced, PoolCreated, TransferFrom,
    TransferLockedFrom, Unlock, UpdateOrder, Withdrawal,
};
use indexer_core::{IndexerConfig, IndexerError, Result};
use indexer_processor::EventProcessor;
use indexer_store::{EventContentId, IndexerStore};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, warn};

use crate::adaptive_batch::{AdaptiveBatchConfig, AdaptiveBatchController};
use crate::provider::{BoxedProvider, ProviderManager};

/// Statistics from a verification cycle
#[derive(Debug, Clone, Default)]
pub struct VerificationStats {
    pub from_block: u64,
    pub to_block: u64,
    pub blocks_verified: u64,
    pub ws_events_count: usize,
    pub rpc1_events_count: usize,
    pub rpc2_events_count: usize,
    pub union_events_count: usize,
    pub missing_events_processed: usize,
    pub duration_ms: u64,
    pub buffer_cleared: bool,
}

/// Periodic RPC verification task that runs alongside WebSocket sync
/// Compares WebSocket-received events against RPC eth_getLogs to find missing events
pub struct RpcVerifier {
    config: IndexerConfig,
    primary_provider: Arc<ProviderManager>,
    processor: Arc<EventProcessor>,
    store: Arc<IndexerStore>,
    shutdown: Arc<AtomicBool>,
    batch_controller: AdaptiveBatchController,
}

impl RpcVerifier {
    pub fn new(
        config: IndexerConfig,
        primary_provider: Arc<ProviderManager>,
        processor: Arc<EventProcessor>,
        store: Arc<IndexerStore>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        let batch_config = AdaptiveBatchConfig::from_env();
        Self {
            config,
            primary_provider,
            processor,
            store,
            shutdown,
            batch_controller: AdaptiveBatchController::new(batch_config),
        }
    }

    /// Run the verification loop indefinitely
    pub async fn run(&self) -> Result<()> {
        if !self.config.verification.enabled {
            info!("RPC verification disabled via config");
            return Ok(());
        }

        let interval_secs = self.config.verification.interval_secs;
        let mut ticker = interval(Duration::from_secs(interval_secs));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Create secondary provider if configured
        let secondary_provider: Option<BoxedProvider> =
            if let Some(ref url) = self.config.verification.rpc_url {
                let parsed_url: reqwest::Url = url.parse().map_err(|e| {
                    IndexerError::Rpc(format!("Invalid verification RPC URL: {}", e))
                })?;
                let provider = ProviderBuilder::new().connect_http(parsed_url);
                info!(url = %url, "Secondary RPC provider created for verification");
                Some(Arc::new(provider))
            } else {
                warn!("No VERIFICATION_RPC_URL set - verification will only use primary RPC");
                None
            };

        info!(
            interval_secs = interval_secs,
            has_secondary_rpc = secondary_provider.is_some(),
            "Starting RPC verification task"
        );

        loop {
            ticker.tick().await;

            // Check for shutdown
            if self.shutdown.load(Ordering::Relaxed) {
                info!("RPC verifier shutting down");
                break;
            }

            match self
                .run_verification_cycle(secondary_provider.as_ref())
                .await
            {
                Ok(stats) => {
                    if stats.blocks_verified > 0 {
                        info!(
                            from_block = stats.from_block,
                            to_block = stats.to_block,
                            blocks_verified = stats.blocks_verified,
                            ws_events = stats.ws_events_count,
                            rpc1_events = stats.rpc1_events_count,
                            rpc2_events = stats.rpc2_events_count,
                            union_events = stats.union_events_count,
                            missing_events = stats.missing_events_processed,
                            duration_ms = stats.duration_ms,
                            buffer_cleared = stats.buffer_cleared,
                            "Verification cycle complete"
                        );
                    } else {
                        debug!("Verification skipped - no blocks to verify");
                    }
                }
                Err(e) => {
                    error!(error = %e, "Verification cycle failed");
                    // Continue running - don't crash on individual failures
                }
            }
        }

        Ok(())
    }

    /// Execute a single verification cycle
    async fn run_verification_cycle(
        &self,
        secondary_provider: Option<&BoxedProvider>,
    ) -> Result<VerificationStats> {
        let cycle_start = Instant::now();

        // Get current block range to verify
        let start_block = self.store.ws_event_buffer.get_verification_start();
        if start_block == 0 {
            // WebSocket hasn't started yet
            return Ok(VerificationStats::default());
        }

        // Get latest block from primary RPC
        let latest_block = self
            .primary_provider
            .http()
            .get_block_number()
            .await
            .map_err(|e| IndexerError::Rpc(e.to_string()))?;

        // Calculate verification range
        let blocks_available = latest_block.saturating_sub(start_block);
        if blocks_available == 0 {
            return Ok(VerificationStats::default());
        }

        // Use batch controller for adaptive sizing
        let batch_size = self.batch_controller.get_size();
        let to_block = start_block + batch_size.min(blocks_available) - 1;

        debug!(
            start_block = start_block,
            to_block = to_block,
            latest_block = latest_block,
            batch_size = batch_size,
            "Starting verification for block range"
        );

        // Get WebSocket events from buffer
        let ws_events = self
            .store
            .ws_event_buffer
            .get_events_in_range(start_block, to_block);
        let ws_events_count = ws_events.len();

        // Fetch events from primary RPC
        let rpc1_result = self
            .fetch_all_events(self.primary_provider.http(), start_block, to_block)
            .await;

        // Fetch events from secondary RPC (if available)
        let rpc2_result = if let Some(provider) = secondary_provider {
            Some(self.fetch_all_events(provider, start_block, to_block).await)
        } else {
            None
        };

        // Check if both fetches succeeded
        let (rpc1_logs, rpc1_events_count) = match rpc1_result {
            Ok(logs) => {
                self.batch_controller.report_success();
                let count = logs.len();
                (logs, count)
            }
            Err(e) => {
                self.batch_controller.report_error();
                warn!(error = %e, "Primary RPC fetch failed - skipping verification cycle");
                return Err(e);
            }
        };

        let (rpc2_logs, rpc2_events_count) = match rpc2_result {
            Some(Ok(logs)) => {
                let count = logs.len();
                (Some(logs), count)
            }
            Some(Err(e)) => {
                warn!(error = %e, "Secondary RPC fetch failed - will only use primary RPC");
                (None, 0)
            }
            None => (None, 0),
        };

        // Union of both RPC results (dedupe by EventContentId - content-based comparison)
        let mut all_rpc_events: HashSet<EventContentId> = HashSet::new();
        let mut rpc_logs_map: std::collections::HashMap<EventContentId, Log> =
            std::collections::HashMap::new();

        for log in &rpc1_logs {
            if let Some(content_id) = EventContentId::from_log(log) {
                all_rpc_events.insert(content_id.clone());
                rpc_logs_map.insert(content_id, log.clone());
            }
        }

        if let Some(ref logs) = rpc2_logs {
            for log in logs {
                if let Some(content_id) = EventContentId::from_log(log) {
                    all_rpc_events.insert(content_id.clone());
                    // Only insert if not already present (prefer primary RPC log)
                    rpc_logs_map.entry(content_id).or_insert_with(|| log.clone());
                }
            }
        }

        let union_events_count = all_rpc_events.len();

        // Find events in RPC but not in WebSocket buffer (by content, not log_index)
        let mut missing_count = 0;
        for content_id in &all_rpc_events {
            if !ws_events.contains(content_id) {
                // This event was missed by WebSocket - process it
                if let Some(log) = rpc_logs_map.get(content_id) {
                    debug!(
                        tx_hash = ?log.transaction_hash,
                        log_index = ?log.log_index,
                        block = ?log.block_number,
                        "Found missing event via RPC verification"
                    );

                    if let Err(e) = self.processor.process_log(log.clone()).await {
                        warn!(
                            error = %e,
                            tx_hash = ?log.transaction_hash,
                            "Failed to process missing event"
                        );
                    } else {
                        missing_count += 1;
                    }
                }
            }
        }

        // Update buffer statistics
        self.store.ws_event_buffer.record_missing(missing_count as u64);

        // Determine if we should clear the buffer
        // Only clear if:
        // 1. Primary RPC succeeded (always required)
        // 2. Secondary RPC either succeeded or wasn't configured
        let should_clear = rpc2_logs.is_some() || secondary_provider.is_none();

        if should_clear {
            // Clear verified range from buffer and update start block
            self.store
                .ws_event_buffer
                .clear_range(start_block, to_block);
        } else {
            warn!("Not clearing buffer - secondary RPC failed");
        }

        let duration_ms = cycle_start.elapsed().as_millis() as u64;

        Ok(VerificationStats {
            from_block: start_block,
            to_block,
            blocks_verified: to_block - start_block + 1,
            ws_events_count,
            rpc1_events_count,
            rpc2_events_count,
            union_events_count,
            missing_events_processed: missing_count,
            duration_ms,
            buffer_cleared: should_clear,
        })
    }

    // Note: EventContentId::from_log is used directly for content-based comparison
    // This avoids relying on log_index which may differ between MiniBlocks and finalized blocks

    /// Fetch all relevant events for verification using combined filter
    async fn fetch_all_events(
        &self,
        provider: &BoxedProvider,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>> {
        // Get all orderbook addresses
        let orderbook_addresses = self.store.pools.get_all_orderbook_addresses();

        // Combine all addresses
        let mut all_addresses = vec![self.config.pool_manager, self.config.balance_manager];
        all_addresses.extend(orderbook_addresses);

        // All 11 event signatures
        let all_topics = vec![
            PoolCreated::SIGNATURE_HASH,
            OrderPlaced::SIGNATURE_HASH,
            OrderMatched::SIGNATURE_HASH,
            UpdateOrder::SIGNATURE_HASH,
            OrderCancelled::SIGNATURE_HASH,
            Deposit::SIGNATURE_HASH,
            Withdrawal::SIGNATURE_HASH,
            Lock::SIGNATURE_HASH,
            Unlock::SIGNATURE_HASH,
            TransferFrom::SIGNATURE_HASH,
            TransferLockedFrom::SIGNATURE_HASH,
        ];

        let filter = Filter::new()
            .address(all_addresses)
            .event_signature(all_topics)
            .from_block(from_block)
            .to_block(to_block);

        let mut logs = provider
            .get_logs(&filter)
            .await
            .map_err(|e| IndexerError::Rpc(format!("{:?}", e)))?;

        // Sort by (block_number, log_index)
        logs.sort_by(|a, b| {
            let block_a = a.block_number.unwrap_or(0);
            let block_b = b.block_number.unwrap_or(0);
            let idx_a = a.log_index.unwrap_or(0);
            let idx_b = b.log_index.unwrap_or(0);
            (block_a, idx_a).cmp(&(block_b, idx_b))
        });

        Ok(logs)
    }
}
