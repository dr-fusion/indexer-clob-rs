use alloy::rpc::types::{Filter, Log};
use alloy_sol_types::SolEvent;
use futures::{stream, StreamExt};
use indexer_core::events::{
    Deposit, Lock, OrderCancelled, OrderMatched, OrderPlaced, PoolCreated, TransferFrom,
    TransferLockedFrom, Unlock, UpdateOrder, Withdrawal,
};
use indexer_core::{IndexerConfig, IndexerError, Result};
use indexer_processor::EventProcessor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::adaptive_batch::{
    AdaptiveBatchConfig, AdaptiveBatchController, AdaptiveConcurrencyConfig,
    AdaptiveConcurrencyController, BlockRangeTracker,
};
use crate::provider::ProviderManager;

/// Historical sync implementation using eth_getLogs with adaptive batch sizing and concurrency
pub struct HistoricalSyncer {
    config: IndexerConfig,
    provider: Arc<ProviderManager>,
    processor: Arc<EventProcessor>,
    batch_controller: Arc<AdaptiveBatchController>,
    concurrency_controller: Arc<AdaptiveConcurrencyController>,
    /// Shutdown flag - checked during sync to allow graceful termination
    shutdown: Arc<AtomicBool>,
}

impl HistoricalSyncer {
    pub fn new(
        config: IndexerConfig,
        provider: Arc<ProviderManager>,
        processor: Arc<EventProcessor>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        let batch_config = AdaptiveBatchConfig::from_env();
        let concurrency_config = AdaptiveConcurrencyConfig::from_env();
        Self {
            config,
            provider,
            processor,
            batch_controller: Arc::new(AdaptiveBatchController::new(batch_config)),
            concurrency_controller: Arc::new(AdaptiveConcurrencyController::new(concurrency_config)),
            shutdown,
        }
    }

    /// Check if shutdown was requested
    fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Sync from start_block to current head
    pub async fn sync_to_head(&self) -> Result<()> {
        // Check shutdown before starting
        if self.is_shutdown() {
            info!("Shutdown requested before historical sync started");
            return Ok(());
        }

        let current_block = self
            .provider
            .http()
            .get_block_number()
            .await
            .map_err(|e| IndexerError::Rpc(e.to_string()))?;

        let start_block = self.config.start_block;
        let total_blocks = current_block.saturating_sub(start_block);

        info!(
            start = start_block,
            end = current_block,
            total = total_blocks,
            "Starting historical sync"
        );

        // Sync all events - fetches PoolCreated, OrderBook, and Balance events together
        // Processes them in order within each batch to ensure pools are discovered
        // before their events are processed
        self.sync_all_events(start_block, current_block).await?;

        // Check if we were interrupted
        if self.is_shutdown() {
            info!("Historical sync interrupted by shutdown");
            return Ok(());
        }

        let pools_count = self.processor.store().pools.count();
        let orders_count = self.processor.store().orders.count();
        let trades_count = self.processor.store().trades.count();

        info!(
            pools = pools_count,
            orders = orders_count,
            trades = trades_count,
            "Historical sync complete"
        );

        Ok(())
    }

    /// Sync all events - processes batches with adaptive sizing and concurrency
    /// Uses AIMD algorithm to dynamically adjust batch size and concurrency based on RPC responses
    async fn sync_all_events(&self, from_block: u64, to_block: u64) -> Result<()> {
        let initial_batch_size = self.batch_controller.get_size();
        let initial_concurrency = self.concurrency_controller.get();
        let range_tracker = Arc::new(BlockRangeTracker::new(to_block));

        info!(
            from = from_block,
            to = to_block,
            initial_batch_size = initial_batch_size,
            initial_concurrency = initial_concurrency,
            "Starting adaptive parallel sync"
        );

        // Generate all batch ranges upfront
        let batch_size = self.batch_controller.get_size();
        let batches: Vec<(u64, u64)> = {
            let mut batches = Vec::new();
            let mut current = from_block;
            while current <= to_block {
                let batch_end = (current + batch_size - 1).min(to_block);
                batches.push((current, batch_end));
                current = batch_end + 1;
            }
            batches
        };

        let total_batches = batches.len();
        info!(total_batches = total_batches, "Generated batch ranges");

        // Process batches in parallel with adaptive concurrency
        // Results are collected and processed to maintain ordering
        let batch_controller = Arc::clone(&self.batch_controller);
        let concurrency_controller = Arc::clone(&self.concurrency_controller);
        let provider = Arc::clone(&self.provider);
        let processor = Arc::clone(&self.processor);
        let config = self.config.clone();
        let range_tracker_clone = Arc::clone(&range_tracker);
        let shutdown = Arc::clone(&self.shutdown);

        // Process batches with bounded concurrency
        let concurrency = self.concurrency_controller.get();
        let results: Vec<std::result::Result<(u64, u64), (u64, u64, IndexerError)>> = stream::iter(batches.into_iter().enumerate())
            .map(|(batch_idx, (from, to))| {
                let provider = Arc::clone(&provider);
                let processor = Arc::clone(&processor);
                let config = config.clone();
                let batch_controller = Arc::clone(&batch_controller);
                let concurrency_controller = Arc::clone(&concurrency_controller);
                let range_tracker = Arc::clone(&range_tracker_clone);
                let shutdown = Arc::clone(&shutdown);

                async move {
                    // Check for shutdown before processing
                    if shutdown.load(Ordering::Relaxed) {
                        return Err((from, to, IndexerError::Sync("Shutdown requested".to_string())));
                    }

                    // Acquire semaphore permit for rate limiting
                    let semaphore = concurrency_controller.semaphore();
                    let _permit = semaphore.acquire().await;

                    // Check again after acquiring permit (might have waited)
                    if shutdown.load(Ordering::Relaxed) {
                        return Err((from, to, IndexerError::Sync("Shutdown requested".to_string())));
                    }

                    match Self::process_event_batch_static(
                        &provider,
                        &processor,
                        &config,
                        from,
                        to,
                        batch_idx,
                        total_batches,
                    )
                    .await
                    {
                        Ok(()) => {
                            batch_controller.report_success();
                            concurrency_controller.report_success();
                            range_tracker.mark_processed(from, to).await;
                            Ok((from, to))
                        }
                        Err(e) if Self::is_retryable_error(&e) => {
                            // Determine which controller to penalize based on error type
                            let err_msg = e.to_string().to_lowercase();
                            if Self::is_too_many_logs_error(&err_msg) {
                                // Too many logs: reduce batch size only
                                batch_controller.report_error();
                            } else if Self::is_rate_limit_error(&err_msg) {
                                // Rate limit: reduce concurrency only
                                concurrency_controller.report_error();
                            } else {
                                // Unknown retryable: reduce both to be safe
                                batch_controller.report_error();
                                concurrency_controller.report_error();
                            }
                            Err((from, to, e))
                        }
                        Err(e) => {
                            Err((from, to, e))
                        }
                    }
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        // Check for shutdown after batch processing
        if self.is_shutdown() {
            info!("Shutdown requested during historical sync");
            return Ok(());
        }

        // Check for failures and retry them
        let mut failed_batches: Vec<(u64, u64)> = Vec::new();
        let mut non_retryable_error: Option<IndexerError> = None;

        for result in results {
            match result {
                Ok(_) => {}
                Err((from, to, e)) => {
                    if Self::is_retryable_error(&e) {
                        warn!(from = from, to = to, error = %e, error_debug = ?e, "Batch failed, will retry");
                        failed_batches.push((from, to));
                    } else {
                        error!(from = from, to = to, error = %e, error_debug = ?e, "Non-retryable error");
                        non_retryable_error = Some(e);
                        break;
                    }
                }
            }
        }

        // Return early if non-retryable error
        if let Some(e) = non_retryable_error {
            return Err(e);
        }

        // Retry failed batches sequentially with smaller batch sizes
        for (from, to) in failed_batches {
            self.fill_gap(from, to).await?;
        }

        // Verify no gaps - safety check
        let gaps = range_tracker.get_gaps(from_block).await;
        if !gaps.is_empty() {
            warn!(gaps = ?gaps, "Found gaps after initial sync, filling...");
            for (gap_from, gap_to) in gaps {
                self.fill_gap(gap_from, gap_to).await?;
            }
        }

        info!(
            batches_processed = total_batches,
            final_batch_size = self.batch_controller.get_size(),
            final_concurrency = self.concurrency_controller.get(),
            "Adaptive parallel sync complete"
        );

        Ok(())
    }

    /// Process a batch with retry logic - splits batch on "too many logs" errors
    async fn process_event_batch_with_retry(
        &self,
        from_block: u64,
        to_block: u64,
        batch_idx: u64,
    ) -> Result<()> {
        let max_attempts = self.config.sync.retry_attempts;
        let mut attempts = 0;
        let mut delay = Duration::from_millis(self.config.sync.retry_delay_ms);

        // Current range to process (may be split on retry)
        let mut current_from = from_block;
        let mut current_to = to_block;

        loop {
            attempts += 1;
            match Self::process_event_batch_static(
                &self.provider,
                &self.processor,
                &self.config,
                current_from,
                current_to,
                batch_idx as usize,
                0, // total_batches not known in adaptive mode
            )
            .await
            {
                Ok(()) => {
                    // If we split the batch, continue with remaining range
                    if current_to < to_block {
                        current_from = current_to + 1;
                        current_to = to_block.min(current_from + self.batch_controller.get_size() - 1);
                        attempts = 0; // Reset attempts for new sub-batch
                        continue;
                    }
                    return Ok(());
                }
                Err(e) if Self::is_retryable_error(&e) && attempts < max_attempts => {
                    // Check if it's a "too many logs" error - split the batch
                    let err_msg = e.to_string().to_lowercase();
                    if err_msg.contains("too many logs") || err_msg.contains("-32005") {
                        // Split the current range in half
                        let range_size = current_to - current_from;
                        if range_size > 10 {
                            let mid = current_from + range_size / 2;
                            warn!(
                                from = current_from,
                                to = current_to,
                                new_to = mid,
                                "Too many logs, splitting batch"
                            );
                            // Brief delay before retry to avoid hammering RPC
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            current_to = mid;
                            attempts = 0; // Reset attempts for smaller batch
                            continue;
                        }
                    }

                    // Check if it's a rate limit error - parse retry time or default to 10 seconds
                    if Self::is_rate_limit_error(&err_msg) {
                        let retry_secs = Self::parse_retry_seconds(&err_msg).unwrap_or(10);
                        let rate_limit_delay = Duration::from_secs(retry_secs);
                        warn!(
                            from = current_from,
                            to = current_to,
                            delay_secs = retry_secs,
                            "Rate limited (429), waiting"
                        );
                        tokio::time::sleep(rate_limit_delay).await;
                        continue;
                    }

                    warn!(
                        attempt = attempts,
                        max = max_attempts,
                        from = current_from,
                        to = current_to,
                        error = %e,
                        "Batch processing failed, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay *= 2; // Exponential backoff
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Fill a gap in processed blocks
    async fn fill_gap(&self, from_block: u64, to_block: u64) -> Result<()> {
        info!(from = from_block, to = to_block, "Filling gap");

        let mut current = from_block;
        while current <= to_block {
            let batch_size = self.batch_controller.get_size();
            let batch_end = (current + batch_size - 1).min(to_block);

            self.process_event_batch_with_retry(current, batch_end, 0)
                .await?;
            current = batch_end + 1;
        }

        Ok(())
    }

    /// Check if error message indicates "too many logs" (need smaller batch size)
    fn is_too_many_logs_error(msg: &str) -> bool {
        msg.contains("too many logs") || msg.contains("-32005")
    }

    /// Check if error message indicates rate limiting (need lower concurrency)
    fn is_rate_limit_error(msg: &str) -> bool {
        msg.contains("rate limit")
            || msg.contains("too many requests")
            || msg.contains("429")
            || msg.contains("resource exhausted")
    }

    /// Parse retry/wait seconds from error message
    /// Looks for patterns like "retry after 30 seconds", "wait 60s", "30 seconds", etc.
    fn parse_retry_seconds(msg: &str) -> Option<u64> {
        // Common patterns: "retry after X seconds", "wait X seconds", "X seconds", "Xs"
        let patterns = [
            r"retry.{0,10}?(\d+)\s*(?:second|sec|s\b)",
            r"wait.{0,10}?(\d+)\s*(?:second|sec|s\b)",
            r"after\s+(\d+)\s*(?:second|sec|s\b)",
            r"(\d+)\s*(?:second|sec)s?\s*(?:delay|wait|retry)",
        ];

        for pattern in patterns {
            if let Ok(re) = regex::Regex::new(pattern) {
                if let Some(caps) = re.captures(msg) {
                    if let Some(num) = caps.get(1) {
                        if let Ok(secs) = num.as_str().parse::<u64>() {
                            return Some(secs.max(1)); // At least 1 second
                        }
                    }
                }
            }
        }

        None
    }

    /// Check if error message indicates connection/timeout issues
    fn is_connection_error(msg: &str) -> bool {
        msg.contains("timeout")
            || msg.contains("connection")
            || msg.contains("limit exceeded")
            || msg.contains("decoding")
            || msg.contains("eof")
            || msg.contains("broken pipe")
            || msg.contains("reset by peer")
    }

    /// Check if an error is retryable (RPC rate limits, timeouts, too many logs, etc.)
    fn is_retryable_error(error: &IndexerError) -> bool {
        match error {
            IndexerError::Rpc(msg) => {
                let msg_lower = msg.to_lowercase();
                Self::is_too_many_logs_error(&msg_lower)
                    || Self::is_rate_limit_error(&msg_lower)
                    || Self::is_connection_error(&msg_lower)
            }
            _ => false,
        }
    }

    async fn process_event_batch_static(
        provider: &ProviderManager,
        processor: &EventProcessor,
        config: &IndexerConfig,
        from_block: u64,
        to_block: u64,
        batch_idx: usize,
        total_batches: usize,
    ) -> Result<()> {
        // Snapshot of known orderbooks BEFORE this batch
        let orderbook_addresses_before = processor.store().pools.get_all_orderbook_addresses();

        // Fetch ALL events in parallel: PoolManager + OrderBook + Balance
        let (pool_logs, orderbook_logs, balance_logs) = tokio::join!(
            Self::fetch_pool_events(provider, config, from_block, to_block),
            Self::fetch_orderbook_events(provider, &orderbook_addresses_before, from_block, to_block),
            Self::fetch_balance_events(provider, config, from_block, to_block)
        );

        let pool_logs = pool_logs?;
        let orderbook_logs = orderbook_logs?;
        let balance_logs = balance_logs?;

        // Combine all logs and sort by block number, then log index
        let mut all_logs: Vec<Log> = Vec::with_capacity(
            pool_logs.len() + orderbook_logs.len() + balance_logs.len()
        );
        all_logs.extend(pool_logs);
        all_logs.extend(orderbook_logs);
        all_logs.extend(balance_logs);

        // Sort by (block_number, log_index) to ensure proper ordering
        all_logs.sort_by(|a, b| {
            let block_a = a.block_number.unwrap_or(0);
            let block_b = b.block_number.unwrap_or(0);
            let log_idx_a = a.log_index.unwrap_or(0);
            let log_idx_b = b.log_index.unwrap_or(0);
            (block_a, log_idx_a).cmp(&(block_b, log_idx_b))
        });

        // Process all events in order
        // PoolCreated will be processed first (it has lower log_index in the same tx)
        // which ensures the orderbook is registered before its events are processed
        for log in all_logs {
            processor.process_log(log).await?;
        }

        // Check if any NEW pools were discovered in this batch
        // If so, we need to re-fetch orderbook events for them in THIS batch's block range
        let orderbook_addresses_after = processor.store().pools.get_all_orderbook_addresses();
        let new_pools: Vec<_> = orderbook_addresses_after
            .into_iter()
            .filter(|addr| !orderbook_addresses_before.contains(addr))
            .collect();

        if !new_pools.is_empty() {
            // Re-fetch orderbook events for newly discovered pools in this batch's range only
            let additional_logs = Self::fetch_orderbook_events(
                provider,
                &new_pools,
                from_block,
                to_block,
            ).await?;

            if !additional_logs.is_empty() {
                // Sort and process
                let mut additional_logs = additional_logs;
                additional_logs.sort_by(|a, b| {
                    let block_a = a.block_number.unwrap_or(0);
                    let block_b = b.block_number.unwrap_or(0);
                    let log_idx_a = a.log_index.unwrap_or(0);
                    let log_idx_b = b.log_index.unwrap_or(0);
                    (block_a, log_idx_a).cmp(&(block_b, log_idx_b))
                });

                for log in additional_logs {
                    processor.process_log(log).await?;
                }
            }
        }

        // Update sync state
        {
            let mut state = processor.store().sync_state.write().await;
            state.set_last_synced_block(to_block);
        }

        // Log progress periodically
        if batch_idx % 50 == 0 || batch_idx == total_batches - 1 {
            let pools = processor.store().pools.count();
            let orders = processor.store().orders.count();
            let trades = processor.store().trades.count();
            info!(
                batch = batch_idx + 1,
                total = total_batches,
                block = to_block,
                pools = pools,
                orders = orders,
                trades = trades,
                "Sync progress"
            );
        }

        Ok(())
    }

    async fn fetch_pool_events(
        provider: &ProviderManager,
        config: &IndexerConfig,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>> {
        let filter = Filter::new()
            .address(config.pool_manager)
            .event_signature(PoolCreated::SIGNATURE_HASH)
            .from_block(from_block)
            .to_block(to_block);

        provider
            .http()
            .get_logs(&filter)
            .await
            .map_err(|e| IndexerError::Rpc(e.to_string()))
    }

    async fn fetch_orderbook_events(
        provider: &ProviderManager,
        orderbook_addresses: &[alloy::primitives::Address],
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>> {
        if orderbook_addresses.is_empty() {
            return Ok(vec![]);
        }

        let orderbook_sigs = vec![
            OrderPlaced::SIGNATURE_HASH,
            OrderMatched::SIGNATURE_HASH,
            UpdateOrder::SIGNATURE_HASH,
            OrderCancelled::SIGNATURE_HASH,
        ];

        // Use event_signature for filtering by topic0
        let filter = Filter::new()
            .address(orderbook_addresses.to_vec())
            .event_signature(orderbook_sigs)
            .from_block(from_block)
            .to_block(to_block);

        provider
            .http()
            .get_logs(&filter)
            .await
            .map_err(|e| IndexerError::Rpc(e.to_string()))
    }

    async fn fetch_balance_events(
        provider: &ProviderManager,
        config: &IndexerConfig,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>> {
        let balance_sigs = vec![
            Deposit::SIGNATURE_HASH,
            Withdrawal::SIGNATURE_HASH,
            Lock::SIGNATURE_HASH,
            Unlock::SIGNATURE_HASH,
            TransferFrom::SIGNATURE_HASH,
            TransferLockedFrom::SIGNATURE_HASH,
        ];

        let filter = Filter::new()
            .address(config.balance_manager)
            .event_signature(balance_sigs)
            .from_block(from_block)
            .to_block(to_block);

        provider
            .http()
            .get_logs(&filter)
            .await
            .map_err(|e| IndexerError::Rpc(e.to_string()))
    }
}
