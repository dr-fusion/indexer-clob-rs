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
use tracing::{debug, error, info, warn};

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
    /// Returns the verified last synced block number
    pub async fn sync_to_head(&self) -> Result<u64> {
        // Check shutdown before starting
        if self.is_shutdown() {
            info!("Shutdown requested before historical sync started");
            let state = self.processor.store().sync_state.read().await;
            return Ok(state.last_synced_block);
        }

        let start_block = self.config.start_block;

        // Get initial target block
        let initial_target = self
            .provider
            .http()
            .get_block_number()
            .await
            .map_err(|e| IndexerError::Rpc(e.to_string()))?;

        let total_blocks = initial_target.saturating_sub(start_block);

        info!(
            start = start_block,
            end = initial_target,
            total = total_blocks,
            "Starting historical sync"
        );

        // Sync all events - fetches PoolCreated, OrderBook, and Balance events together
        // Processes them in order within each batch to ensure pools are discovered
        // before their events are processed
        self.sync_all_events(start_block, initial_target).await?;

        // Check if we were interrupted
        if self.is_shutdown() {
            info!("Historical sync interrupted by shutdown");
            let state = self.processor.store().sync_state.read().await;
            return Ok(state.last_synced_block);
        }

        // VERIFICATION LOOP: Ensure we've synced all blocks before switching to real-time
        // This handles blocks that arrived while we were syncing
        loop {
            let latest_block = self
                .provider
                .http()
                .get_block_number()
                .await
                .map_err(|e| IndexerError::Rpc(e.to_string()))?;

            let last_synced = {
                let state = self.processor.store().sync_state.read().await;
                state.last_synced_block
            };

            if last_synced >= latest_block {
                info!(
                    last_synced = last_synced,
                    latest_block = latest_block,
                    "Historical sync verified complete - all blocks synced"
                );

                let pools_count = self.processor.store().pools.count();
                let orders_count = self.processor.store().orders.count();
                let trades_count = self.processor.store().trades.count();

                info!(
                    pools = pools_count,
                    orders = orders_count,
                    trades = trades_count,
                    verified_block = last_synced,
                    "Historical sync complete"
                );

                return Ok(last_synced);
            }

            // Check shutdown
            if self.is_shutdown() {
                info!("Shutdown requested during verification loop");
                return Ok(last_synced);
            }

            // There's a gap - sync remaining blocks
            info!(
                from = last_synced + 1,
                to = latest_block,
                gap = latest_block - last_synced,
                "Syncing remaining blocks before switching to real-time"
            );
            self.sync_all_events(last_synced + 1, latest_block).await?;
        }
    }

    /// Sync all events using windowed processing:
    /// - Fetch N batches in parallel (where N = concurrency)
    /// - Each batch fetches ALL events (PoolCreated + OrderBook + Balance)
    /// - Process those N batches in sequential order
    /// - If a batch fails, retry immediately with exponential backoff (not deferred)
    /// - If new pools discovered, fetch their orderbook events for that batch
    /// - Move to next window only after ALL batches are complete
    /// - Track sync state contiguously (no skipping ahead past failed batches)
    /// Uses AIMD algorithm to dynamically adjust batch size and concurrency based on RPC responses
    async fn sync_all_events(&self, from_block: u64, to_block: u64) -> Result<()> {
        use std::collections::HashMap;

        let initial_batch_size = self.batch_controller.get_size();
        let initial_concurrency = self.concurrency_controller.get();

        info!(
            from = from_block,
            to = to_block,
            initial_batch_size = initial_batch_size,
            initial_concurrency = initial_concurrency,
            "Starting sync with windowed processing"
        );

        // Track processed ranges for contiguous sync state
        let range_tracker = BlockRangeTracker::new(to_block);

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

        let mut processed_batches = 0usize;
        let max_retries = self.config.sync.retry_attempts;

        // Process batches in windows based on concurrency
        let mut window_start = 0usize;
        while window_start < total_batches {
            // Check shutdown
            if self.is_shutdown() {
                info!("Shutdown requested");
                return Ok(());
            }

            // Snapshot current known orderbooks before this window
            let known_orderbooks_before: Vec<alloy::primitives::Address> =
                self.processor.store().pools.get_all_orderbook_addresses();

            // Get current concurrency (may change based on AIMD)
            let concurrency = self.concurrency_controller.get();
            let window_end = (window_start + concurrency).min(total_batches);
            let window_size = window_end - window_start;

            debug!(
                window_start = window_start + 1,
                window_end = window_end,
                window_size = window_size,
                total = total_batches,
                concurrency = concurrency,
                known_orderbooks = known_orderbooks_before.len(),
                "Starting window"
            );

            // Fetch all batches in this window in parallel
            let window_batches: Vec<(usize, u64, u64)> = batches[window_start..window_end]
                .iter()
                .enumerate()
                .map(|(i, (from, to))| (window_start + i, *from, *to))
                .collect();

            type FetchResult = std::result::Result<(usize, u64, u64, Vec<Log>), (usize, u64, u64, IndexerError)>;

            let batch_controller = Arc::clone(&self.batch_controller);
            let concurrency_controller = Arc::clone(&self.concurrency_controller);
            let provider = Arc::clone(&self.provider);
            let config = self.config.clone();
            let shutdown = Arc::clone(&self.shutdown);
            let known_orderbooks = Arc::new(known_orderbooks_before.clone());

            let fetch_results: Vec<FetchResult> = stream::iter(window_batches)
                .map(|(batch_idx, from, to)| {
                    let provider = Arc::clone(&provider);
                    let config = config.clone();
                    let batch_controller = Arc::clone(&batch_controller);
                    let concurrency_controller = Arc::clone(&concurrency_controller);
                    let shutdown = Arc::clone(&shutdown);
                    let orderbook_addresses = Arc::clone(&known_orderbooks);
                    let total = total_batches;

                    async move {
                        // Check for shutdown
                        if shutdown.load(Ordering::Relaxed) {
                            return Err((batch_idx, from, to, IndexerError::Sync("Shutdown requested".to_string())));
                        }

                        let fetch_start = std::time::Instant::now();

                        // Fetch ALL events in parallel (PoolCreated + OrderBook + Balance = 11 calls)
                        let (
                            pool_created,
                            order_placed,
                            order_matched,
                            update_order,
                            order_cancelled,
                            deposit,
                            withdrawal,
                            lock,
                            unlock,
                            transfer_from,
                            transfer_locked_from,
                        ) = tokio::join!(
                            Self::fetch_single_event(&provider, config.pool_manager, PoolCreated::SIGNATURE_HASH, from, to, "PoolCreated"),
                            Self::fetch_single_event_multi_address(&provider, &orderbook_addresses, OrderPlaced::SIGNATURE_HASH, from, to, "OrderPlaced"),
                            Self::fetch_single_event_multi_address(&provider, &orderbook_addresses, OrderMatched::SIGNATURE_HASH, from, to, "OrderMatched"),
                            Self::fetch_single_event_multi_address(&provider, &orderbook_addresses, UpdateOrder::SIGNATURE_HASH, from, to, "UpdateOrder"),
                            Self::fetch_single_event_multi_address(&provider, &orderbook_addresses, OrderCancelled::SIGNATURE_HASH, from, to, "OrderCancelled"),
                            Self::fetch_single_event(&provider, config.balance_manager, Deposit::SIGNATURE_HASH, from, to, "Deposit"),
                            Self::fetch_single_event(&provider, config.balance_manager, Withdrawal::SIGNATURE_HASH, from, to, "Withdrawal"),
                            Self::fetch_single_event(&provider, config.balance_manager, Lock::SIGNATURE_HASH, from, to, "Lock"),
                            Self::fetch_single_event(&provider, config.balance_manager, Unlock::SIGNATURE_HASH, from, to, "Unlock"),
                            Self::fetch_single_event(&provider, config.balance_manager, TransferFrom::SIGNATURE_HASH, from, to, "TransferFrom"),
                            Self::fetch_single_event(&provider, config.balance_manager, TransferLockedFrom::SIGNATURE_HASH, from, to, "TransferLockedFrom"),
                        );

                        // Collect results, propagating first error
                        let mut all_logs: Vec<Log> = Vec::new();

                        match (pool_created, order_placed, order_matched, update_order, order_cancelled,
                               deposit, withdrawal, lock, unlock, transfer_from, transfer_locked_from) {
                            (Ok(a), Ok(b), Ok(c), Ok(d), Ok(e), Ok(f), Ok(g), Ok(h), Ok(i), Ok(j), Ok(k)) => {
                                all_logs.extend(a);
                                all_logs.extend(b);
                                all_logs.extend(c);
                                all_logs.extend(d);
                                all_logs.extend(e);
                                all_logs.extend(f);
                                all_logs.extend(g);
                                all_logs.extend(h);
                                all_logs.extend(i);
                                all_logs.extend(j);
                                all_logs.extend(k);
                            }
                            (Err(e), _, _, _, _, _, _, _, _, _, _) |
                            (_, Err(e), _, _, _, _, _, _, _, _, _) |
                            (_, _, Err(e), _, _, _, _, _, _, _, _) |
                            (_, _, _, Err(e), _, _, _, _, _, _, _) |
                            (_, _, _, _, Err(e), _, _, _, _, _, _) |
                            (_, _, _, _, _, Err(e), _, _, _, _, _) |
                            (_, _, _, _, _, _, Err(e), _, _, _, _) |
                            (_, _, _, _, _, _, _, Err(e), _, _, _) |
                            (_, _, _, _, _, _, _, _, Err(e), _, _) |
                            (_, _, _, _, _, _, _, _, _, Err(e), _) |
                            (_, _, _, _, _, _, _, _, _, _, Err(e)) => {
                                let err_msg = e.to_string().to_lowercase();
                                if Self::is_too_many_logs_error(&err_msg) {
                                    warn!(from = from, to = to, error = %e, "Too many logs - reducing batch size");
                                    batch_controller.report_error();
                                } else if Self::is_rate_limit_error(&err_msg) {
                                    warn!(from = from, to = to, error = %e, "Rate limit - reducing concurrency");
                                    concurrency_controller.report_error();
                                } else if Self::is_connection_error(&err_msg) {
                                    warn!(from = from, to = to, error = %e, "Connection error - reducing concurrency");
                                    concurrency_controller.report_error();
                                } else {
                                    error!(from = from, to = to, error = %e, "Unknown fetch error");
                                }
                                return Err((batch_idx, from, to, e));
                            }
                        }

                        // Sort by (block_number, log_index)
                        all_logs.sort_by(|a, b| {
                            let block_a = a.block_number.unwrap_or(0);
                            let block_b = b.block_number.unwrap_or(0);
                            let log_idx_a = a.log_index.unwrap_or(0);
                            let log_idx_b = b.log_index.unwrap_or(0);
                            (block_a, log_idx_a).cmp(&(block_b, log_idx_b))
                        });

                        batch_controller.report_success();
                        concurrency_controller.report_success();

                        debug!(
                            batch = batch_idx + 1,
                            total = total,
                            from = from,
                            to = to,
                            events = all_logs.len(),
                            fetch_ms = fetch_start.elapsed().as_millis(),
                            "Fetched batch"
                        );

                        Ok((batch_idx, from, to, all_logs))
                    }
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;

            // Collect results into a map for ordered processing
            // Failed batches will be retried immediately (not deferred)
            let mut fetched_window: HashMap<usize, (u64, u64, Vec<Log>)> = HashMap::new();
            let mut failed_batches: Vec<(usize, u64, u64)> = Vec::new();

            for result in fetch_results {
                match result {
                    Ok((batch_idx, from, to, logs)) => {
                        fetched_window.insert(batch_idx, (from, to, logs));
                    }
                    Err((batch_idx, from, to, e)) => {
                        if Self::is_retryable_error(&e) {
                            warn!(batch = batch_idx + 1, from = from, to = to, "Batch fetch failed, will retry immediately");
                            failed_batches.push((batch_idx, from, to));
                        } else {
                            error!(batch = batch_idx + 1, from = from, to = to, error = %e, "Non-retryable fetch error");
                            return Err(e);
                        }
                    }
                }
            }

            // Process batches in this window in sequential order
            for batch_idx in window_start..window_end {
                // Check shutdown
                if self.is_shutdown() {
                    info!("Shutdown requested during processing");
                    return Ok(());
                }

                let (from, to, logs) = if let Some(data) = fetched_window.remove(&batch_idx) {
                    data
                } else {
                    // This batch failed - retry immediately with exponential backoff
                    let (from, to) = batches[batch_idx];
                    warn!(batch = batch_idx + 1, from = from, to = to, "Retrying failed batch immediately");

                    let logs = self.retry_fetch_with_backoff(from, to, max_retries).await?;
                    (from, to, logs)
                };

                let process_start = std::time::Instant::now();

                // Snapshot orderbooks before processing this batch
                let orderbooks_before: Vec<alloy::primitives::Address> =
                    self.processor.store().pools.get_all_orderbook_addresses();

                // Process all logs for this batch
                for log in &logs {
                    self.processor.process_log(log.clone()).await?;
                }

                // Check if new pools were discovered in this batch
                let orderbooks_after = self.processor.store().pools.get_all_orderbook_addresses();
                let new_orderbooks: Vec<_> = orderbooks_after
                    .into_iter()
                    .filter(|addr| !orderbooks_before.contains(addr))
                    .collect();

                // If new pools discovered, fetch their orderbook events for this batch range
                let mut additional_events = 0usize;
                if !new_orderbooks.is_empty() {
                    debug!(
                        batch = batch_idx + 1,
                        new_pools = new_orderbooks.len(),
                        from = from,
                        to = to,
                        "New pools discovered, fetching their orderbook events"
                    );

                    let (order_placed, order_matched, update_order, order_cancelled) = tokio::join!(
                        Self::fetch_single_event_multi_address(&self.provider, &new_orderbooks, OrderPlaced::SIGNATURE_HASH, from, to, "OrderPlaced"),
                        Self::fetch_single_event_multi_address(&self.provider, &new_orderbooks, OrderMatched::SIGNATURE_HASH, from, to, "OrderMatched"),
                        Self::fetch_single_event_multi_address(&self.provider, &new_orderbooks, UpdateOrder::SIGNATURE_HASH, from, to, "UpdateOrder"),
                        Self::fetch_single_event_multi_address(&self.provider, &new_orderbooks, OrderCancelled::SIGNATURE_HASH, from, to, "OrderCancelled"),
                    );

                    let mut new_pool_logs: Vec<Log> = Vec::new();
                    new_pool_logs.extend(order_placed?);
                    new_pool_logs.extend(order_matched?);
                    new_pool_logs.extend(update_order?);
                    new_pool_logs.extend(order_cancelled?);

                    if !new_pool_logs.is_empty() {
                        // Sort and process
                        new_pool_logs.sort_by(|a, b| {
                            let block_a = a.block_number.unwrap_or(0);
                            let block_b = b.block_number.unwrap_or(0);
                            let log_idx_a = a.log_index.unwrap_or(0);
                            let log_idx_b = b.log_index.unwrap_or(0);
                            (block_a, log_idx_a).cmp(&(block_b, log_idx_b))
                        });

                        additional_events = new_pool_logs.len();
                        for log in new_pool_logs {
                            self.processor.process_log(log).await?;
                        }
                    }
                }

                // Mark this range as processed in the tracker
                range_tracker.mark_processed(from, to).await;

                // Update sync state only for contiguous range
                // This ensures we don't skip ahead past failed batches
                let contiguous_end = range_tracker.get_contiguous_end(from_block).await;
                {
                    let mut state = self.processor.store().sync_state.write().await;
                    state.set_last_synced_block(contiguous_end);
                }

                processed_batches += 1;
                let process_ms = process_start.elapsed().as_millis();

                // Log progress every 10 batches or at end
                if processed_batches % 10 == 0 || batch_idx == total_batches - 1 {
                    info!(
                        batch = batch_idx + 1,
                        total = total_batches,
                        from = from,
                        to = to,
                        contiguous_synced = contiguous_end,
                        events = logs.len() + additional_events,
                        process_ms = process_ms,
                        pools = self.processor.store().pools.count(),
                        orders = self.processor.store().orders.count(),
                        trades = self.processor.store().trades.count(),
                        "Processed batch"
                    );
                }
            }

            // Move to next window
            window_start = window_end;
        }

        // Verify all blocks are synced (no gaps)
        let gaps = range_tracker.get_gaps(from_block).await;
        if !gaps.is_empty() {
            warn!(gaps = ?gaps, "Gaps detected after sync - filling");
            for (gap_from, gap_to) in gaps {
                if self.is_shutdown() {
                    info!("Shutdown requested during gap filling");
                    return Ok(());
                }
                self.fill_gap(gap_from, gap_to).await?;
            }
        }

        info!(
            final_batch_size = self.batch_controller.get_size(),
            final_concurrency = self.concurrency_controller.get(),
            pools = self.processor.store().pools.count(),
            orders = self.processor.store().orders.count(),
            trades = self.processor.store().trades.count(),
            "Historical sync complete"
        );

        Ok(())
    }

    /// Retry fetching a batch with exponential backoff
    /// Returns the fetched logs on success
    async fn retry_fetch_with_backoff(
        &self,
        from: u64,
        to: u64,
        max_retries: u32,
    ) -> Result<Vec<Log>> {
        let mut attempts = 0u32;
        let mut delay = Duration::from_millis(self.config.sync.retry_delay_ms);
        let max_delay = Duration::from_secs(30);

        loop {
            attempts += 1;

            // Get current orderbooks for this retry
            let orderbook_addresses = self.processor.store().pools.get_all_orderbook_addresses();

            // Try to fetch all events
            let result = Self::fetch_all_events_parallel(
                &self.provider,
                &self.config,
                &orderbook_addresses,
                from,
                to,
            )
            .await;

            match result {
                Ok(logs) => {
                    if attempts > 1 {
                        info!(from = from, to = to, attempts = attempts, "Batch fetch succeeded after retry");
                    }
                    self.batch_controller.report_success();
                    self.concurrency_controller.report_success();
                    return Ok(logs);
                }
                Err(e) if attempts < max_retries && Self::is_retryable_error(&e) => {
                    let err_msg = e.to_string().to_lowercase();

                    // Adjust AIMD controllers based on error type
                    if Self::is_too_many_logs_error(&err_msg) {
                        self.batch_controller.report_error();
                    } else if Self::is_rate_limit_error(&err_msg) || Self::is_connection_error(&err_msg) {
                        self.concurrency_controller.report_error();
                    }

                    // For rate limits, try to parse the retry time
                    let actual_delay = if Self::is_rate_limit_error(&err_msg) {
                        let retry_secs = Self::parse_retry_seconds(&err_msg).unwrap_or(10);
                        Duration::from_secs(retry_secs)
                    } else {
                        delay
                    };

                    warn!(
                        from = from,
                        to = to,
                        attempt = attempts,
                        max_retries = max_retries,
                        delay_ms = actual_delay.as_millis(),
                        error = %e,
                        "Batch fetch failed, retrying with backoff"
                    );

                    tokio::time::sleep(actual_delay).await;
                    delay = (delay * 2).min(max_delay); // Exponential backoff capped at max
                }
                Err(e) => {
                    error!(from = from, to = to, attempts = attempts, error = %e, "Batch fetch failed after max retries");
                    return Err(e);
                }
            }
        }
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
        let total_blocks = to_block - from_block + 1;
        info!(
            from = from_block,
            to = to_block,
            blocks = total_blocks,
            batch_size = self.batch_controller.get_size(),
            "Filling gap - starting"
        );

        let gap_start = std::time::Instant::now();
        let mut current = from_block;
        let mut batches_processed = 0;

        while current <= to_block {
            // Check shutdown
            if self.is_shutdown() {
                info!(from = from_block, to = to_block, "Gap fill interrupted by shutdown");
                return Ok(());
            }

            let batch_size = self.batch_controller.get_size();
            let batch_end = (current + batch_size - 1).min(to_block);

            debug!(
                from = current,
                to = batch_end,
                remaining = to_block - current + 1,
                "Gap fill batch"
            );

            self.process_event_batch_with_retry(current, batch_end, 0)
                .await?;

            current = batch_end + 1;
            batches_processed += 1;
        }

        let elapsed = gap_start.elapsed();
        info!(
            from = from_block,
            to = to_block,
            blocks = total_blocks,
            batches = batches_processed,
            elapsed_secs = elapsed.as_secs(),
            "Gap filled successfully"
        );

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
            || msg.contains("error sending request")
            || msg.contains("request error")
            || msg.contains("network")
            || msg.contains("dns")
            || msg.contains("resolve")
            || msg.contains("unreachable")
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
        use std::time::Instant;

        let batch_start = Instant::now();

        // Snapshot of known orderbooks BEFORE this batch
        let orderbook_addresses_before = processor.store().pools.get_all_orderbook_addresses();

        // Fetch ALL events in parallel - 11 concurrent RPC calls (one per event type)
        let fetch_start = Instant::now();
        debug!(
            from = from_block,
            to = to_block,
            blocks = to_block - from_block + 1,
            orderbooks = orderbook_addresses_before.len(),
            "Fetching events (11 parallel RPC calls)"
        );

        let all_logs = Self::fetch_all_events_parallel(
            provider,
            config,
            &orderbook_addresses_before,
            from_block,
            to_block,
        ).await?;

        let fetch_ms = fetch_start.elapsed().as_millis();
        let total_events = all_logs.len();

        debug!(
            from = from_block,
            to = to_block,
            total = total_events,
            fetch_ms = fetch_ms,
            "Fetched all events"
        );

        // Process all events in order (already sorted by block_number, log_index)
        let process_start = Instant::now();
        for log in &all_logs {
            processor.process_log(log.clone()).await?;
        }
        let process_ms = process_start.elapsed().as_millis();

        // Check if any NEW pools were discovered in this batch
        let orderbook_addresses_after = processor.store().pools.get_all_orderbook_addresses();
        let new_pools: Vec<_> = orderbook_addresses_after
            .into_iter()
            .filter(|addr| !orderbook_addresses_before.contains(addr))
            .collect();

        let mut additional_events = 0;
        if !new_pools.is_empty() {
            info!(
                from = from_block,
                to = to_block,
                new_pools = new_pools.len(),
                "Discovered new pools, re-fetching orderbook events"
            );

            // Re-fetch orderbook events for newly discovered pools (4 parallel calls)
            let (order_placed, order_matched, update_order, order_cancelled) = tokio::join!(
                Self::fetch_single_event_multi_address(
                    provider,
                    &new_pools,
                    OrderPlaced::SIGNATURE_HASH,
                    from_block,
                    to_block,
                    "OrderPlaced"
                ),
                Self::fetch_single_event_multi_address(
                    provider,
                    &new_pools,
                    OrderMatched::SIGNATURE_HASH,
                    from_block,
                    to_block,
                    "OrderMatched"
                ),
                Self::fetch_single_event_multi_address(
                    provider,
                    &new_pools,
                    UpdateOrder::SIGNATURE_HASH,
                    from_block,
                    to_block,
                    "UpdateOrder"
                ),
                Self::fetch_single_event_multi_address(
                    provider,
                    &new_pools,
                    OrderCancelled::SIGNATURE_HASH,
                    from_block,
                    to_block,
                    "OrderCancelled"
                ),
            );

            let mut additional_logs: Vec<Log> = Vec::new();
            additional_logs.extend(order_placed?);
            additional_logs.extend(order_matched?);
            additional_logs.extend(update_order?);
            additional_logs.extend(order_cancelled?);

            additional_events = additional_logs.len();
            if !additional_logs.is_empty() {
                // Sort by (block_number, log_index)
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

                info!(
                    from = from_block,
                    to = to_block,
                    additional_events = additional_events,
                    "Processed additional orderbook events for new pools"
                );
            }
        }

        // Update sync state
        {
            let mut state = processor.store().sync_state.write().await;
            state.set_last_synced_block(to_block);
        }

        let batch_ms = batch_start.elapsed().as_millis();

        // Log every batch with timing info
        info!(
            batch = batch_idx + 1,
            total = total_batches,
            from = from_block,
            to = to_block,
            events = total_events + additional_events,
            fetch_ms = fetch_ms,
            process_ms = process_ms,
            total_ms = batch_ms,
            "Batch complete"
        );

        Ok(())
    }

    /// Fetch a single event type from a contract
    async fn fetch_single_event(
        provider: &ProviderManager,
        address: alloy::primitives::Address,
        event_sig: alloy::primitives::FixedBytes<32>,
        from_block: u64,
        to_block: u64,
        event_name: &str,
    ) -> Result<Vec<Log>> {
        let filter = Filter::new()
            .address(address)
            .event_signature(event_sig)
            .from_block(from_block)
            .to_block(to_block);

        let logs = provider
            .http()
            .get_logs(&filter)
            .await
            .map_err(|e| {
                // Use Debug format to get full error chain including response details
                IndexerError::Rpc(format!("{:?}", e))
            })?;

        if !logs.is_empty() {
            debug!(
                event = event_name,
                address = ?address,
                from = from_block,
                to = to_block,
                count = logs.len(),
                "Fetched event logs"
            );
        }

        Ok(logs)
    }

    /// Fetch a single event type from multiple contracts (e.g., orderbooks)
    async fn fetch_single_event_multi_address(
        provider: &ProviderManager,
        addresses: &[alloy::primitives::Address],
        event_sig: alloy::primitives::FixedBytes<32>,
        from_block: u64,
        to_block: u64,
        event_name: &str,
    ) -> Result<Vec<Log>> {
        if addresses.is_empty() {
            return Ok(vec![]);
        }

        let filter = Filter::new()
            .address(addresses.to_vec())
            .event_signature(event_sig)
            .from_block(from_block)
            .to_block(to_block);

        let logs = provider
            .http()
            .get_logs(&filter)
            .await
            .map_err(|e| {
                // Use Debug format to get full error chain including response details
                IndexerError::Rpc(format!("{:?}", e))
            })?;

        if !logs.is_empty() {
            debug!(
                event = event_name,
                addresses = addresses.len(),
                from = from_block,
                to = to_block,
                count = logs.len(),
                "Fetched event logs (multi-address)"
            );
        }

        Ok(logs)
    }

    /// Fetch all events in parallel - one RPC call per event type
    async fn fetch_all_events_parallel(
        provider: &ProviderManager,
        config: &IndexerConfig,
        orderbook_addresses: &[alloy::primitives::Address],
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>> {
        // Launch all event fetches in parallel
        let (
            pool_created,
            order_placed,
            order_matched,
            update_order,
            order_cancelled,
            deposit,
            withdrawal,
            lock,
            unlock,
            transfer_from,
            transfer_locked_from,
        ) = tokio::join!(
            // PoolManager events
            Self::fetch_single_event(
                provider,
                config.pool_manager,
                PoolCreated::SIGNATURE_HASH,
                from_block,
                to_block,
                "PoolCreated"
            ),
            // OrderBook events (all orderbooks)
            Self::fetch_single_event_multi_address(
                provider,
                orderbook_addresses,
                OrderPlaced::SIGNATURE_HASH,
                from_block,
                to_block,
                "OrderPlaced"
            ),
            Self::fetch_single_event_multi_address(
                provider,
                orderbook_addresses,
                OrderMatched::SIGNATURE_HASH,
                from_block,
                to_block,
                "OrderMatched"
            ),
            Self::fetch_single_event_multi_address(
                provider,
                orderbook_addresses,
                UpdateOrder::SIGNATURE_HASH,
                from_block,
                to_block,
                "UpdateOrder"
            ),
            Self::fetch_single_event_multi_address(
                provider,
                orderbook_addresses,
                OrderCancelled::SIGNATURE_HASH,
                from_block,
                to_block,
                "OrderCancelled"
            ),
            // BalanceManager events
            Self::fetch_single_event(
                provider,
                config.balance_manager,
                Deposit::SIGNATURE_HASH,
                from_block,
                to_block,
                "Deposit"
            ),
            Self::fetch_single_event(
                provider,
                config.balance_manager,
                Withdrawal::SIGNATURE_HASH,
                from_block,
                to_block,
                "Withdrawal"
            ),
            Self::fetch_single_event(
                provider,
                config.balance_manager,
                Lock::SIGNATURE_HASH,
                from_block,
                to_block,
                "Lock"
            ),
            Self::fetch_single_event(
                provider,
                config.balance_manager,
                Unlock::SIGNATURE_HASH,
                from_block,
                to_block,
                "Unlock"
            ),
            Self::fetch_single_event(
                provider,
                config.balance_manager,
                TransferFrom::SIGNATURE_HASH,
                from_block,
                to_block,
                "TransferFrom"
            ),
            Self::fetch_single_event(
                provider,
                config.balance_manager,
                TransferLockedFrom::SIGNATURE_HASH,
                from_block,
                to_block,
                "TransferLockedFrom"
            ),
        );

        // Collect all results, propagating errors
        let mut all_logs: Vec<Log> = Vec::new();
        all_logs.extend(pool_created?);
        all_logs.extend(order_placed?);
        all_logs.extend(order_matched?);
        all_logs.extend(update_order?);
        all_logs.extend(order_cancelled?);
        all_logs.extend(deposit?);
        all_logs.extend(withdrawal?);
        all_logs.extend(lock?);
        all_logs.extend(unlock?);
        all_logs.extend(transfer_from?);
        all_logs.extend(transfer_locked_from?);

        // Sort by (block_number, log_index) to ensure proper ordering
        all_logs.sort_by(|a, b| {
            let block_a = a.block_number.unwrap_or(0);
            let block_b = b.block_number.unwrap_or(0);
            let log_idx_a = a.log_index.unwrap_or(0);
            let log_idx_b = b.log_index.unwrap_or(0);
            (block_a, log_idx_a).cmp(&(block_b, log_idx_b))
        });

        Ok(all_logs)
    }
}
