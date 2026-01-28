use alloy::rpc::types::{Filter, Log};
use alloy_sol_types::SolEvent;
use futures::{stream, StreamExt};
use indexer_core::events::{
    Deposit, Lock, OrderCancelled, OrderMatched, OrderPlaced, PoolCreated, TransferFrom,
    TransferLockedFrom, Unlock, UpdateOrder, Withdrawal,
};
use indexer_core::types::now_micros;
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

/// Tracks event counts per type for logging
#[derive(Default)]
struct EventCounts {
    pool_created: usize,
    order_placed: usize,
    order_matched: usize,
    update_order: usize,
    order_cancelled: usize,
    deposit: usize,
    withdrawal: usize,
    lock: usize,
    unlock: usize,
    transfer_from: usize,
    transfer_locked_from: usize,
}

impl EventCounts {
    /// Count events by topic0 (event signature) from a list of logs
    fn from_logs(logs: &[Log]) -> Self {
        let mut counts = Self::default();
        for log in logs {
            if let Some(topic0) = log.topics().first() {
                if *topic0 == PoolCreated::SIGNATURE_HASH {
                    counts.pool_created += 1;
                } else if *topic0 == OrderPlaced::SIGNATURE_HASH {
                    counts.order_placed += 1;
                } else if *topic0 == OrderMatched::SIGNATURE_HASH {
                    counts.order_matched += 1;
                } else if *topic0 == UpdateOrder::SIGNATURE_HASH {
                    counts.update_order += 1;
                } else if *topic0 == OrderCancelled::SIGNATURE_HASH {
                    counts.order_cancelled += 1;
                } else if *topic0 == Deposit::SIGNATURE_HASH {
                    counts.deposit += 1;
                } else if *topic0 == Withdrawal::SIGNATURE_HASH {
                    counts.withdrawal += 1;
                } else if *topic0 == Lock::SIGNATURE_HASH {
                    counts.lock += 1;
                } else if *topic0 == Unlock::SIGNATURE_HASH {
                    counts.unlock += 1;
                } else if *topic0 == TransferFrom::SIGNATURE_HASH {
                    counts.transfer_from += 1;
                } else if *topic0 == TransferLockedFrom::SIGNATURE_HASH {
                    counts.transfer_locked_from += 1;
                }
            }
        }
        counts
    }

    /// Returns a compact summary string of non-zero event counts
    fn summary(&self) -> String {
        let mut parts = Vec::new();
        if self.pool_created > 0 {
            parts.push(format!("pools:{}", self.pool_created));
        }
        if self.order_placed > 0 {
            parts.push(format!("placed:{}", self.order_placed));
        }
        if self.order_matched > 0 {
            parts.push(format!("matched:{}", self.order_matched));
        }
        if self.update_order > 0 {
            parts.push(format!("updated:{}", self.update_order));
        }
        if self.order_cancelled > 0 {
            parts.push(format!("cancelled:{}", self.order_cancelled));
        }
        if self.deposit > 0 {
            parts.push(format!("deposits:{}", self.deposit));
        }
        if self.withdrawal > 0 {
            parts.push(format!("withdrawals:{}", self.withdrawal));
        }
        if self.lock > 0 {
            parts.push(format!("locks:{}", self.lock));
        }
        if self.unlock > 0 {
            parts.push(format!("unlocks:{}", self.unlock));
        }
        if self.transfer_from > 0 {
            parts.push(format!("transfers:{}", self.transfer_from));
        }
        if self.transfer_locked_from > 0 {
            parts.push(format!("locked_transfers:{}", self.transfer_locked_from));
        }
        if parts.is_empty() {
            "none".to_string()
        } else {
            parts.join(" ")
        }
    }
}

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
            let _window_size = window_end - window_start;

            info!(
                window = format!("{}-{}/{}", window_start + 1, window_end, total_batches),
                concurrency = concurrency,
                batch_size = self.batch_controller.get_size(),
                known_orderbooks = known_orderbooks_before.len(),
                "Starting fetch window"
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

                        info!(
                            batch = batch_idx + 1,
                            total = total,
                            blocks = format!("{}-{}", from, to),
                            block_count = to - from + 1,
                            orderbooks = orderbook_addresses.len(),
                            "Fetching batch (1 RPC call)"
                        );

                        let fetch_start = std::time::Instant::now();

                        // Combine all addresses: PoolManager + BalanceManager + OrderBooks
                        let mut all_addresses = vec![config.pool_manager, config.balance_manager];
                        all_addresses.extend(orderbook_addresses.iter().cloned());

                        // Combine all 11 event signatures
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
                            .from_block(from)
                            .to_block(to);

                        let result = provider.http().get_logs(&filter).await;
                        let fetch_ms = fetch_start.elapsed().as_millis();

                        match result {
                            Ok(mut all_logs) => {
                                // Sort by (block_number, log_index)
                                all_logs.sort_by(|a, b| {
                                    let block_a = a.block_number.unwrap_or(0);
                                    let block_b = b.block_number.unwrap_or(0);
                                    let log_idx_a = a.log_index.unwrap_or(0);
                                    let log_idx_b = b.log_index.unwrap_or(0);
                                    (block_a, log_idx_a).cmp(&(block_b, log_idx_b))
                                });

                                // Count events by type for logging
                                let event_counts = EventCounts::from_logs(&all_logs);

                                batch_controller.report_success();
                                concurrency_controller.report_success();

                                let total_events = all_logs.len();

                                // Log at INFO level with event breakdown
                                info!(
                                    batch = batch_idx + 1,
                                    total = total,
                                    blocks = format!("{}-{}", from, to),
                                    fetch_ms = fetch_ms,
                                    events = total_events,
                                    breakdown = %event_counts.summary(),
                                    "Batch fetched"
                                );

                                Ok((batch_idx, from, to, all_logs))
                            }
                            Err(e) => {
                                let err = IndexerError::Rpc(format!("{:?}", e));
                                let err_msg = err.to_string().to_lowercase();

                                if Self::is_too_many_logs_error(&err_msg) {
                                    warn!(
                                        batch = batch_idx,
                                        from = from,
                                        to = to,
                                        blocks = to - from + 1,
                                        fetch_ms = fetch_ms,
                                        error = %err,
                                        "Too many logs - reducing batch size"
                                    );
                                    batch_controller.report_error();
                                } else if Self::is_rate_limit_error(&err_msg) {
                                    warn!(
                                        batch = batch_idx,
                                        from = from,
                                        to = to,
                                        blocks = to - from + 1,
                                        fetch_ms = fetch_ms,
                                        error = %err,
                                        "Rate limit - reducing concurrency"
                                    );
                                    concurrency_controller.report_error();
                                } else if Self::is_connection_error(&err_msg) {
                                    warn!(
                                        batch = batch_idx,
                                        from = from,
                                        to = to,
                                        blocks = to - from + 1,
                                        fetch_ms = fetch_ms,
                                        error = %err,
                                        "Connection error - reducing concurrency"
                                    );
                                    concurrency_controller.report_error();
                                } else {
                                    error!(
                                        batch = batch_idx,
                                        from = from,
                                        to = to,
                                        blocks = to - from + 1,
                                        fetch_ms = fetch_ms,
                                        error = %err,
                                        error_debug = ?e,
                                        "Unknown fetch error"
                                    );
                                }
                                Err((batch_idx, from, to, err))
                            }
                        }
                    }
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;

            // Collect results into a map for ordered processing
            // Failed batches will be retried immediately (not deferred)
            let mut fetched_window: HashMap<usize, (u64, u64, Vec<Log>)> = HashMap::new();
            let mut failed_batches: Vec<(usize, u64, u64)> = Vec::new();

            // Track additional orderbook events to add to subsequent batches
            // Key: batch_idx, Value: additional logs for new pools discovered in earlier batches
            let mut additional_logs_for_batch: HashMap<usize, Vec<Log>> = HashMap::new();

            for result in fetch_results {
                match result {
                    Ok((batch_idx, from, to, logs)) => {
                        fetched_window.insert(batch_idx, (from, to, logs));
                    }
                    Err((batch_idx, from, to, e)) => {
                        if Self::is_retryable_error(&e) {
                            warn!(
                                batch = batch_idx + 1,
                                from = from,
                                to = to,
                                blocks = to - from + 1,
                                error = %e,
                                "Batch fetch failed (retryable), will retry immediately"
                            );
                            failed_batches.push((batch_idx, from, to));
                        } else {
                            error!(
                                batch = batch_idx + 1,
                                from = from,
                                to = to,
                                blocks = to - from + 1,
                                error = %e,
                                error_debug = ?e,
                                "Non-retryable fetch error - see error_debug for full details"
                            );
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

                // Process all logs for this batch with detailed timing
                let batch_received_at = now_micros();
                for log in &logs {
                    let log_start = std::time::Instant::now();
                    let block_num = log.block_number.unwrap_or_default();
                    let log_idx = log.log_index.unwrap_or_default();
                    let topic0 = log.topics().first().cloned();

                    // Identify event type for logging
                    let event_type = match topic0 {
                        Some(t) if t == OrderPlaced::SIGNATURE_HASH => "OrderPlaced",
                        Some(t) if t == OrderMatched::SIGNATURE_HASH => "OrderMatched",
                        Some(t) if t == UpdateOrder::SIGNATURE_HASH => "UpdateOrder",
                        Some(t) if t == OrderCancelled::SIGNATURE_HASH => "OrderCancelled",
                        Some(t) if t == PoolCreated::SIGNATURE_HASH => "PoolCreated",
                        Some(t) if t == Deposit::SIGNATURE_HASH => "Deposit",
                        Some(t) if t == Withdrawal::SIGNATURE_HASH => "Withdrawal",
                        Some(t) if t == Lock::SIGNATURE_HASH => "Lock",
                        Some(t) if t == Unlock::SIGNATURE_HASH => "Unlock",
                        Some(t) if t == TransferFrom::SIGNATURE_HASH => "TransferFrom",
                        Some(t) if t == TransferLockedFrom::SIGNATURE_HASH => "TransferLockedFrom",
                        _ => "Unknown",
                    };

                    self.processor.process_log(log.clone()).await?;
                    let log_us = log_start.elapsed().as_micros();

                    debug!(
                        event_type = event_type,
                        block = block_num,
                        log_index = log_idx,
                        batch_received_at = batch_received_at,
                        process_us = log_us,
                        "Event processed (historical batch)"
                    );
                }

                // Check if new pools were discovered in this batch
                let orderbooks_after = self.processor.store().pools.get_all_orderbook_addresses();
                let new_orderbooks: Vec<_> = orderbooks_after
                    .into_iter()
                    .filter(|addr| !orderbooks_before.contains(addr))
                    .collect();

                // Process any additional logs from pools discovered in earlier batches
                let mut additional_events = 0usize;
                if let Some(earlier_pool_logs) = additional_logs_for_batch.remove(&batch_idx) {
                    let count = earlier_pool_logs.len();
                    if count > 0 {
                        info!(
                            batch = batch_idx + 1,
                            events = count,
                            "Processing additional orderbook events from pools discovered in earlier batches"
                        );
                        for log in earlier_pool_logs {
                            self.processor.process_log(log).await?;
                        }
                        additional_events += count;
                    }
                }

                // If new pools discovered, fetch their orderbook events for this batch range AND all subsequent batches
                if !new_orderbooks.is_empty() {
                    info!(
                        batch = batch_idx + 1,
                        new_pools = new_orderbooks.len(),
                        from = from,
                        to = to,
                        remaining_batches = window_end - batch_idx - 1,
                        "New pools discovered, fetching orderbook events for this and subsequent batches"
                    );

                    // Fetch orderbook events for THIS batch (with retry/split on errors)
                    let new_pool_logs = Self::fetch_orderbook_events_with_retry(
                        &self.provider,
                        &new_orderbooks,
                        from,
                        to,
                        self.config.sync.retry_attempts,
                        self.config.sync.retry_delay_ms,
                        0, // depth
                    ).await?;

                    additional_events += new_pool_logs.len();
                    for log in new_pool_logs {
                        self.processor.process_log(log).await?;
                    }

                    // Fetch orderbook events for ALL subsequent batches in this window IN PARALLEL
                    // These batches were already fetched without knowing about the new pools
                    let subsequent_batches: Vec<(usize, u64, u64)> = ((batch_idx + 1)..window_end)
                        .filter_map(|idx| {
                            fetched_window.get(&idx).map(|(from, to, _)| (idx, *from, *to))
                        })
                        .collect();

                    if !subsequent_batches.is_empty() {
                        let num_subsequent = subsequent_batches.len();
                        info!(
                            batches = num_subsequent,
                            new_pools = new_orderbooks.len(),
                            "Re-fetching orderbook events for batches that missed new pools (parallel)"
                        );

                        // Use the same concurrency as the main fetch
                        let refetch_concurrency = self.concurrency_controller.get();
                        let provider = Arc::clone(&self.provider);
                        let new_orderbooks_arc = Arc::new(new_orderbooks.clone());
                        let total_subsequent = num_subsequent;
                        let max_retries = self.config.sync.retry_attempts;
                        let retry_delay_ms = self.config.sync.retry_delay_ms;

                        let refetch_results: Vec<(usize, Vec<Log>)> = stream::iter(subsequent_batches)
                            .map(|(idx, sub_from, sub_to)| {
                                let provider = Arc::clone(&provider);
                                let orderbooks = Arc::clone(&new_orderbooks_arc);
                                async move {
                                    info!(
                                        batch = idx + 1,
                                        total = total_subsequent,
                                        blocks = format!("{}-{}", sub_from, sub_to),
                                        block_count = sub_to - sub_from + 1,
                                        new_pools = orderbooks.len(),
                                        "Re-fetching orderbook events (with retry/split)"
                                    );

                                    let fetch_start = std::time::Instant::now();
                                    let result = Self::fetch_orderbook_events_with_retry(
                                        &provider,
                                        &orderbooks,
                                        sub_from,
                                        sub_to,
                                        max_retries,
                                        retry_delay_ms,
                                        0, // depth
                                    ).await;
                                    let fetch_ms = fetch_start.elapsed().as_millis();

                                    match result {
                                        Ok(logs) => {
                                            let event_counts = EventCounts::from_logs(&logs);
                                            info!(
                                                batch = idx + 1,
                                                total = total_subsequent,
                                                blocks = format!("{}-{}", sub_from, sub_to),
                                                fetch_ms = fetch_ms,
                                                events = logs.len(),
                                                breakdown = %event_counts.summary(),
                                                "Re-fetch batch complete"
                                            );
                                            (idx, logs)
                                        }
                                        Err(e) => {
                                            error!(
                                                batch = idx + 1,
                                                from = sub_from,
                                                to = sub_to,
                                                fetch_ms = fetch_ms,
                                                error = %e,
                                                "Failed to re-fetch orderbook events for batch after retries"
                                            );
                                            (idx, vec![])
                                        }
                                    }
                                }
                            })
                            .buffer_unordered(refetch_concurrency)
                            .collect()
                            .await;

                        // Queue the results for processing
                        for (idx, logs) in refetch_results {
                            if !logs.is_empty() {
                                additional_logs_for_batch
                                    .entry(idx)
                                    .or_insert_with(Vec::new)
                                    .extend(logs);
                            }
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
    /// For "too many logs" errors, splits the range in half and fetches recursively
    /// Returns the fetched logs on success
    async fn retry_fetch_with_backoff(
        &self,
        from: u64,
        to: u64,
        max_retries: u32,
    ) -> Result<Vec<Log>> {
        // Use the recursive implementation that handles range splitting
        self.fetch_with_split(from, to, max_retries, 0).await
    }

    /// Recursive fetch that splits range on "too many logs" errors
    fn fetch_with_split<'a>(
        &'a self,
        from: u64,
        to: u64,
        max_retries: u32,
        depth: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Log>>> + Send + 'a>> {
        Box::pin(async move {
            let mut attempts = 0u32;
            let mut delay = Duration::from_millis(self.config.sync.retry_delay_ms);
            let max_delay = Duration::from_secs(30);
            let max_depth = 10; // Prevent infinite recursion

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
                        if attempts > 1 || depth > 0 {
                            info!(from = from, to = to, blocks = to - from + 1, attempts = attempts, depth = depth, "Batch fetch succeeded");
                        }
                        self.batch_controller.report_success();
                        self.concurrency_controller.report_success();
                        return Ok(logs);
                    }
                    Err(e) => {
                        let err_msg = e.to_string().to_lowercase();
                        let is_too_many = Self::is_too_many_logs_error(&err_msg);

                        // For "too many logs" errors, split the range and fetch recursively
                        if is_too_many && from < to && depth < max_depth {
                            self.batch_controller.report_error();

                            let mid = from + (to - from) / 2;
                            warn!(
                                from = from,
                                to = to,
                                blocks = to - from + 1,
                                split_at = mid,
                                depth = depth,
                                "Too many logs - splitting range in half"
                            );

                            // Fetch first half
                            let mut logs = self.fetch_with_split(from, mid, max_retries, depth + 1).await?;

                            // Fetch second half
                            let logs2 = self.fetch_with_split(mid + 1, to, max_retries, depth + 1).await?;

                            // Combine and sort by (block_number, log_index)
                            logs.extend(logs2);
                            logs.sort_by(|a, b| {
                                let block_a = a.block_number.unwrap_or(0);
                                let block_b = b.block_number.unwrap_or(0);
                                let idx_a = a.log_index.unwrap_or(0);
                                let idx_b = b.log_index.unwrap_or(0);
                                (block_a, idx_a).cmp(&(block_b, idx_b))
                            });

                            return Ok(logs);
                        }

                        // For other retryable errors, use exponential backoff
                        if attempts < max_retries && Self::is_retryable_error(&e) {
                            if Self::is_rate_limit_error(&err_msg) || Self::is_connection_error(&err_msg) {
                                self.concurrency_controller.report_error();
                            }

                            let actual_delay = if Self::is_rate_limit_error(&err_msg) {
                                let retry_secs = Self::parse_retry_seconds(&err_msg).unwrap_or(10);
                                Duration::from_secs(retry_secs)
                            } else {
                                delay
                            };

                            warn!(
                                from = from,
                                to = to,
                                blocks = to - from + 1,
                                attempt = attempts,
                                max_retries = max_retries,
                                delay_ms = actual_delay.as_millis(),
                                error = %e,
                                "Batch fetch failed, retrying with backoff"
                            );

                            tokio::time::sleep(actual_delay).await;
                            delay = (delay * 2).min(max_delay);
                        } else {
                            error!(from = from, to = to, blocks = to - from + 1, attempts = attempts, error = %e, "Batch fetch failed after max retries");
                            return Err(e);
                        }
                    }
                }
            }
        })
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
        // Error messages
        msg.contains("too many logs")
            || msg.contains("exceeds max results")
            || msg.contains("query returned more than")
            || msg.contains("log response size exceeded")
            // Error codes
            || msg.contains("-32005")
            || msg.contains("-32602")
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
            // hyper/reqwest specific errors
            || msg.contains("incompletemessage")
            || msg.contains("incomplete message")
            || msg.contains("sendrequest")
            || msg.contains("send request")
            || msg.contains("hyper")
            || msg.contains("reqwest")
            || msg.contains("transport")
            || msg.contains("closed")
            || msg.contains("aborted")
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
        let batch_received_at = now_micros();
        for log in &all_logs {
            let log_start = Instant::now();
            let block_num = log.block_number.unwrap_or_default();
            let log_idx = log.log_index.unwrap_or_default();
            let topic0 = log.topics().first().cloned();

            // Identify event type for logging
            let event_type = match topic0 {
                Some(t) if t == OrderPlaced::SIGNATURE_HASH => "OrderPlaced",
                Some(t) if t == OrderMatched::SIGNATURE_HASH => "OrderMatched",
                Some(t) if t == UpdateOrder::SIGNATURE_HASH => "UpdateOrder",
                Some(t) if t == OrderCancelled::SIGNATURE_HASH => "OrderCancelled",
                Some(t) if t == PoolCreated::SIGNATURE_HASH => "PoolCreated",
                Some(t) if t == Deposit::SIGNATURE_HASH => "Deposit",
                Some(t) if t == Withdrawal::SIGNATURE_HASH => "Withdrawal",
                Some(t) if t == Lock::SIGNATURE_HASH => "Lock",
                Some(t) if t == Unlock::SIGNATURE_HASH => "Unlock",
                Some(t) if t == TransferFrom::SIGNATURE_HASH => "TransferFrom",
                Some(t) if t == TransferLockedFrom::SIGNATURE_HASH => "TransferLockedFrom",
                _ => "Unknown",
            };

            processor.process_log(log.clone()).await?;
            let log_us = log_start.elapsed().as_micros();

            debug!(
                event_type = event_type,
                block = block_num,
                log_index = log_idx,
                batch_received_at = batch_received_at,
                process_us = log_us,
                "Event processed (historical static)"
            );
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
                "Discovered new pools, re-fetching orderbook events (1 RPC call)"
            );

            // Re-fetch orderbook events for newly discovered pools (1 RPC call with 4 topics)
            let additional_logs = Self::fetch_orderbook_events_for_pools(
                provider,
                &new_pools,
                from_block,
                to_block,
            ).await?;

            additional_events = additional_logs.len();
            for log in additional_logs {
                processor.process_log(log).await?;
            }

            if additional_events > 0 {
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

    /// Build JSON-RPC request body for logging
    fn build_eth_get_logs_request_body(
        addresses: &[alloy::primitives::Address],
        topics: &[alloy::primitives::FixedBytes<32>],
        from_block: u64,
        to_block: u64,
    ) -> String {
        let addresses_json: Vec<String> = addresses.iter().map(|a| format!("{:?}", a)).collect();
        let topics_json: Vec<String> = topics.iter().map(|t| format!("{:?}", t)).collect();

        serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "address": if addresses_json.len() == 1 {
                    serde_json::Value::String(addresses_json[0].clone())
                } else {
                    serde_json::Value::Array(addresses_json.into_iter().map(serde_json::Value::String).collect())
                },
                "topics": [topics_json],
                "fromBlock": format!("0x{:x}", from_block),
                "toBlock": format!("0x{:x}", to_block)
            }],
            "id": 1
        }).to_string()
    }

    /// Fetch all events with a single RPC call using combined addresses and topics
    async fn fetch_all_events_parallel(
        provider: &ProviderManager,
        config: &IndexerConfig,
        orderbook_addresses: &[alloy::primitives::Address],
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>> {
        // Combine all addresses: PoolManager + BalanceManager + all OrderBooks
        let mut all_addresses = vec![config.pool_manager, config.balance_manager];
        all_addresses.extend_from_slice(orderbook_addresses);

        // Combine all 11 event signatures
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
            .address(all_addresses.clone())
            .event_signature(all_topics.clone())
            .from_block(from_block)
            .to_block(to_block);

        // Build request body for logging
        let request_body = Self::build_eth_get_logs_request_body(
            &all_addresses,
            &all_topics,
            from_block,
            to_block,
        );

        debug!(
            from = from_block,
            to = to_block,
            blocks = to_block - from_block + 1,
            addresses = all_addresses.len(),
            topics = all_topics.len(),
            request_body = %request_body,
            "RPC Request: eth_getLogs (combined)"
        );

        let request_start = std::time::Instant::now();
        let result = provider.http().get_logs(&filter).await;
        let request_ms = request_start.elapsed().as_millis();

        match result {
            Ok(mut logs) => {
                debug!(
                    from = from_block,
                    to = to_block,
                    count = logs.len(),
                    request_ms = request_ms,
                    "RPC Response: eth_getLogs (combined) success"
                );

                // Sort by (block_number, log_index) to ensure proper ordering
                logs.sort_by(|a, b| {
                    let block_a = a.block_number.unwrap_or(0);
                    let block_b = b.block_number.unwrap_or(0);
                    let log_idx_a = a.log_index.unwrap_or(0);
                    let log_idx_b = b.log_index.unwrap_or(0);
                    (block_a, log_idx_a).cmp(&(block_b, log_idx_b))
                });

                Ok(logs)
            }
            Err(e) => {
                error!(
                    from = from_block,
                    to = to_block,
                    blocks = to_block - from_block + 1,
                    request_ms = request_ms,
                    request_body = %request_body,
                    error = %e,
                    error_debug = ?e,
                    "RPC Error: eth_getLogs (combined) failed"
                );
                Err(IndexerError::Rpc(format!("{:?}", e)))
            }
        }
    }

    /// Fetch orderbook events for newly discovered pools with a single RPC call
    async fn fetch_orderbook_events_for_pools(
        provider: &ProviderManager,
        orderbook_addresses: &[alloy::primitives::Address],
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>> {
        if orderbook_addresses.is_empty() {
            return Ok(vec![]);
        }

        // 4 orderbook event signatures
        let orderbook_topics = vec![
            OrderPlaced::SIGNATURE_HASH,
            OrderMatched::SIGNATURE_HASH,
            UpdateOrder::SIGNATURE_HASH,
            OrderCancelled::SIGNATURE_HASH,
        ];

        let filter = Filter::new()
            .address(orderbook_addresses.to_vec())
            .event_signature(orderbook_topics.clone())
            .from_block(from_block)
            .to_block(to_block);

        let request_body = Self::build_eth_get_logs_request_body(
            orderbook_addresses,
            &orderbook_topics,
            from_block,
            to_block,
        );

        debug!(
            from = from_block,
            to = to_block,
            addresses = orderbook_addresses.len(),
            request_body = %request_body,
            "RPC Request: eth_getLogs (new pools orderbook events)"
        );

        let request_start = std::time::Instant::now();
        let result = provider.http().get_logs(&filter).await;
        let request_ms = request_start.elapsed().as_millis();

        match result {
            Ok(mut logs) => {
                debug!(
                    from = from_block,
                    to = to_block,
                    count = logs.len(),
                    request_ms = request_ms,
                    "RPC Response: eth_getLogs (new pools) success"
                );

                // Sort by (block_number, log_index)
                logs.sort_by(|a, b| {
                    let block_a = a.block_number.unwrap_or(0);
                    let block_b = b.block_number.unwrap_or(0);
                    let log_idx_a = a.log_index.unwrap_or(0);
                    let log_idx_b = b.log_index.unwrap_or(0);
                    (block_a, log_idx_a).cmp(&(block_b, log_idx_b))
                });

                Ok(logs)
            }
            Err(e) => {
                error!(
                    from = from_block,
                    to = to_block,
                    request_ms = request_ms,
                    request_body = %request_body,
                    error = %e,
                    error_debug = ?e,
                    "RPC Error: eth_getLogs (new pools) failed"
                );
                Err(IndexerError::Rpc(format!("{:?}", e)))
            }
        }
    }

    /// Fetch orderbook events with retry and range splitting on "too many logs" errors
    /// Similar to fetch_with_split but for orderbook-only fetches
    fn fetch_orderbook_events_with_retry<'a>(
        provider: &'a ProviderManager,
        orderbook_addresses: &'a [alloy::primitives::Address],
        from: u64,
        to: u64,
        max_retries: u32,
        retry_delay_ms: u64,
        depth: u32,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Log>>> + Send + 'a>> {
        Box::pin(async move {
            let mut attempts = 0u32;
            let mut delay = Duration::from_millis(retry_delay_ms);
            let max_delay = Duration::from_secs(30);
            let max_depth = 10; // Prevent infinite recursion

            loop {
                attempts += 1;

                let fetch_start = std::time::Instant::now();
                let result = Self::fetch_orderbook_events_for_pools(
                    provider,
                    orderbook_addresses,
                    from,
                    to,
                ).await;
                let fetch_ms = fetch_start.elapsed().as_millis();

                match result {
                    Ok(logs) => {
                        if attempts > 1 || depth > 0 {
                            info!(
                                from = from,
                                to = to,
                                blocks = to - from + 1,
                                attempts = attempts,
                                depth = depth,
                                events = logs.len(),
                                "Orderbook re-fetch succeeded after retry"
                            );
                        }
                        return Ok(logs);
                    }
                    Err(e) => {
                        let err_msg = e.to_string().to_lowercase();
                        let is_too_many = Self::is_too_many_logs_error(&err_msg);

                        // For "too many logs" errors, split the range and fetch recursively
                        if is_too_many && from < to && depth < max_depth {
                            let mid = from + (to - from) / 2;
                            warn!(
                                from = from,
                                to = to,
                                blocks = to - from + 1,
                                split_at = mid,
                                depth = depth,
                                fetch_ms = fetch_ms,
                                "Too many logs in orderbook re-fetch - splitting range in half"
                            );

                            // Fetch first half
                            let mut logs = Self::fetch_orderbook_events_with_retry(
                                provider,
                                orderbook_addresses,
                                from,
                                mid,
                                max_retries,
                                retry_delay_ms,
                                depth + 1,
                            ).await?;

                            // Fetch second half
                            let logs2 = Self::fetch_orderbook_events_with_retry(
                                provider,
                                orderbook_addresses,
                                mid + 1,
                                to,
                                max_retries,
                                retry_delay_ms,
                                depth + 1,
                            ).await?;

                            // Combine and sort by (block_number, log_index)
                            logs.extend(logs2);
                            logs.sort_by(|a, b| {
                                let block_a = a.block_number.unwrap_or(0);
                                let block_b = b.block_number.unwrap_or(0);
                                let idx_a = a.log_index.unwrap_or(0);
                                let idx_b = b.log_index.unwrap_or(0);
                                (block_a, idx_a).cmp(&(block_b, idx_b))
                            });

                            return Ok(logs);
                        }

                        // For other retryable errors, use exponential backoff
                        if attempts < max_retries && Self::is_retryable_error(&e) {
                            let actual_delay = if Self::is_rate_limit_error(&err_msg) {
                                let retry_secs = Self::parse_retry_seconds(&err_msg).unwrap_or(10);
                                Duration::from_secs(retry_secs)
                            } else {
                                delay
                            };

                            warn!(
                                from = from,
                                to = to,
                                blocks = to - from + 1,
                                attempt = attempts,
                                max_retries = max_retries,
                                delay_ms = actual_delay.as_millis(),
                                fetch_ms = fetch_ms,
                                error = %e,
                                "Orderbook re-fetch failed, retrying with backoff"
                            );

                            tokio::time::sleep(actual_delay).await;
                            delay = (delay * 2).min(max_delay);
                        } else {
                            error!(
                                from = from,
                                to = to,
                                blocks = to - from + 1,
                                attempts = attempts,
                                fetch_ms = fetch_ms,
                                error = %e,
                                "Orderbook re-fetch failed after max retries"
                            );
                            return Err(e);
                        }
                    }
                }
            }
        })
    }
}
