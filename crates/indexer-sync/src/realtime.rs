use alloy::rpc::types::Filter;
use alloy_sol_types::SolEvent;
use futures_util::{SinkExt, StreamExt};
use indexer_core::events::{
    Deposit, Lock, OrderCancelled, OrderMatched, OrderPlaced, PoolCreated, TransferFrom,
    TransferLockedFrom, Unlock, UpdateOrder, Withdrawal,
};
use indexer_core::types::now_micros;
use indexer_core::{IndexerConfig, IndexerError, Result};
use indexer_processor::EventProcessor;
use indexer_store::EventId;
use serde::Deserialize;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, trace, warn};

use crate::provider::ProviderManager;

/// MegaETH miniblock structure for WebSocket subscription
/// Fields match the actual miniBlocks WebSocket response (snake_case)
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // Fields are used for deserialization
pub struct MiniBlock {
    /// Block number of the EVM block this mini-block belongs to
    pub block_number: u64,
    /// Block timestamp (seconds)
    #[serde(default)]
    pub block_timestamp: u64,
    /// Index of this mini-block in the EVM block
    #[serde(default)]
    pub index: u32,
    /// Miniblock number
    #[serde(default)]
    pub number: u64,
    /// Timestamp when mini-block was created (Unix timestamp in milliseconds)
    #[serde(default)]
    pub timestamp: u64,
    /// Gas used inside this mini-block
    #[serde(default)]
    pub gas_used: u64,
    /// Transactions in this mini-block (stored as raw JSON to avoid type parsing issues)
    #[serde(default)]
    pub transactions: Vec<serde_json::Value>,
    /// Receipts - using custom struct to handle non-standard transaction types
    #[serde(default)]
    pub receipts: Vec<MiniBlockReceipt>,
    /// Buckets to canonicalize
    #[serde(default)]
    pub buckets_to_canonicalize: Vec<serde_json::Value>,
}

/// Receipt structure that only extracts what we need (logs)
/// This avoids issues with non-standard transaction types like 0x7e
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MiniBlockReceipt {
    #[serde(default)]
    pub logs: Vec<alloy::rpc::types::Log>,
}

impl MiniBlock {
    /// Extract all logs from this mini block
    pub fn extract_logs(&self) -> Vec<alloy::rpc::types::Log> {
        self.receipts
            .iter()
            .flat_map(|receipt| receipt.logs.clone())
            .collect()
    }

    /// Get total log count in this mini block
    pub fn log_count(&self) -> usize {
        self.receipts.iter().map(|r| r.logs.len()).sum()
    }

    /// Get total transaction count in this mini block
    pub fn tx_count(&self) -> usize {
        self.receipts.len()
    }
}

/// Reconnection configuration for WebSocket
struct ReconnectConfig {
    initial_delay_ms: u64,
    max_delay_ms: u64,
    max_attempts: u32, // 0 = unlimited
}

impl ReconnectConfig {
    fn from_env() -> Self {
        Self {
            initial_delay_ms: std::env::var("WS_RECONNECT_INITIAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(500), // Start with 500ms for faster recovery
            max_delay_ms: std::env::var("WS_RECONNECT_MAX_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(15000), // Cap at 15 seconds
            max_attempts: std::env::var("WS_RECONNECT_MAX_ATTEMPTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0), // 0 = unlimited
        }
    }
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

/// Real-time syncer using MegaETH WebSocket logs subscription
pub struct RealtimeSyncer {
    config: IndexerConfig,
    provider: Arc<ProviderManager>,
    processor: Arc<EventProcessor>,
    /// The verified block from historical sync - real-time starts from here
    start_from_block: u64,
    last_processed_block: AtomicU64,
    #[allow(dead_code)]
    last_miniblock_index: AtomicU32,
}

impl RealtimeSyncer {
    pub fn new(
        config: IndexerConfig,
        provider: Arc<ProviderManager>,
        processor: Arc<EventProcessor>,
        start_from_block: u64,
    ) -> Self {
        Self {
            config,
            provider,
            processor,
            start_from_block,
            last_processed_block: AtomicU64::new(start_from_block),
            last_miniblock_index: AtomicU32::new(0),
        }
    }

    /// Calculate reconnect delay with exponential backoff + jitter
    fn get_reconnect_delay(&self, attempts: u32) -> Duration {
        let config = ReconnectConfig::default();
        let base_delay = (config.initial_delay_ms as f64 * 2_f64.powi(attempts as i32))
            .min(config.max_delay_ms as f64);
        // Use simple jitter based on current time (no rand crate needed)
        let jitter_factor = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as f64)
            / 4_294_967_295.0;
        let jitter = base_delay * 0.2 * jitter_factor;
        Duration::from_millis((base_delay + jitter) as u64)
    }

    /// Record an event identifier in the verification buffer
    /// Called after process_log() to track what WebSocket has received
    fn record_event_for_verification(&self, log: &alloy::rpc::types::Log) {
        if let (Some(tx_hash), Some(log_index), Some(block_number)) =
            (log.transaction_hash, log.log_index, log.block_number)
        {
            let event_id = EventId::new(tx_hash, log_index as u64);
            self.processor
                .store()
                .ws_event_buffer
                .record_event(block_number, event_id);
        }
    }

    /// Run the real-time syncer with WebSocket and automatic reconnection
    pub async fn run(&mut self, _gap_tx: mpsc::Sender<u64>) -> Result<()> {
        info!(
            start_from_block = self.start_from_block,
            "Starting real-time sync from verified block"
        );

        // Verify sync state matches our start block
        {
            let state = self.processor.store().sync_state.read().await;
            if state.last_synced_block != self.start_from_block {
                warn!(
                    sync_state_block = state.last_synced_block,
                    start_from_block = self.start_from_block,
                    "Sync state mismatch - using start_from_block"
                );
                drop(state);
                let mut state = self.processor.store().sync_state.write().await;
                state.set_last_synced_block(self.start_from_block);
            }
        }

        // Initialize verification buffer start block for RPC verification
        self.processor
            .store()
            .ws_event_buffer
            .set_verification_start(self.start_from_block);

        let mut reconnect_attempts = 0u32;
        let config = ReconnectConfig::from_env();

        info!(
            initial_delay_ms = config.initial_delay_ms,
            max_delay_ms = config.max_delay_ms,
            max_attempts = config.max_attempts,
            "WebSocket reconnection config loaded"
        );

        loop {
            info!(
                attempt = reconnect_attempts + 1,
                start_block = self.start_from_block,
                "Attempting WebSocket connection"
            );

            match self.run_websocket_mode().await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if config.max_attempts > 0 && reconnect_attempts >= config.max_attempts {
                        error!(
                            attempts = reconnect_attempts,
                            "Max reconnection attempts reached"
                        );
                        return Err(e);
                    }

                    let delay = self.get_reconnect_delay(reconnect_attempts);
                    warn!(
                        error = %e,
                        attempt = reconnect_attempts + 1,
                        delay_ms = delay.as_millis(),
                        "WebSocket disconnected, will reconnect"
                    );

                    // Recover any missed blocks during disconnection
                    info!("Recovering missed blocks before reconnect...");
                    if let Err(gap_err) = self.recover_missed_blocks().await {
                        error!(error = %gap_err, "Failed to recover missed blocks");
                    }

                    info!(
                        delay_ms = delay.as_millis(),
                        attempt = reconnect_attempts + 1,
                        "Waiting before reconnection"
                    );
                    tokio::time::sleep(delay).await;
                    reconnect_attempts += 1;
                }
            }
        }
    }

    /// Get all event signature topics for subscription
    /// We subscribe by topics (not addresses) to automatically capture events from new pools
    fn get_event_topics(&self) -> Vec<String> {
        let topics = vec![
            format!("{:?}", PoolCreated::SIGNATURE_HASH),
            format!("{:?}", OrderPlaced::SIGNATURE_HASH),
            format!("{:?}", OrderMatched::SIGNATURE_HASH),
            format!("{:?}", UpdateOrder::SIGNATURE_HASH),
            format!("{:?}", OrderCancelled::SIGNATURE_HASH),
            format!("{:?}", Deposit::SIGNATURE_HASH),
            format!("{:?}", Withdrawal::SIGNATURE_HASH),
            format!("{:?}", Lock::SIGNATURE_HASH),
            format!("{:?}", Unlock::SIGNATURE_HASH),
            format!("{:?}", TransferFrom::SIGNATURE_HASH),
            format!("{:?}", TransferLockedFrom::SIGNATURE_HASH),
        ];
        debug!(
            topic_count = topics.len(),
            "Prepared event topics for subscription"
        );
        topics
    }

    /// Check if a log is from one of our contracts (client-side filtering)
    fn is_relevant_log(&self, log: &alloy::rpc::types::Log) -> bool {
        let address = log.address();
        let topic0 = log.topics().first();

        match topic0 {
            // PoolCreated - only from PoolManager
            Some(t) if *t == PoolCreated::SIGNATURE_HASH => address == self.config.pool_manager,
            // Balance events - only from BalanceManager
            Some(t)
                if *t == Deposit::SIGNATURE_HASH
                    || *t == Withdrawal::SIGNATURE_HASH
                    || *t == Lock::SIGNATURE_HASH
                    || *t == Unlock::SIGNATURE_HASH
                    || *t == TransferFrom::SIGNATURE_HASH
                    || *t == TransferLockedFrom::SIGNATURE_HASH =>
            {
                address == self.config.balance_manager
            }
            // Orderbook events - from any known orderbook (including newly discovered)
            Some(t)
                if *t == OrderPlaced::SIGNATURE_HASH
                    || *t == OrderMatched::SIGNATURE_HASH
                    || *t == UpdateOrder::SIGNATURE_HASH
                    || *t == OrderCancelled::SIGNATURE_HASH =>
            {
                self.processor.store().pools.is_known_orderbook(&address)
            }
            _ => false,
        }
    }

    /// Identify event type from topic0 signature for logging
    fn identify_event_type(&self, topic0: Option<alloy::primitives::B256>) -> &'static str {
        match topic0 {
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
        }
    }

    /// WebSocket connection and subscription
    /// Uses miniBlocks subscription on MegaETH, logs subscription on other chains
    async fn run_websocket_mode(&mut self) -> Result<()> {
        let ws_url = &self.config.ws_url;
        info!(
            url = %ws_url,
            miniblocks_enabled = self.config.miniblocks_enabled,
            "Connecting to WebSocket for real-time sync"
        );

        let (ws_stream, response) = connect_async(ws_url)
            .await
            .map_err(|e| IndexerError::Rpc(format!("WebSocket connect: {}", e)))?;

        debug!(
            status = ?response.status(),
            "WebSocket connection established"
        );

        let (mut write, mut read) = ws_stream.split();

        // Build subscription message based on mode
        let subscribe_msg = if self.config.miniblocks_enabled {
            // MegaETH: Subscribe to miniBlocks stream
            info!(
                pool_manager = ?self.config.pool_manager,
                balance_manager = ?self.config.balance_manager,
                known_orderbooks = self.processor.store().pools.count(),
                "Subscribing to miniBlocks stream (MegaETH mode)"
            );
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_subscribe",
                "params": ["miniBlocks"],
                "id": 1
            })
        } else {
            // Standard EVM: Subscribe to logs with topic filtering
            let topics = self.get_event_topics();
            info!(
                topic_count = topics.len(),
                pool_manager = ?self.config.pool_manager,
                balance_manager = ?self.config.balance_manager,
                known_orderbooks = self.processor.store().pools.count(),
                "Subscribing to logs by event topics (standard EVM mode)"
            );
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_subscribe",
                "params": ["logs", {
                    "topics": [topics],
                    "fromBlock": "pending",
                    "toBlock": "pending"
                }],
                "id": 1
            })
        };

        debug!("Sending subscription request");
        write
            .send(Message::Text(subscribe_msg.to_string()))
            .await
            .map_err(|e| IndexerError::Rpc(format!("WebSocket send: {}", e)))?;

        // Wait for subscription confirmation
        let subscription_id = self.wait_for_subscription(&mut read).await?;
        info!(
            subscription_id = %subscription_id,
            miniblocks_enabled = self.config.miniblocks_enabled,
            "Successfully subscribed - now receiving real-time events"
        );

        // NOTE: We do NOT check for gaps at subscription time because RPC might be delayed.
        // Instead, we detect gaps on the FIRST WebSocket event (see first_event_gap_checked below).

        // Start keepalive task
        let keepalive_write = Arc::new(tokio::sync::Mutex::new(write));
        let keepalive_handle = self.spawn_keepalive(Arc::clone(&keepalive_write));
        debug!("Keepalive task started (eth_chainId every 30s)");

        // Process incoming messages
        let mut last_block_number = self.start_from_block;
        let mut miniblocks_received: u64 = 0;
        let mut logs_extracted: u64 = 0;
        let mut logs_relevant: u64 = 0;
        let mut logs_filtered: u64 = 0;
        let mut messages_total: u64 = 0;
        let mut last_message_time = Instant::now();

        // Flag to track if we've checked for gaps on the first WebSocket event
        // We detect gaps here (not at subscription time) because RPC might be delayed
        let mut first_event_gap_checked = false;

        info!(
            start_block = self.start_from_block,
            miniblocks_enabled = self.config.miniblocks_enabled,
            "Starting to process real-time WebSocket events"
        );

        while let Some(msg) = read.next().await {
            messages_total += 1;
            let since_last_msg_ms = last_message_time.elapsed().as_millis();
            last_message_time = Instant::now();

            match msg {
                Ok(Message::Text(text)) => {
                    // Log raw message length for debugging
                    trace!(
                        msg_len = text.len(),
                        messages_total = messages_total,
                        since_last_ms = since_last_msg_ms,
                        "WebSocket text message received"
                    );

                    let parsed: serde_json::Value = serde_json::from_str(&text)
                        .map_err(|e| IndexerError::Sync(format!("JSON parse: {}", e)))?;

                    // Check if it's a keepalive response (id >= 1000)
                    if let Some(id) = parsed.get("id").and_then(|v| v.as_u64()) {
                        if id >= 1000 {
                            debug!(
                                id = id,
                                messages_total = messages_total,
                                "Keepalive response received"
                            );
                            continue;
                        }
                    }

                    // Log the method for any non-keepalive message
                    let method = parsed.get("method").and_then(|v| v.as_str()).unwrap_or("unknown");
                    debug!(
                        method = method,
                        messages_total = messages_total,
                        since_last_ms = since_last_msg_ms,
                        "WebSocket message received"
                    );

                    // Check if it's a subscription notification
                    if parsed.get("method") == Some(&serde_json::json!("eth_subscription")) {
                        if let Some(result) = parsed.pointer("/params/result") {
                            let received_at = now_micros();

                            if self.config.miniblocks_enabled {
                                // ===== MINIBLOCKS MODE (MegaETH) =====
                                let mini_block: MiniBlock = match serde_json::from_value(result.clone()) {
                                    Ok(mb) => mb,
                                    Err(e) => {
                                        warn!(error = %e, "Failed to parse miniBlock, skipping");
                                        continue;
                                    }
                                };
                                miniblocks_received += 1;

                                let block_number = mini_block.block_number;
                                let miniblock_index = mini_block.index;
                                let tx_count = mini_block.tx_count();
                                let log_count = mini_block.log_count();

                                // ALWAYS update block number from every mini block
                                if block_number > last_block_number {
                                    last_block_number = block_number;
                                    self.last_processed_block.store(block_number, Ordering::Relaxed);

                                    // Update sync state for every new block
                                    let mut state = self.processor.store().sync_state.write().await;
                                    state.set_last_synced_block(block_number);
                                }
                                self.last_miniblock_index.store(miniblock_index, Ordering::Relaxed);

                                // Log mini block reception (throttled to avoid log spam)
                                if miniblocks_received % 10 == 1 || log_count > 0 {
                                    debug!(
                                        block = block_number,
                                        miniblock_idx = miniblock_index,
                                        tx_count = tx_count,
                                        log_count = log_count,
                                        miniblocks_received = miniblocks_received,
                                        "MiniBlock received"
                                    );
                                }

                                // Skip if no logs to process
                                if log_count == 0 {
                                    continue;
                                }

                                // Extract all logs with full metadata
                                let logs = mini_block.extract_logs();
                                logs_extracted += logs.len() as u64;

                                // GAP DETECTION: Check for gaps on first relevant event
                                // We use the first log's block_number and log_index
                                if !first_event_gap_checked {
                                    // Find first relevant log to get its log_index
                                    if let Some(first_relevant_log) = logs.iter().find(|l| self.is_relevant_log(l)) {
                                        first_event_gap_checked = true;
                                        let first_ws_block = block_number;
                                        let first_ws_log_index = first_relevant_log.log_index.unwrap_or(0);

                                        // Use last_processed_block for reconnection, falls back to start_from_block
                                        let last_synced = self.last_processed_block.load(Ordering::Relaxed);
                                        let last_synced = if last_synced > 0 { last_synced } else { self.start_from_block };

                                        if first_ws_block > last_synced {
                                            let gap_from = last_synced + 1;
                                            let gap_to = first_ws_block;

                                            info!(
                                                last_synced = last_synced,
                                                first_ws_block = first_ws_block,
                                                first_ws_log_index = first_ws_log_index,
                                                gap_from = gap_from,
                                                gap_to = gap_to,
                                                "Gap detected on first WebSocket event"
                                            );

                                            // Spawn background task to wait for RPC and fill gap
                                            self.spawn_wait_and_fill_gap(gap_from, gap_to, first_ws_block, first_ws_log_index);
                                        } else if first_ws_block == last_synced && first_ws_log_index > 0 {
                                            // Same block but might have missed earlier logs
                                            info!(
                                                last_synced = last_synced,
                                                first_ws_block = first_ws_block,
                                                first_ws_log_index = first_ws_log_index,
                                                "Same block - checking for missed logs before log_index"
                                            );
                                            self.spawn_wait_and_fill_gap(first_ws_block, first_ws_block, first_ws_block, first_ws_log_index);
                                        } else {
                                            info!(
                                                last_synced = last_synced,
                                                first_ws_block = first_ws_block,
                                                first_ws_log_index = first_ws_log_index,
                                                "No gap - WebSocket continues from expected block"
                                            );
                                        }
                                    }
                                }

                                // Filter and process relevant logs
                                for log in logs {
                                    if !self.is_relevant_log(&log) {
                                        logs_filtered += 1;
                                        continue;
                                    }

                                    logs_relevant += 1;
                                    let process_start = Instant::now();

                                    // Identify event type for logging
                                    let topic0 = log.topics().first().cloned();
                                    let event_type = self.identify_event_type(topic0);

                                    info!(
                                        event_type = event_type,
                                        block = block_number,
                                        tx_hash = ?log.transaction_hash,
                                        log_index = log.log_index,
                                        "Processing event from miniBlock"
                                    );

                                    // Process through existing pipeline
                                    self.processor.process_log(log.clone()).await?;

                                    // Record event ID for RPC verification (tracking only)
                                    self.record_event_for_verification(&log);

                                    let process_us = process_start.elapsed().as_micros();
                                    debug!(
                                        event_type = event_type,
                                        block = block_number,
                                        process_us = process_us,
                                        "Event processed (miniBlock)"
                                    );
                                }

                                // Periodic stats logging
                                if miniblocks_received % 100 == 0 {
                                    info!(
                                        miniblocks_received = miniblocks_received,
                                        logs_extracted = logs_extracted,
                                        logs_relevant = logs_relevant,
                                        logs_filtered = logs_filtered,
                                        last_block = last_block_number,
                                        "MiniBlocks sync stats"
                                    );
                                }
                            } else {
                                // ===== LOGS MODE (Standard EVM) =====
                                let log: alloy::rpc::types::Log = match serde_json::from_value(result.clone()) {
                                    Ok(l) => l,
                                    Err(e) => {
                                        warn!(error = %e, "Failed to parse log, skipping");
                                        continue;
                                    }
                                };
                                logs_extracted += 1;

                                let block_number = log.block_number.unwrap_or_default();
                                let log_index = log.log_index.unwrap_or_default();
                                let topic0 = log.topics().first().cloned();
                                let event_type = self.identify_event_type(topic0);

                                debug!(
                                    event_type = event_type,
                                    block = block_number,
                                    log_index = log_index,
                                    received_at = received_at,
                                    "Log received from WebSocket"
                                );

                                // CLIENT-SIDE FILTERING
                                if !self.is_relevant_log(&log) {
                                    logs_filtered += 1;
                                    if logs_filtered % 100 == 0 {
                                        info!(
                                            logs_extracted = logs_extracted,
                                            logs_relevant = logs_relevant,
                                            logs_filtered = logs_filtered,
                                            last_block = last_block_number,
                                            "Real-time sync stats (filtering active)"
                                        );
                                    }
                                    continue;
                                }

                                logs_relevant += 1;

                                // GAP DETECTION: Check for gaps on first relevant event
                                if !first_event_gap_checked {
                                    first_event_gap_checked = true;
                                    let first_ws_block = block_number;
                                    let first_ws_log_index = log_index;

                                    // Use last_processed_block for reconnection, falls back to start_from_block
                                    let last_synced = self.last_processed_block.load(Ordering::Relaxed);
                                    let last_synced = if last_synced > 0 { last_synced } else { self.start_from_block };

                                    if first_ws_block > last_synced {
                                        let gap_from = last_synced + 1;
                                        let gap_to = first_ws_block;

                                        info!(
                                            last_synced = last_synced,
                                            first_ws_block = first_ws_block,
                                            first_ws_log_index = first_ws_log_index,
                                            gap_from = gap_from,
                                            gap_to = gap_to,
                                            "Gap detected on first WebSocket event (logs mode)"
                                        );

                                        // Spawn background task to wait for RPC and fill gap
                                        self.spawn_wait_and_fill_gap(gap_from, gap_to, first_ws_block, first_ws_log_index);
                                    } else if first_ws_block == last_synced && first_ws_log_index > 0 {
                                        // Same block but might have missed earlier logs
                                        info!(
                                            last_synced = last_synced,
                                            first_ws_block = first_ws_block,
                                            first_ws_log_index = first_ws_log_index,
                                            "Same block - checking for missed logs before log_index (logs mode)"
                                        );
                                        self.spawn_wait_and_fill_gap(first_ws_block, first_ws_block, first_ws_block, first_ws_log_index);
                                    } else {
                                        info!(
                                            last_synced = last_synced,
                                            first_ws_block = first_ws_block,
                                            first_ws_log_index = first_ws_log_index,
                                            "No gap - WebSocket continues from expected block (logs mode)"
                                        );
                                    }
                                }

                                let process_start = Instant::now();

                                // Track block number
                                if let Some(block_num) = log.block_number {
                                    last_block_number = block_num;
                                    self.last_processed_block.store(block_num, Ordering::Relaxed);
                                }

                                info!(
                                    event_type = event_type,
                                    block = block_number,
                                    tx_hash = ?log.transaction_hash,
                                    log_index = log_index,
                                    "Processing event from logs subscription"
                                );

                                // Process the log
                                self.processor.process_log(log.clone()).await?;

                                // Record event ID for RPC verification (tracking only)
                                self.record_event_for_verification(&log);

                                let process_us = process_start.elapsed().as_micros();

                                debug!(
                                    event_type = event_type,
                                    block = block_number,
                                    process_us = process_us,
                                    "Event processed (logs)"
                                );

                                // Update sync state
                                if last_block_number > 0 {
                                    let mut state = self.processor.store().sync_state.write().await;
                                    state.set_last_synced_block(last_block_number);
                                }

                                // Periodic stats logging
                                if logs_relevant % 100 == 0 {
                                    info!(
                                        logs_extracted = logs_extracted,
                                        logs_relevant = logs_relevant,
                                        logs_filtered = logs_filtered,
                                        last_block = last_block_number,
                                        "Real-time sync stats"
                                    );
                                }
                            }
                        }
                    } else {
                        // Not a subscription message
                        debug!(
                            method = method,
                            messages_total = messages_total,
                            "Non-subscription message received"
                        );
                    }

                    // Periodic heartbeat (every 1000 messages)
                    if messages_total % 1000 == 0 {
                        info!(
                            messages_total = messages_total,
                            miniblocks_received = miniblocks_received,
                            logs_relevant = logs_relevant,
                            last_block = last_block_number,
                            "WebSocket heartbeat"
                        );
                    }
                }
                Ok(Message::Close(frame)) => {
                    warn!(
                        frame = ?frame,
                        logs_relevant = logs_relevant,
                        logs_filtered = logs_filtered,
                        "WebSocket closed by server"
                    );
                    keepalive_handle.abort();
                    return Err(IndexerError::Sync("WebSocket closed".into()));
                }
                Ok(Message::Ping(data)) => {
                    debug!(
                        messages_total = messages_total,
                        "Received ping from server, sending pong"
                    );
                    let mut write = keepalive_write.lock().await;
                    let _ = write.send(Message::Pong(data)).await;
                }
                Ok(Message::Binary(data)) => {
                    debug!(
                        len = data.len(),
                        messages_total = messages_total,
                        "Received binary message from WebSocket"
                    );
                }
                Ok(Message::Pong(_)) => {
                    trace!(
                        messages_total = messages_total,
                        "Received pong response"
                    );
                }
                Ok(Message::Frame(_)) => {
                    trace!(
                        messages_total = messages_total,
                        "Received raw frame"
                    );
                }
                Err(e) => {
                    error!(
                        error = %e,
                        logs_relevant = logs_relevant,
                        logs_filtered = logs_filtered,
                        messages_total = messages_total,
                        "WebSocket error"
                    );
                    keepalive_handle.abort();
                    return Err(IndexerError::Rpc(format!("WebSocket: {}", e)));
                }
            }
        }

        keepalive_handle.abort();
        Ok(())
    }

    /// Wait for subscription confirmation
    async fn wait_for_subscription<S>(&self, read: &mut S) -> Result<String>
    where
        S: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        let timeout = Duration::from_secs(10);
        match tokio::time::timeout(timeout, async {
            while let Some(msg) = read.next().await {
                if let Ok(Message::Text(text)) = msg {
                    let parsed: serde_json::Value = serde_json::from_str(&text)
                        .map_err(|e| IndexerError::Sync(format!("JSON parse: {}", e)))?;

                    // Check for subscription confirmation (id=1)
                    if parsed.get("id") == Some(&serde_json::json!(1)) {
                        if let Some(result) = parsed.get("result").and_then(|v| v.as_str()) {
                            return Ok(result.to_string());
                        }
                        if let Some(error) = parsed.get("error") {
                            return Err(IndexerError::Rpc(format!("Subscription error: {}", error)));
                        }
                    }
                }
            }
            Err(IndexerError::Sync(
                "WebSocket closed during subscription".into(),
            ))
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(IndexerError::Sync("Subscription timeout".into())),
        }
    }

    /// Spawn keepalive task that sends both WebSocket Ping frames and eth_chainId
    /// Ping frames are sent every 10s, eth_chainId every 30s
    fn spawn_keepalive(
        &self,
        write: Arc<
            tokio::sync::Mutex<
                futures_util::stream::SplitSink<
                    tokio_tungstenite::WebSocketStream<
                        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
                    >,
                    Message,
                >,
            >,
        >,
    ) -> tokio::task::JoinHandle<()> {
        // WebSocket ping every 10 seconds (more aggressive to prevent drops)
        let ping_interval_secs = std::env::var("WS_PING_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10u64);

        // eth_chainId every 30 seconds as backup
        let rpc_interval_secs = std::env::var("WS_RPC_KEEPALIVE_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30u64);

        info!(
            ping_interval_secs = ping_interval_secs,
            rpc_interval_secs = rpc_interval_secs,
            "Starting WebSocket keepalive task"
        );

        tokio::spawn(async move {
            let mut ping_interval = tokio::time::interval(Duration::from_secs(ping_interval_secs));
            let mut rpc_interval = tokio::time::interval(Duration::from_secs(rpc_interval_secs));
            let mut request_id = 1000u64;
            let mut ping_count = 0u64;

            loop {
                tokio::select! {
                    _ = ping_interval.tick() => {
                        ping_count += 1;
                        let ping_data = format!("ping-{}", ping_count).into_bytes();

                        debug!(
                            ping_count = ping_count,
                            "Sending WebSocket Ping frame"
                        );

                        let mut write = write.lock().await;
                        if let Err(e) = write.send(Message::Ping(ping_data)).await {
                            warn!(
                                error = %e,
                                ping_count = ping_count,
                                "WebSocket Ping send failed - connection may be closed"
                            );
                            break;
                        }
                    }
                    _ = rpc_interval.tick() => {
                        request_id += 1;

                        let msg = serde_json::json!({
                            "jsonrpc": "2.0",
                            "method": "eth_chainId",
                            "params": [],
                            "id": request_id
                        });

                        debug!(
                            request_id = request_id,
                            "Sending RPC keepalive (eth_chainId)"
                        );

                        let mut write = write.lock().await;
                        if let Err(e) = write.send(Message::Text(msg.to_string())).await {
                            warn!(
                                error = %e,
                                request_id = request_id,
                                "RPC keepalive send failed - connection may be closed"
                            );
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Spawn a background task to wait for RPC to catch up, then fill the gap with log_index filtering
    /// This is used for initial connection and reconnection gap detection
    fn spawn_wait_and_fill_gap(
        &self,
        gap_from: u64,
        gap_to: u64,
        first_ws_block: u64,
        first_ws_log_index: u64,
    ) {
        let provider = Arc::clone(&self.provider);
        let processor = Arc::clone(&self.processor);
        let config = self.config.clone();

        tokio::spawn(async move {
            // Poll RPC until it confirms it has at least gap_to block
            let poll_interval = Duration::from_millis(100);
            let max_wait = Duration::from_secs(10);
            let start = Instant::now();

            loop {
                match provider.http().get_block_number().await {
                    Ok(rpc_head) => {
                        if rpc_head >= gap_to {
                            info!(
                                rpc_head = rpc_head,
                                gap_to = gap_to,
                                waited_ms = start.elapsed().as_millis(),
                                "RPC caught up, starting gap fill"
                            );
                            break;
                        }
                        debug!(
                            rpc_head = rpc_head,
                            gap_to = gap_to,
                            "Waiting for RPC to catch up..."
                        );
                    }
                    Err(e) => {
                        warn!(error = %e, "RPC error while waiting for catchup");
                    }
                }

                if start.elapsed() > max_wait {
                    warn!(
                        waited_ms = start.elapsed().as_millis(),
                        "RPC catchup timeout, proceeding with gap fill anyway"
                    );
                    break;
                }

                tokio::time::sleep(poll_interval).await;
            }

            // Now fill the gap with log_index filtering
            info!(
                gap_from = gap_from,
                gap_to = gap_to,
                first_ws_block = first_ws_block,
                first_ws_log_index = first_ws_log_index,
                "Starting gap fill with log_index filtering"
            );

            let fill_start = Instant::now();

            // Process in batches to avoid overwhelming the RPC
            let batch_size = 100u64;
            let mut current = gap_from;
            let mut total_processed = 0usize;
            let mut total_skipped = 0usize;

            while current <= gap_to {
                let batch_end = (current + batch_size - 1).min(gap_to);

                // Phase 1: PoolCreated events (must be first to discover new pools)
                let pool_filter = Filter::new()
                    .address(config.pool_manager)
                    .event_signature(PoolCreated::SIGNATURE_HASH)
                    .from_block(current)
                    .to_block(batch_end);

                if let Ok(pool_logs) = provider.http().get_logs(&pool_filter).await {
                    for log in pool_logs {
                        let log_block = log.block_number.unwrap_or(0);
                        let log_idx = log.log_index.unwrap_or(0);

                        // Filter: skip logs from first_ws_block with log_index >= first_ws_log_index
                        if log_block == first_ws_block && log_idx >= first_ws_log_index {
                            total_skipped += 1;
                            continue;
                        }

                        if let Err(e) = processor.process_log(log).await {
                            warn!(error = %e, "Failed to process pool log during gap fill");
                        } else {
                            total_processed += 1;
                        }
                    }
                }

                // Phase 2: OrderBook events
                let orderbook_addresses = processor.store().pools.get_all_orderbook_addresses();
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
                        .from_block(current)
                        .to_block(batch_end);

                    if let Ok(logs) = provider.http().get_logs(&orderbook_filter).await {
                        for log in logs {
                            let log_block = log.block_number.unwrap_or(0);
                            let log_idx = log.log_index.unwrap_or(0);

                            // Filter: skip logs from first_ws_block with log_index >= first_ws_log_index
                            if log_block == first_ws_block && log_idx >= first_ws_log_index {
                                total_skipped += 1;
                                continue;
                            }

                            if let Err(e) = processor.process_log(log).await {
                                warn!(error = %e, "Failed to process orderbook log during gap fill");
                            } else {
                                total_processed += 1;
                            }
                        }
                    }
                }

                // Phase 3: Balance events
                let balance_sigs = vec![
                    Deposit::SIGNATURE_HASH,
                    Withdrawal::SIGNATURE_HASH,
                    Lock::SIGNATURE_HASH,
                    Unlock::SIGNATURE_HASH,
                    TransferFrom::SIGNATURE_HASH,
                    TransferLockedFrom::SIGNATURE_HASH,
                ];

                let balance_filter = Filter::new()
                    .address(config.balance_manager)
                    .event_signature(balance_sigs)
                    .from_block(current)
                    .to_block(batch_end);

                if let Ok(logs) = provider.http().get_logs(&balance_filter).await {
                    for log in logs {
                        let log_block = log.block_number.unwrap_or(0);
                        let log_idx = log.log_index.unwrap_or(0);

                        // Filter: skip logs from first_ws_block with log_index >= first_ws_log_index
                        if log_block == first_ws_block && log_idx >= first_ws_log_index {
                            total_skipped += 1;
                            continue;
                        }

                        if let Err(e) = processor.process_log(log).await {
                            warn!(error = %e, "Failed to process balance log during gap fill");
                        } else {
                            total_processed += 1;
                        }
                    }
                }

                current = batch_end + 1;
            }

            let elapsed = fill_start.elapsed();
            info!(
                gap_from = gap_from,
                gap_to = gap_to,
                first_ws_block = first_ws_block,
                first_ws_log_index = first_ws_log_index,
                events_processed = total_processed,
                events_skipped = total_skipped,
                elapsed_ms = elapsed.as_millis(),
                "Gap fill complete"
            );
        });
    }

    /// Recover missed blocks after reconnection
    async fn recover_missed_blocks(&self) -> Result<()> {
        info!("Checking for missed blocks after disconnection");

        let last_processed = self.last_processed_block.load(Ordering::Relaxed);
        if last_processed == 0 {
            debug!("No blocks processed yet, skipping gap recovery");
            return Ok(());
        }

        let current_block = self
            .provider
            .http()
            .get_block_number()
            .await
            .map_err(|e| IndexerError::Rpc(e.to_string()))?;

        debug!(
            last_processed = last_processed,
            current_block = current_block,
            "Comparing last processed block with current chain head"
        );

        if current_block > last_processed + 1 {
            let missed = current_block - last_processed - 1;
            warn!(
                last_processed = last_processed,
                current_block = current_block,
                missed_blocks = missed,
                "Gap detected after reconnection - recovering via HTTP polling"
            );

            // Fetch missed blocks via polling (reuse existing method)
            self.fetch_and_process_events(last_processed + 1, current_block)
                .await?;

            // Update sync state
            let mut state = self.processor.store().sync_state.write().await;
            state.set_last_synced_block(current_block);

            info!(
                from = last_processed + 1,
                to = current_block,
                missed_blocks = missed,
                "Gap recovery complete"
            );
        } else {
            info!("No gaps detected, all blocks are synced");
        }

        Ok(())
    }

    /// Polling mode fallback - poll for new blocks periodically (used for gap recovery)
    #[allow(dead_code)]
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
        debug!(
            from = from_block,
            to = to_block,
            blocks = to_block - from_block + 1,
            "Fetching events via HTTP polling"
        );

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
                from_block = from_block,
                to_block = to_block,
                "New pool(s) discovered during gap recovery"
            );
        }

        // Step 2: Get ALL orderbook addresses (now includes any newly discovered pools)
        let orderbook_addresses = self.processor.store().pools.get_all_orderbook_addresses();
        let orderbook_count = orderbook_addresses.len();

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

            let orderbook_events = logs.len();
            for log in logs {
                self.processor.process_log(log).await?;
            }

            if orderbook_events > 0 {
                debug!(
                    orderbook_events = orderbook_events,
                    orderbooks_queried = orderbook_count,
                    "Processed orderbook events during gap recovery"
                );
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

        let balance_events = logs.len();
        for log in logs {
            self.processor.process_log(log).await?;
        }

        if balance_events > 0 {
            debug!(
                balance_events = balance_events,
                "Processed balance events during gap recovery"
            );
        }

        debug!(
            from = from_block,
            to = to_block,
            pool_events = new_pools,
            "Gap recovery fetch complete"
        );

        Ok(())
    }
}
