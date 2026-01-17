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
use serde::Deserialize;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, trace, warn};

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

    /// WebSocket connection and subscription (topic-based, not address-based)
    async fn run_websocket_mode(&mut self) -> Result<()> {
        let ws_url = &self.config.ws_url;
        info!(url = %ws_url, "Connecting to WebSocket for real-time logs subscription");

        let (ws_stream, response) = connect_async(ws_url)
            .await
            .map_err(|e| IndexerError::Rpc(format!("WebSocket connect: {}", e)))?;

        debug!(
            status = ?response.status(),
            "WebSocket connection established"
        );

        let (mut write, mut read) = ws_stream.split();

        // Subscribe by TOPICS only (not addresses) - this way we get all events
        // matching our signatures and filter by address client-side.
        // This ensures we automatically receive events from newly created pools.
        let topics = self.get_event_topics();
        info!(
            topic_count = topics.len(),
            pool_manager = ?self.config.pool_manager,
            balance_manager = ?self.config.balance_manager,
            known_orderbooks = self.processor.store().pools.count(),
            "Subscribing to logs by event topics (topic-based, not address-based)"
        );

        let subscribe_msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_subscribe",
            "params": ["logs", {
                "topics": [topics],
                "fromBlock": "pending",
                "toBlock": "pending"
            }],
            "id": 1
        });

        debug!("Sending subscription request");
        write
            .send(Message::Text(subscribe_msg.to_string()))
            .await
            .map_err(|e| IndexerError::Rpc(format!("WebSocket send: {}", e)))?;

        // Wait for subscription confirmation
        let subscription_id = self.wait_for_subscription(&mut read).await?;
        info!(
            subscription_id = %subscription_id,
            "Successfully subscribed to logs - now receiving real-time events"
        );

        // Start keepalive task
        let keepalive_write = Arc::new(tokio::sync::Mutex::new(write));
        let keepalive_handle = self.spawn_keepalive(Arc::clone(&keepalive_write));
        debug!("Keepalive task started (eth_chainId every 30s)");

        // Process incoming messages
        let mut last_block_number = self.start_from_block;
        let mut events_received: u64 = 0;
        let mut events_processed: u64 = 0;
        let mut events_filtered: u64 = 0;
        let mut messages_total: u64 = 0;
        let mut last_message_time = Instant::now();

        info!(
            start_block = self.start_from_block,
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

                    // Check if it's a log notification
                    if parsed.get("method") == Some(&serde_json::json!("eth_subscription")) {
                        if let Some(result) = parsed.pointer("/params/result") {
                            // Capture reception timestamp immediately
                            let received_at = now_micros();
                            let process_start = Instant::now();
                            events_received += 1;

                            let log: alloy::rpc::types::Log =
                                serde_json::from_value(result.clone())
                                    .map_err(|e| IndexerError::Sync(format!("Log parse: {}", e)))?;

                            let block_number = log.block_number.unwrap_or_default();
                            let log_index = log.log_index.unwrap_or_default();
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

                            debug!(
                                event_type = event_type,
                                block = block_number,
                                log_index = log_index,
                                received_at = received_at,
                                "Event received from WebSocket"
                            );

                            // CLIENT-SIDE FILTERING: Only process logs from our contracts
                            if !self.is_relevant_log(&log) {
                                events_filtered += 1;
                                debug!(
                                    event_type = event_type,
                                    address = ?log.address(),
                                    block = block_number,
                                    log_index = log_index,
                                    events_received = events_received,
                                    events_filtered = events_filtered,
                                    "Filtered out irrelevant log (not from our contracts)"
                                );

                                // Periodic stats even for filtered events
                                if events_filtered % 100 == 0 {
                                    info!(
                                        events_received = events_received,
                                        events_processed = events_processed,
                                        events_filtered = events_filtered,
                                        last_block = last_block_number,
                                        messages_total = messages_total,
                                        "Real-time sync stats (filtering active)"
                                    );
                                }
                                continue;
                            }

                            events_processed += 1;

                            // Track block number for gap detection
                            if let Some(block_num) = log.block_number {
                                last_block_number = block_num;
                                self.last_processed_block.store(block_num, Ordering::Relaxed);
                            }

                            // Log the event being processed with timing
                            debug!(
                                event_type = event_type,
                                address = ?log.address(),
                                block = block_number,
                                tx_hash = ?log.transaction_hash,
                                log_index = log_index,
                                received_at = received_at,
                                "Processing real-time event"
                            );

                            // Process the log (this handles PoolCreated to add new orderbooks)
                            let pipeline_start = Instant::now();
                            self.processor.process_log(log).await?;
                            let pipeline_us = pipeline_start.elapsed().as_micros();
                            let total_us = process_start.elapsed().as_micros();

                            // Log completion with full timing breakdown
                            info!(
                                event_type = event_type,
                                block = block_number,
                                log_index = log_index,
                                received_at = received_at,
                                pipeline_us = pipeline_us,
                                total_us = total_us,
                                "Event processed (WebSocket)"
                            );

                            // Update sync state
                            if last_block_number > 0 {
                                let mut state =
                                    self.processor.store().sync_state.write().await;
                                state.set_last_synced_block(last_block_number);
                            }

                            // Periodic stats logging
                            if events_processed % 100 == 0 {
                                info!(
                                    received = events_received,
                                    processed = events_processed,
                                    filtered = events_filtered,
                                    last_block = last_block_number,
                                    messages_total = messages_total,
                                    "Real-time sync stats"
                                );
                            }
                        }
                    } else {
                        // Not a subscription message - log what we got
                        debug!(
                            method = method,
                            messages_total = messages_total,
                            "Non-subscription message received"
                        );
                    }

                    // Periodic heartbeat even when no events (every 1000 messages)
                    if messages_total % 1000 == 0 {
                        info!(
                            messages_total = messages_total,
                            events_received = events_received,
                            events_processed = events_processed,
                            events_filtered = events_filtered,
                            last_block = last_block_number,
                            "WebSocket heartbeat - connection alive"
                        );
                    }
                }
                Ok(Message::Close(frame)) => {
                    warn!(
                        frame = ?frame,
                        events_received = events_received,
                        events_processed = events_processed,
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
                        events_received = events_received,
                        events_processed = events_processed,
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
