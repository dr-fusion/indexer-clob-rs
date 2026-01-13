use crate::channels::{pool_channel, ticker_channel};
use crate::messages::{PoolStatsMessage, TickerMessage};
use crate::publisher::RedisPublisher;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, error};

/// Debounced event types
#[derive(Debug, Clone)]
pub enum DebouncedEvent {
    PoolStats(PoolStatsMessage),
    Ticker(TickerMessage),
}

/// Key for debounced events
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DebouncedKey {
    event_type: String,
    pool_id: String,
}

/// Pending event with timing info
struct PendingEvent {
    event: DebouncedEvent,
    first_seen: Instant,
    last_update: Instant,
}

/// Debounced emitter for pool stats and ticker updates
/// Batches rapid updates and emits at most once per debounce interval
pub struct DebouncedEmitter {
    sender: mpsc::Sender<DebouncedEvent>,
}

impl DebouncedEmitter {
    /// Create a new debounced emitter
    pub fn new(
        publisher: Arc<RedisPublisher>,
        debounce_ms: u64,
        max_debounce_ms: u64,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(10000);

        // Spawn background debounce task
        tokio::spawn(Self::debounce_loop(
            publisher,
            receiver,
            debounce_ms,
            max_debounce_ms,
        ));

        Self { sender }
    }

    /// Queue pool stats update (debounced)
    pub async fn emit_pool_stats(&self, stats: PoolStatsMessage) {
        if self.sender.try_send(DebouncedEvent::PoolStats(stats)).is_err() {
            debug!("Debounce queue full, dropping pool stats update");
        }
    }

    /// Queue ticker update (debounced)
    pub async fn emit_ticker(&self, ticker: TickerMessage) {
        if self.sender.try_send(DebouncedEvent::Ticker(ticker)).is_err() {
            debug!("Debounce queue full, dropping ticker update");
        }
    }

    /// Background debounce loop
    async fn debounce_loop(
        publisher: Arc<RedisPublisher>,
        mut receiver: mpsc::Receiver<DebouncedEvent>,
        debounce_ms: u64,
        max_debounce_ms: u64,
    ) {
        let pending: DashMap<DebouncedKey, PendingEvent> = DashMap::new();
        let debounce = Duration::from_millis(debounce_ms);
        let max_debounce = Duration::from_millis(max_debounce_ms);
        let check_interval = Duration::from_millis(debounce_ms / 2);

        loop {
            tokio::select! {
                // Receive new events
                event = receiver.recv() => {
                    match event {
                        Some(event) => {
                            let key = match &event {
                                DebouncedEvent::PoolStats(s) => DebouncedKey {
                                    event_type: "pool".to_string(),
                                    pool_id: s.pool_id.clone(),
                                },
                                DebouncedEvent::Ticker(t) => DebouncedKey {
                                    event_type: "ticker".to_string(),
                                    pool_id: t.pool_id.clone(),
                                },
                            };

                            let now = Instant::now();

                            pending.entry(key)
                                .and_modify(|e| {
                                    e.event = event.clone();
                                    e.last_update = now;
                                })
                                .or_insert_with(|| PendingEvent {
                                    event,
                                    first_seen: now,
                                    last_update: now,
                                });
                        }
                        None => {
                            // Channel closed, flush remaining and exit
                            Self::flush_all(&publisher, &pending).await;
                            break;
                        }
                    }
                }

                // Periodic check for events to emit
                _ = tokio::time::sleep(check_interval) => {
                    let now = Instant::now();
                    let mut to_emit = Vec::new();

                    // Find events ready to emit
                    for entry in pending.iter() {
                        let key = entry.key();
                        let event = entry.value();

                        let since_last_update = now.duration_since(event.last_update);
                        let since_first_seen = now.duration_since(event.first_seen);

                        // Emit if:
                        // 1. Debounce interval has passed since last update, OR
                        // 2. Max debounce time exceeded (force emit)
                        if since_last_update >= debounce || since_first_seen >= max_debounce {
                            to_emit.push(key.clone());
                        }
                    }

                    // Emit and remove ready events
                    for key in to_emit {
                        if let Some((_, pending_event)) = pending.remove(&key) {
                            Self::emit_event(&publisher, pending_event.event).await;
                        }
                    }
                }
            }
        }
    }

    /// Emit a single event
    async fn emit_event(publisher: &RedisPublisher, event: DebouncedEvent) {
        let result = match event {
            DebouncedEvent::PoolStats(stats) => {
                let channel = pool_channel(&stats.pool_id);
                publisher.publish(channel, &stats).await
            }
            DebouncedEvent::Ticker(ticker) => {
                let channel = ticker_channel(&ticker.pool_id);
                publisher.publish(channel, &ticker).await
            }
        };

        if let Err(e) = result {
            error!(error = %e, "Failed to emit debounced event");
        }
    }

    /// Flush all pending events
    async fn flush_all(publisher: &RedisPublisher, pending: &DashMap<DebouncedKey, PendingEvent>) {
        for entry in pending.iter() {
            Self::emit_event(publisher, entry.value().event.clone()).await;
        }
        pending.clear();
    }
}

impl Clone for DebouncedEmitter {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}
