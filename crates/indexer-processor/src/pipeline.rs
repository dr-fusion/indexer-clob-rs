use alloy::rpc::types::Log;
use alloy_sol_types::SolEvent;
use indexer_core::events::{
    Deposit, Lock, OrderCancelled, OrderMatched, OrderPlaced, PoolCreated, TransferFrom,
    TransferLockedFrom, Unlock, UpdateOrder, Withdrawal,
};
use indexer_core::types::{now_micros, OrderStatus};
use indexer_core::{IndexerConfig, Result};
use indexer_store::IndexerStore;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, trace, warn};

use crate::handlers::{
    BalanceEventHandler, OrderCancelledHandler, OrderMatchedHandler, OrderPlacedHandler,
    OrderUpdatedHandler, PoolCreatedHandler,
};
use crate::sinks::traits::{
    BalanceEvent as SinkBalanceEvent, BalanceEventType as SinkBalanceEventType, OrderEvent,
    OrderEventType, PoolCreatedEvent, TradeEvent,
};
use crate::sinks::{CompositeSink, SinkEvent};

/// Event processor that routes logs to appropriate handlers
pub struct EventProcessor {
    store: Arc<IndexerStore>,
    config: IndexerConfig,

    // Handlers
    pool_created: PoolCreatedHandler,
    order_placed: OrderPlacedHandler,
    order_matched: OrderMatchedHandler,
    order_updated: OrderUpdatedHandler,
    order_cancelled: OrderCancelledHandler,
    balance_handler: BalanceEventHandler,

    // Optional sinks for database/redis
    sinks: Arc<RwLock<CompositeSink>>,
}

impl EventProcessor {
    pub fn new(store: Arc<IndexerStore>, config: IndexerConfig) -> Self {
        Self {
            store: store.clone(),
            config,
            pool_created: PoolCreatedHandler::new(store.clone()),
            order_placed: OrderPlacedHandler::new(store.clone()),
            order_matched: OrderMatchedHandler::new(store.clone()),
            order_updated: OrderUpdatedHandler::new(store.clone()),
            order_cancelled: OrderCancelledHandler::new(store.clone()),
            balance_handler: BalanceEventHandler::new(store),
            sinks: Arc::new(RwLock::new(CompositeSink::new())),
        }
    }

    /// Set the composite sink for database/redis
    pub async fn set_sinks(&self, sinks: CompositeSink) {
        let mut s = self.sinks.write().await;
        *s = sinks;
    }

    /// Get reference to sinks
    pub fn sinks(&self) -> Arc<RwLock<CompositeSink>> {
        self.sinks.clone()
    }

    /// Get reference to the store
    pub fn store(&self) -> &Arc<IndexerStore> {
        &self.store
    }

    /// Get reference to config
    pub fn config(&self) -> &IndexerConfig {
        &self.config
    }

    /// Check if a log is from a contract we care about
    pub fn is_relevant_log(&self, log: &Log) -> bool {
        let address = log.address();

        // Check static contract addresses
        if address == self.config.pool_manager || address == self.config.balance_manager {
            return true;
        }

        // Check if it's a known OrderBook
        self.store.pools.is_known_orderbook(&address)
    }

    /// Process a single log, routing to the appropriate handler
    pub async fn process_log(&self, log: Log) -> Result<()> {
        let process_start = Instant::now();

        let topic0 = match log.topics().first() {
            Some(t) => t,
            None => {
                trace!("Skipping log without topic0");
                return Ok(());
            }
        };

        let tx_hash = format!("{:?}", log.transaction_hash.unwrap_or_default());
        let block_number = log.block_number.unwrap_or_default();
        let log_index = log.log_index.unwrap_or_default() as u64;
        let address = log.address();
        let timestamp = 0u64; // We'll get this from the event where available

        debug!(
            block = block_number,
            log_index = log_index,
            address = ?address,
            topic0 = ?topic0,
            tx_hash = %tx_hash,
            "Processing log event"
        );

        // Route to appropriate handler based on event signature
        match *topic0 {
            sig if sig == PoolCreated::SIGNATURE_HASH => {
                let handler_start = Instant::now();
                self.pool_created.handle(&log).await?;
                let handler_duration_us = handler_start.elapsed().as_micros();

                // Emit to sinks
                let sink_start = Instant::now();
                if let Ok(event) = PoolCreated::decode_log(&log.inner) {
                    let sink_event = SinkEvent::PoolCreated(PoolCreatedEvent {
                        pool_id: format!("{:?}", event.poolId),
                        orderbook: format!("{:?}", event.orderBook),
                        base_currency: format!("{:?}", event.baseCurrency),
                        quote_currency: format!("{:?}", event.quoteCurrency),
                        tx_hash: tx_hash.clone(),
                        block_number,
                        log_index,
                        timestamp,
                    });
                    self.emit_to_sinks(sink_event).await;
                }
                let sink_duration_us = sink_start.elapsed().as_micros();
                let total_duration_us = process_start.elapsed().as_micros();

                debug!(
                    event = "PoolCreated",
                    block = block_number,
                    log_index = log_index,
                    handler_us = handler_duration_us,
                    sink_us = sink_duration_us,
                    total_us = total_duration_us,
                    "Processed PoolCreated event"
                );
                Ok(())
            }
            sig if sig == OrderPlaced::SIGNATURE_HASH => {
                let pipeline_entered_at = now_micros();

                debug!(
                    event = "OrderPlaced",
                    block = block_number,
                    log_index = log_index,
                    pipeline_start = pipeline_entered_at,
                    "Processing OrderPlaced - entering pipeline"
                );

                let handler_start = Instant::now();
                self.order_placed.handle(&log).await?;
                let handler_duration_us = handler_start.elapsed().as_micros();

                debug!(
                    event = "OrderPlaced",
                    block = block_number,
                    log_index = log_index,
                    handler_us = handler_duration_us,
                    "OrderPlaced handler complete"
                );

                // Emit to sinks
                let sink_start = Instant::now();
                let mut order_id_for_log: u64 = 0;
                if let Ok(event) = OrderPlaced::decode_log(&log.inner) {
                    order_id_for_log = event.orderId.to::<u64>();
                    let orderbook_address = log.address();
                    if let Some(pool_id) = self.store.pools.get_pool_id_by_orderbook(&orderbook_address) {
                        let sink_event = SinkEvent::OrderPlaced(OrderEvent {
                            pool_id: format!("{:?}", pool_id),
                            order_id: event.orderId.to::<u64>(),
                            user: format!("{:?}", event.user),
                            side: if event.side == 0 { "BUY".to_string() } else { "SELL".to_string() },
                            price: event.price.to_string(),
                            quantity: event.quantity.to_string(),
                            filled: "0".to_string(),
                            status: "OPEN".to_string(),
                            expiry: event.expiry.to::<u64>(),
                            event_type: OrderEventType::Placed,
                            tx_hash: tx_hash.clone(),
                            block_number,
                            log_index,
                            timestamp, // Use the local timestamp variable
                        });
                        self.emit_to_sinks(sink_event).await;
                    }
                }
                let sink_duration_us = sink_start.elapsed().as_micros();
                let total_duration_us = process_start.elapsed().as_micros();

                debug!(
                    event = "OrderPlaced",
                    block = block_number,
                    log_index = log_index,
                    sink_us = sink_duration_us,
                    "OrderPlaced sinks complete"
                );

                // Summary log at INFO level for easy visibility
                info!(
                    event = "OrderPlaced",
                    block = block_number,
                    log_index = log_index,
                    order_id = order_id_for_log,
                    pipeline_start = pipeline_entered_at,
                    handler_us = handler_duration_us,
                    sink_us = sink_duration_us,
                    total_us = total_duration_us,
                    "OrderPlaced complete"
                );
                Ok(())
            }
            sig if sig == OrderMatched::SIGNATURE_HASH => {
                let pipeline_entered_at = now_micros();

                debug!(
                    event = "OrderMatched",
                    block = block_number,
                    log_index = log_index,
                    pipeline_start = pipeline_entered_at,
                    "Processing OrderMatched - entering pipeline"
                );

                let handler_start = Instant::now();
                self.order_matched.handle(&log).await?;
                let handler_duration_us = handler_start.elapsed().as_micros();

                debug!(
                    event = "OrderMatched",
                    block = block_number,
                    log_index = log_index,
                    handler_us = handler_duration_us,
                    "OrderMatched handler complete"
                );

                // Emit to sinks
                let sink_start = Instant::now();
                let mut buy_order_id_log: u64 = 0;
                let mut sell_order_id_log: u64 = 0;
                if let Ok(event) = OrderMatched::decode_log(&log.inner) {
                    buy_order_id_log = event.buyOrderId.to::<u64>();
                    sell_order_id_log = event.sellOrderId.to::<u64>();
                    let orderbook_address = log.address();
                    if let Some(pool_id) = self.store.pools.get_pool_id_by_orderbook(&orderbook_address) {
                        let sink_event = SinkEvent::OrderMatched(TradeEvent {
                            pool_id: format!("{:?}", pool_id),
                            buy_order_id: event.buyOrderId.to::<u64>(),
                            sell_order_id: event.sellOrderId.to::<u64>(),
                            taker_address: format!("{:?}", event.user),
                            taker_side: if event.side == 0 { "BUY".to_string() } else { "SELL".to_string() },
                            execution_price: event.executionPrice.to_string(),
                            taker_limit_price: event.takerLimitPrice.to_string(),
                            executed_quantity: event.executedQuantity.to_string(),
                            tx_hash: tx_hash.clone(),
                            block_number,
                            log_index,
                            timestamp: event.timestamp.to::<u64>(),
                        });
                        self.emit_to_sinks(sink_event).await;
                    }
                }
                let sink_duration_us = sink_start.elapsed().as_micros();
                let total_duration_us = process_start.elapsed().as_micros();

                debug!(
                    event = "OrderMatched",
                    block = block_number,
                    log_index = log_index,
                    sink_us = sink_duration_us,
                    "OrderMatched sinks complete"
                );

                // Summary log at INFO level for easy visibility
                info!(
                    event = "OrderMatched",
                    block = block_number,
                    log_index = log_index,
                    buy_order = buy_order_id_log,
                    sell_order = sell_order_id_log,
                    pipeline_start = pipeline_entered_at,
                    handler_us = handler_duration_us,
                    sink_us = sink_duration_us,
                    total_us = total_duration_us,
                    "OrderMatched complete"
                );
                Ok(())
            }
            sig if sig == UpdateOrder::SIGNATURE_HASH => {
                let handler_start = Instant::now();
                self.order_updated.handle(&log).await?;
                let handler_duration_us = handler_start.elapsed().as_micros();

                // Emit to sinks
                let sink_start = Instant::now();
                if let Ok(event) = UpdateOrder::decode_log(&log.inner) {
                    let orderbook_address = log.address();
                    if let Some(pool_id) = self.store.pools.get_pool_id_by_orderbook(&orderbook_address) {
                        // Get order details from store
                        let order_key = indexer_core::types::OrderKey {
                            pool_id,
                            order_id: event.orderId.to::<u64>(),
                        };
                        if let Some(order) = self.store.orders.get(&order_key) {
                            let status = OrderStatus::from(event.status as u8);
                            let event_type = match status {
                                OrderStatus::Filled => OrderEventType::Filled,
                                OrderStatus::PartiallyFilled => OrderEventType::PartiallyFilled,
                                _ => OrderEventType::Updated,
                            };
                            let sink_event = SinkEvent::OrderUpdated(OrderEvent {
                                pool_id: format!("{:?}", pool_id),
                                order_id: event.orderId.to::<u64>(),
                                user: format!("{:?}", order.user),
                                side: format!("{:?}", order.side),
                                price: order.price.to_string(),
                                quantity: order.original_quantity.to_string(),
                                filled: event.filled.to_string(),
                                status: format!("{:?}", status),
                                expiry: order.expiry,
                                event_type,
                                tx_hash: tx_hash.clone(),
                                block_number,
                                log_index,
                                timestamp: event.timestamp.to::<u64>(),
                            });
                            self.emit_to_sinks(sink_event).await;
                        }
                    }
                }
                let sink_duration_us = sink_start.elapsed().as_micros();
                let total_duration_us = process_start.elapsed().as_micros();

                debug!(
                    event = "UpdateOrder",
                    block = block_number,
                    log_index = log_index,
                    handler_us = handler_duration_us,
                    sink_us = sink_duration_us,
                    total_us = total_duration_us,
                    "Processed UpdateOrder event"
                );
                Ok(())
            }
            sig if sig == OrderCancelled::SIGNATURE_HASH => {
                let handler_start = Instant::now();
                self.order_cancelled.handle(&log).await?;
                let handler_duration_us = handler_start.elapsed().as_micros();

                // Emit to sinks
                let sink_start = Instant::now();
                if let Ok(event) = OrderCancelled::decode_log(&log.inner) {
                    let orderbook_address = log.address();
                    if let Some(pool_id) = self.store.pools.get_pool_id_by_orderbook(&orderbook_address) {
                        // Get order details from store
                        let order_key = indexer_core::types::OrderKey {
                            pool_id,
                            order_id: event.orderId.to::<u64>(),
                        };
                        if let Some(order) = self.store.orders.get(&order_key) {
                            let sink_event = SinkEvent::OrderCancelled(OrderEvent {
                                pool_id: format!("{:?}", pool_id),
                                order_id: event.orderId.to::<u64>(),
                                user: format!("{:?}", event.user),
                                side: format!("{:?}", order.side),
                                price: order.price.to_string(),
                                quantity: order.original_quantity.to_string(),
                                filled: order.filled_quantity.to_string(),
                                status: "CANCELLED".to_string(),
                                expiry: order.expiry,
                                event_type: OrderEventType::Cancelled,
                                tx_hash: tx_hash.clone(),
                                block_number,
                                log_index,
                                timestamp: event.timestamp.to::<u64>(),
                            });
                            self.emit_to_sinks(sink_event).await;
                        }
                    }
                }
                let sink_duration_us = sink_start.elapsed().as_micros();
                let total_duration_us = process_start.elapsed().as_micros();

                debug!(
                    event = "OrderCancelled",
                    block = block_number,
                    log_index = log_index,
                    handler_us = handler_duration_us,
                    sink_us = sink_duration_us,
                    total_us = total_duration_us,
                    "Processed OrderCancelled event"
                );
                Ok(())
            }
            sig if sig == Deposit::SIGNATURE_HASH => {
                let handler_start = Instant::now();
                self.balance_handler.handle_deposit(&log).await?;
                let handler_duration_us = handler_start.elapsed().as_micros();

                // Emit to sinks
                let sink_start = Instant::now();
                if let Ok(event) = Deposit::decode_log(&log.inner) {
                    // Get current balance from store
                    if let Some(balance) = self.store.balances.get(&event.user, &event.id) {
                        let sink_event = SinkEvent::BalanceChanged(SinkBalanceEvent {
                            user: format!("{:?}", event.user),
                            currency: format!("{:?}", event.id),
                            available: balance.available.to_string(),
                            locked: balance.locked.to_string(),
                            event_type: SinkBalanceEventType::Deposit,
                            tx_hash: tx_hash.clone(),
                            block_number,
                            log_index,
                            timestamp,
                        });
                        self.emit_to_sinks(sink_event).await;
                    }
                }
                let sink_duration_us = sink_start.elapsed().as_micros();
                let total_duration_us = process_start.elapsed().as_micros();

                debug!(
                    event = "Deposit",
                    block = block_number,
                    log_index = log_index,
                    handler_us = handler_duration_us,
                    sink_us = sink_duration_us,
                    total_us = total_duration_us,
                    "Processed Deposit event"
                );
                Ok(())
            }
            sig if sig == Withdrawal::SIGNATURE_HASH => {
                let handler_start = Instant::now();
                self.balance_handler.handle_withdrawal(&log).await?;
                let handler_duration_us = handler_start.elapsed().as_micros();

                // Emit to sinks
                let sink_start = Instant::now();
                if let Ok(event) = Withdrawal::decode_log(&log.inner) {
                    if let Some(balance) = self.store.balances.get(&event.user, &event.id) {
                        let sink_event = SinkEvent::BalanceChanged(SinkBalanceEvent {
                            user: format!("{:?}", event.user),
                            currency: format!("{:?}", event.id),
                            available: balance.available.to_string(),
                            locked: balance.locked.to_string(),
                            event_type: SinkBalanceEventType::Withdrawal,
                            tx_hash: tx_hash.clone(),
                            block_number,
                            log_index,
                            timestamp,
                        });
                        self.emit_to_sinks(sink_event).await;
                    }
                }
                let sink_duration_us = sink_start.elapsed().as_micros();
                let total_duration_us = process_start.elapsed().as_micros();

                debug!(
                    event = "Withdrawal",
                    block = block_number,
                    log_index = log_index,
                    handler_us = handler_duration_us,
                    sink_us = sink_duration_us,
                    total_us = total_duration_us,
                    "Processed Withdrawal event"
                );
                Ok(())
            }
            sig if sig == Lock::SIGNATURE_HASH => {
                let handler_start = Instant::now();
                self.balance_handler.handle_lock(&log).await?;
                let handler_duration_us = handler_start.elapsed().as_micros();

                // Emit to sinks
                let sink_start = Instant::now();
                if let Ok(event) = Lock::decode_log(&log.inner) {
                    if let Some(balance) = self.store.balances.get(&event.user, &event.id) {
                        let sink_event = SinkEvent::BalanceChanged(SinkBalanceEvent {
                            user: format!("{:?}", event.user),
                            currency: format!("{:?}", event.id),
                            available: balance.available.to_string(),
                            locked: balance.locked.to_string(),
                            event_type: SinkBalanceEventType::Lock,
                            tx_hash: tx_hash.clone(),
                            block_number,
                            log_index,
                            timestamp,
                        });
                        self.emit_to_sinks(sink_event).await;
                    }
                }
                let sink_duration_us = sink_start.elapsed().as_micros();
                let total_duration_us = process_start.elapsed().as_micros();

                debug!(
                    event = "Lock",
                    block = block_number,
                    log_index = log_index,
                    handler_us = handler_duration_us,
                    sink_us = sink_duration_us,
                    total_us = total_duration_us,
                    "Processed Lock event"
                );
                Ok(())
            }
            sig if sig == Unlock::SIGNATURE_HASH => {
                let handler_start = Instant::now();
                self.balance_handler.handle_unlock(&log).await?;
                let handler_duration_us = handler_start.elapsed().as_micros();

                // Emit to sinks
                let sink_start = Instant::now();
                if let Ok(event) = Unlock::decode_log(&log.inner) {
                    if let Some(balance) = self.store.balances.get(&event.user, &event.id) {
                        let sink_event = SinkEvent::BalanceChanged(SinkBalanceEvent {
                            user: format!("{:?}", event.user),
                            currency: format!("{:?}", event.id),
                            available: balance.available.to_string(),
                            locked: balance.locked.to_string(),
                            event_type: SinkBalanceEventType::Unlock,
                            tx_hash: tx_hash.clone(),
                            block_number,
                            log_index,
                            timestamp,
                        });
                        self.emit_to_sinks(sink_event).await;
                    }
                }
                let sink_duration_us = sink_start.elapsed().as_micros();
                let total_duration_us = process_start.elapsed().as_micros();

                debug!(
                    event = "Unlock",
                    block = block_number,
                    log_index = log_index,
                    handler_us = handler_duration_us,
                    sink_us = sink_duration_us,
                    total_us = total_duration_us,
                    "Processed Unlock event"
                );
                Ok(())
            }
            sig if sig == TransferFrom::SIGNATURE_HASH => {
                let handler_start = Instant::now();
                self.balance_handler.handle_transfer_from(&log).await?;
                let handler_duration_us = handler_start.elapsed().as_micros();

                // Emit to sinks for both sender and receiver
                let sink_start = Instant::now();
                if let Ok(event) = TransferFrom::decode_log(&log.inner) {
                    // Sender balance
                    if let Some(balance) = self.store.balances.get(&event.sender, &event.id) {
                        let sink_event = SinkEvent::BalanceChanged(SinkBalanceEvent {
                            user: format!("{:?}", event.sender),
                            currency: format!("{:?}", event.id),
                            available: balance.available.to_string(),
                            locked: balance.locked.to_string(),
                            event_type: SinkBalanceEventType::TransferOut,
                            tx_hash: tx_hash.clone(),
                            block_number,
                            log_index,
                            timestamp,
                        });
                        self.emit_to_sinks(sink_event).await;
                    }

                    // Receiver balance
                    if let Some(balance) = self.store.balances.get(&event.receiver, &event.id) {
                        let sink_event = SinkEvent::BalanceChanged(SinkBalanceEvent {
                            user: format!("{:?}", event.receiver),
                            currency: format!("{:?}", event.id),
                            available: balance.available.to_string(),
                            locked: balance.locked.to_string(),
                            event_type: SinkBalanceEventType::TransferIn,
                            tx_hash: tx_hash.clone(),
                            block_number,
                            log_index,
                            timestamp,
                        });
                        self.emit_to_sinks(sink_event).await;
                    }
                }
                let sink_duration_us = sink_start.elapsed().as_micros();
                let total_duration_us = process_start.elapsed().as_micros();

                debug!(
                    event = "TransferFrom",
                    block = block_number,
                    log_index = log_index,
                    handler_us = handler_duration_us,
                    sink_us = sink_duration_us,
                    total_us = total_duration_us,
                    "Processed TransferFrom event"
                );
                Ok(())
            }
            sig if sig == TransferLockedFrom::SIGNATURE_HASH => {
                let handler_start = Instant::now();
                self.balance_handler.handle_transfer_locked_from(&log).await?;
                let handler_duration_us = handler_start.elapsed().as_micros();

                // Emit to sinks for both sender and receiver
                let sink_start = Instant::now();
                if let Ok(event) = TransferLockedFrom::decode_log(&log.inner) {
                    // Sender balance
                    if let Some(balance) = self.store.balances.get(&event.sender, &event.id) {
                        let sink_event = SinkEvent::BalanceChanged(SinkBalanceEvent {
                            user: format!("{:?}", event.sender),
                            currency: format!("{:?}", event.id),
                            available: balance.available.to_string(),
                            locked: balance.locked.to_string(),
                            event_type: SinkBalanceEventType::TransferOut,
                            tx_hash: tx_hash.clone(),
                            block_number,
                            log_index,
                            timestamp,
                        });
                        self.emit_to_sinks(sink_event).await;
                    }

                    // Receiver balance
                    if let Some(balance) = self.store.balances.get(&event.receiver, &event.id) {
                        let sink_event = SinkEvent::BalanceChanged(SinkBalanceEvent {
                            user: format!("{:?}", event.receiver),
                            currency: format!("{:?}", event.id),
                            available: balance.available.to_string(),
                            locked: balance.locked.to_string(),
                            event_type: SinkBalanceEventType::TransferIn,
                            tx_hash: tx_hash.clone(),
                            block_number,
                            log_index,
                            timestamp,
                        });
                        self.emit_to_sinks(sink_event).await;
                    }
                }
                let sink_duration_us = sink_start.elapsed().as_micros();
                let total_duration_us = process_start.elapsed().as_micros();

                debug!(
                    event = "TransferLockedFrom",
                    block = block_number,
                    log_index = log_index,
                    handler_us = handler_duration_us,
                    sink_us = sink_duration_us,
                    total_us = total_duration_us,
                    "Processed TransferLockedFrom event"
                );
                Ok(())
            }
            _ => {
                trace!(topic0 = ?topic0, "Unknown event signature");
                Ok(())
            }
        }
    }

    /// Emit event to all configured sinks
    async fn emit_to_sinks(&self, event: SinkEvent) {
        let sinks = self.sinks.read().await;
        if !sinks.is_empty() {
            if let Err(e) = sinks.emit(event).await {
                warn!(error = %e, "Failed to emit event to sinks");
            }
        }
    }

    /// Process multiple logs in order
    pub async fn process_logs(&self, logs: Vec<Log>) -> Result<()> {
        for log in logs {
            self.process_log(log).await?;
        }
        Ok(())
    }
}
