use super::traits::{EventSink, SinkEvent};
use async_trait::async_trait;
use indexer_core::Result;
use indexer_db::models::{DbBalance, DbOrder, DbOrderBookTrade, DbOrderHistory, DbPool, DbUser};
use indexer_db::BatchWriter;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

/// Database sink for PostgreSQL persistence
pub struct DatabaseSink {
    writer: Arc<BatchWriter>,
    chain_id: i64,
}

impl DatabaseSink {
    pub fn new(writer: Arc<BatchWriter>, chain_id: i64) -> Self {
        Self { writer, chain_id }
    }
}

#[async_trait]
impl EventSink for DatabaseSink {
    async fn handle_event(&self, event: SinkEvent) -> Result<()> {
        match event {
            SinkEvent::PoolCreated(e) => {
                debug!(pool_id = %e.pool_id, "Writing pool to database");

                let db_pool = DbPool {
                    id: e.pool_id.clone(),
                    chain_id: self.chain_id,
                    coin: None,
                    order_book: e.orderbook,
                    base_currency: e.base_currency,
                    quote_currency: e.quote_currency,
                    base_decimals: Some(18),
                    quote_decimals: Some(18),
                    timestamp: Some(e.timestamp as i32),
                    token0_price: None,
                    token1_price: None,
                };

                self.writer
                    .write_pool(db_pool)
                    .await
                    .map_err(|e| indexer_core::IndexerError::Database(e.to_string()))?;
            }

            SinkEvent::OrderPlaced(e)
            | SinkEvent::OrderUpdated(e)
            | SinkEvent::OrderCancelled(e) => {
                let sink_start = Instant::now();
                let order_id_str = format!("{}_{}_{}", self.chain_id, e.pool_id, e.order_id);
                debug!(
                    order_id = %order_id_str,
                    block = e.block_number,
                    log_index = e.log_index,
                    "Queuing order for database"
                );

                let db_order = DbOrder {
                    id: order_id_str.clone(),
                    chain_id: self.chain_id,
                    pool_id: e.pool_id.clone(),
                    order_id: e.order_id as i64,
                    transaction_id: Some(e.tx_hash.clone()),
                    user: Some(e.user.clone()),
                    side: Some(e.side.clone()),
                    timestamp: Some(e.timestamp as i32),
                    price: e.price.parse().ok(),
                    quantity: e.quantity.parse().ok(),
                    filled: e.filled.parse().ok(),
                    total_filled_value: None,
                    order_type: Some("LIMIT".to_string()),
                    status: Some(e.status.clone()),
                    expiry: Some(e.expiry as i32),
                };

                let order_queue_start = Instant::now();
                self.writer
                    .write_order(db_order)
                    .await
                    .map_err(|err| indexer_core::IndexerError::Database(err.to_string()))?;
                let order_queue_us = order_queue_start.elapsed().as_micros();

                // Write order history
                let history_id = format!("{}_{}", e.tx_hash, e.log_index);
                let db_history = DbOrderHistory {
                    id: history_id,
                    chain_id: self.chain_id,
                    pool_id: e.pool_id.clone(),
                    order_id: Some(order_id_str.clone()),
                    transaction_id: Some(e.tx_hash.clone()),
                    timestamp: Some(e.timestamp as i32),
                    filled: e.filled.parse().ok(),
                    status: Some(e.status),
                };

                let history_queue_start = Instant::now();
                self.writer
                    .write_order_history(db_history)
                    .await
                    .map_err(|err| indexer_core::IndexerError::Database(err.to_string()))?;
                let history_queue_us = history_queue_start.elapsed().as_micros();

                // Write user if new
                let db_user = DbUser {
                    user: e.user.clone(),
                    chain_id: self.chain_id,
                    created_at: Some(e.timestamp as i32),
                };

                let user_queue_start = Instant::now();
                self.writer
                    .write_user(db_user)
                    .await
                    .map_err(|err| indexer_core::IndexerError::Database(err.to_string()))?;
                let user_queue_us = user_queue_start.elapsed().as_micros();

                let total_queue_us = sink_start.elapsed().as_micros();
                debug!(
                    order_id = %order_id_str,
                    block = e.block_number,
                    log_index = e.log_index,
                    order_queue_us = order_queue_us,
                    history_queue_us = history_queue_us,
                    user_queue_us = user_queue_us,
                    total_queue_us = total_queue_us,
                    "Order queued for DB"
                );
            }

            SinkEvent::OrderMatched(e) => {
                let sink_start = Instant::now();
                let trade_id = format!("{}_{}", e.tx_hash, e.log_index);
                debug!(
                    trade_id = %trade_id,
                    block = e.block_number,
                    log_index = e.log_index,
                    buy_order = e.buy_order_id,
                    sell_order = e.sell_order_id,
                    "Queuing trade for database"
                );

                let db_trade = DbOrderBookTrade {
                    id: trade_id.clone(),
                    chain_id: self.chain_id,
                    price: e.execution_price.parse().ok(),
                    taker_limit_price: e.taker_limit_price.parse().ok(),
                    quantity: e.executed_quantity.parse().ok(),
                    timestamp: Some(e.timestamp as i32),
                    log_index: Some(e.log_index as i32),
                    transaction_id: Some(e.tx_hash),
                    side: Some(e.taker_side),
                    pool_id: e.pool_id,
                };

                self.writer
                    .write_orderbook_trade(db_trade)
                    .await
                    .map_err(|err| indexer_core::IndexerError::Database(err.to_string()))?;

                let queue_us = sink_start.elapsed().as_micros();
                debug!(
                    trade_id = %trade_id,
                    block = e.block_number,
                    log_index = e.log_index,
                    buy_order = e.buy_order_id,
                    sell_order = e.sell_order_id,
                    queue_us = queue_us,
                    "Trade queued for DB"
                );
            }

            SinkEvent::BalanceChanged(e) => {
                let balance_id = format!("{}_{}_{}", self.chain_id, e.user, e.currency);
                debug!(balance_id = %balance_id, "Writing balance to database");

                let db_balance = DbBalance {
                    id: balance_id,
                    chain_id: self.chain_id,
                    user: Some(e.user.clone()),
                    currency: Some(e.currency),
                    amount: e.available.parse().ok(),
                    locked_amount: e.locked.parse().ok(),
                };

                self.writer
                    .write_balance(db_balance)
                    .await
                    .map_err(|err| indexer_core::IndexerError::Database(err.to_string()))?;

                // Write user if new
                let db_user = DbUser {
                    user: e.user,
                    chain_id: self.chain_id,
                    created_at: Some(e.timestamp as i32),
                };
                self.writer
                    .write_user(db_user)
                    .await
                    .map_err(|err| indexer_core::IndexerError::Database(err.to_string()))?;
            }
        }

        Ok(())
    }
}
