use super::traits::{BalanceEventType, EventSink, SinkEvent};
use async_trait::async_trait;
use indexer_core::Result;
use indexer_redis::channels::{
    orderbook_channel, trades_channel, user_balance_channel, user_orders_channel,
    user_orders_pool_channel, user_trades_pool_channel,
};
use indexer_redis::messages::{BalanceMessage, OrderMessage, TradeMessage};
use indexer_redis::RedisPublisher;
use std::sync::Arc;
use tracing::debug;

/// Redis sink for real-time event streaming
pub struct RedisSink {
    publisher: Arc<RedisPublisher>,
}

impl RedisSink {
    pub fn new(publisher: Arc<RedisPublisher>) -> Self {
        Self { publisher }
    }
}

#[async_trait]
impl EventSink for RedisSink {
    async fn handle_event(&self, event: SinkEvent) -> Result<()> {
        match event {
            SinkEvent::PoolCreated(e) => {
                // Pool creation doesn't need streaming typically
                debug!(pool_id = %e.pool_id, "Pool created (Redis)");
            }

            SinkEvent::OrderPlaced(e)
            | SinkEvent::OrderUpdated(e)
            | SinkEvent::OrderCancelled(e) => {
                let event_type = match e.event_type {
                    super::traits::OrderEventType::Placed => "placed",
                    super::traits::OrderEventType::Updated => "updated",
                    super::traits::OrderEventType::Cancelled => "cancelled",
                    super::traits::OrderEventType::Filled => "filled",
                    super::traits::OrderEventType::PartiallyFilled => "partially_filled",
                };

                let msg = OrderMessage {
                    event_type: event_type.to_string(),
                    pool_id: e.pool_id.clone(),
                    order_id: e.order_id,
                    side: e.side.clone(),
                    price: e.price,
                    quantity: e.quantity,
                    filled: e.filled,
                    status: e.status,
                    user: e.user.clone(),
                    timestamp: e.timestamp,
                    tx_hash: e.tx_hash,
                };

                // Publish to orderbook channel
                let channel = orderbook_channel(&e.pool_id);
                if let Err(err) = self.publisher.publish_order(channel, &msg).await {
                    tracing::error!(error = %err, "Failed to publish order to Redis");
                }

                // Publish to user's orders channel
                let user_channel = user_orders_channel(&e.user);
                if let Err(err) = self.publisher.publish_order(user_channel, &msg).await {
                    tracing::error!(error = %err, "Failed to publish to user orders channel");
                }

                // Publish to user's pool-specific orders channel
                let user_pool_channel = user_orders_pool_channel(&e.user, &e.pool_id);
                if let Err(err) = self.publisher.publish_order(user_pool_channel, &msg).await {
                    tracing::error!(error = %err, "Failed to publish to user pool orders channel");
                }
            }

            SinkEvent::OrderMatched(e) => {
                let msg = TradeMessage {
                    pool_id: e.pool_id.clone(),
                    price: e.execution_price,
                    quantity: e.executed_quantity,
                    side: e.taker_side.clone(),
                    timestamp: e.timestamp,
                    tx_hash: e.tx_hash,
                    log_index: e.log_index,
                    buy_order_id: e.buy_order_id,
                    sell_order_id: e.sell_order_id,
                    taker_address: e.taker_address.clone(),
                };

                // Publish to trades channel
                let channel = trades_channel(&e.pool_id);
                if let Err(err) = self.publisher.publish_trade(channel, &msg).await {
                    tracing::error!(error = %err, "Failed to publish trade to Redis");
                }

                // Publish to user's trades channel
                let user_channel = user_trades_pool_channel(&e.taker_address, &e.pool_id);
                if let Err(err) = self.publisher.publish_trade(user_channel, &msg).await {
                    tracing::error!(error = %err, "Failed to publish to user trades channel");
                }
            }

            SinkEvent::BalanceChanged(e) => {
                let event_type = match e.event_type {
                    BalanceEventType::Deposit => "deposit",
                    BalanceEventType::Withdrawal => "withdrawal",
                    BalanceEventType::Lock => "lock",
                    BalanceEventType::Unlock => "unlock",
                    BalanceEventType::TransferIn => "transfer_in",
                    BalanceEventType::TransferOut => "transfer_out",
                };

                let msg = BalanceMessage {
                    user: e.user.clone(),
                    currency: e.currency,
                    amount: e.available,
                    locked_amount: e.locked,
                    event_type: event_type.to_string(),
                    timestamp: e.timestamp,
                    tx_hash: e.tx_hash,
                };

                // Publish to user's balance channel
                let channel = user_balance_channel(&e.user);
                if let Err(err) = self.publisher.publish_balance(channel, &msg).await {
                    tracing::error!(error = %err, "Failed to publish balance to Redis");
                }
            }
        }

        Ok(())
    }
}
