use async_trait::async_trait;
use indexer_core::Result;

/// Events that can be emitted to sinks
#[derive(Debug, Clone)]
pub enum SinkEvent {
    /// Pool created event
    PoolCreated(PoolCreatedEvent),
    /// Order placed event
    OrderPlaced(OrderEvent),
    /// Order matched (trade) event
    OrderMatched(TradeEvent),
    /// Order updated event
    OrderUpdated(OrderEvent),
    /// Order cancelled event
    OrderCancelled(OrderEvent),
    /// Balance changed event
    BalanceChanged(BalanceEvent),
}

#[derive(Debug, Clone)]
pub struct PoolCreatedEvent {
    pub pool_id: String,
    pub orderbook: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub tx_hash: String,
    pub block_number: u64,
    pub log_index: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct OrderEvent {
    pub pool_id: String,
    pub order_id: u64,
    pub user: String,
    pub side: String,
    pub price: String,
    pub quantity: String,
    pub filled: String,
    pub status: String,
    pub expiry: u64,
    pub event_type: OrderEventType,
    pub tx_hash: String,
    pub block_number: u64,
    pub log_index: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum OrderEventType {
    Placed,
    Updated,
    Cancelled,
    Filled,
    PartiallyFilled,
}

#[derive(Debug, Clone)]
pub struct TradeEvent {
    pub pool_id: String,
    pub buy_order_id: u64,
    pub sell_order_id: u64,
    pub taker_address: String,
    pub taker_side: String,
    pub execution_price: String,
    pub taker_limit_price: String,
    pub executed_quantity: String,
    pub tx_hash: String,
    pub block_number: u64,
    pub log_index: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct BalanceEvent {
    pub user: String,
    pub currency: String,
    pub available: String,
    pub locked: String,
    pub event_type: BalanceEventType,
    pub tx_hash: String,
    pub block_number: u64,
    pub log_index: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum BalanceEventType {
    Deposit,
    Withdrawal,
    Lock,
    Unlock,
    TransferIn,
    TransferOut,
}

/// Trait for event sinks (database, redis, etc.)
#[async_trait]
pub trait EventSink: Send + Sync {
    /// Handle an event
    async fn handle_event(&self, event: SinkEvent) -> Result<()>;
}
