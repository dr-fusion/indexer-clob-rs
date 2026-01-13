use alloy_primitives::{Address, FixedBytes, B256, U128};

use super::OrderSide;

/// Unique key for trade lookup (tx_hash + log_index)
/// This ensures idempotent insertion - same event processed twice = same key
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TradeKey {
    pub tx_hash: B256,
    pub log_index: u64,
}

/// Represents a trade (order match)
#[derive(Debug, Clone)]
pub struct Trade {
    /// Unique trade key (tx_hash + log_index)
    pub key: TradeKey,
    /// Pool where trade occurred
    pub pool_id: FixedBytes<32>,
    /// Buy order ID
    pub buy_order_id: u64,
    /// Sell order ID
    pub sell_order_id: u64,
    /// Taker (initiator) address
    pub taker_address: Address,
    /// Taker side (the side that initiated the trade)
    pub taker_side: OrderSide,
    /// Execution price (maker's price)
    pub execution_price: U128,
    /// Taker's limit price
    pub taker_limit_price: U128,
    /// Amount executed in this trade
    pub executed_quantity: U128,
    /// Timestamp of the trade
    pub timestamp: u64,
    /// Block number
    pub block_number: u64,
}

impl Trade {
    /// Get transaction hash from key
    pub fn tx_hash(&self) -> B256 {
        self.key.tx_hash
    }

    /// Get log index from key
    pub fn log_index(&self) -> u64 {
        self.key.log_index
    }
}
