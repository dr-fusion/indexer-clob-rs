mod balance;
mod order;
mod pool;
mod trade;

pub use balance::{BalanceEvent, BalanceEventType, BalanceKey, UserBalance};
pub use order::{Order, OrderKey, OrderSide, OrderStatus};
pub use pool::Pool;
pub use trade::{Trade, TradeKey};

use std::time::{SystemTime, UNIX_EPOCH};

/// Get current timestamp in microseconds since Unix epoch
pub fn now_micros() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
}

/// Get current timestamp in milliseconds since Unix epoch
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
