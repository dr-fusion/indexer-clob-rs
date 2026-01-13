/// Channel name builders for Redis streaming
/// Matches the Ponder reference implementation patterns

/// Pool stats channel (debounced)
pub fn pool_channel(pool_address: &str) -> String {
    format!("pool:{}", pool_address)
}

/// Ticker channel (debounced, bid/ask updates)
pub fn ticker_channel(pool_address: &str) -> String {
    format!("ticker:{}", pool_address)
}

/// Trades channel (real-time)
pub fn trades_channel(pool_id: &str) -> String {
    format!("trades:{}", pool_id)
}

/// Orderbook channel (order add/update/cancel/match)
pub fn orderbook_channel(pool_id: &str) -> String {
    format!("orderbook:{}", pool_id)
}

/// Candle channel for specific interval
pub fn candles_channel(interval: &str, pool_id: &str) -> String {
    format!("candles:{}:{}", interval, pool_id)
}

/// User orders channel (all pools)
pub fn user_orders_channel(user: &str) -> String {
    format!("user:{}:orders", user)
}

/// User orders channel (specific pool)
pub fn user_orders_pool_channel(user: &str, pool_id: &str) -> String {
    format!("user:{}:orders:{}", user, pool_id)
}

/// User trades channel (specific pool)
pub fn user_trades_pool_channel(user: &str, pool_id: &str) -> String {
    format!("user:{}:trades:{}", user, pool_id)
}

/// Balance channel for user
pub fn user_balance_channel(user: &str) -> String {
    format!("user:{}:balance", user)
}

/// All available candle intervals
pub const CANDLE_INTERVALS: &[&str] = &[
    "1m", "5m", "15m", "1h", "2h", "4h", "1d", "1w", "1M",
];
