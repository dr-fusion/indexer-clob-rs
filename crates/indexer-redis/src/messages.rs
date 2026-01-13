use serde::{Deserialize, Serialize};

/// Trade message for Redis streaming
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeMessage {
    pub pool_id: String,
    pub price: String,
    pub quantity: String,
    pub side: String,
    pub timestamp: u64,
    pub tx_hash: String,
    pub log_index: u64,
    pub buy_order_id: u64,
    pub sell_order_id: u64,
    pub taker_address: String,
}

/// Order update message for orderbook channel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderMessage {
    #[serde(rename = "type")]
    pub event_type: String, // "placed", "matched", "updated", "cancelled"
    pub pool_id: String,
    pub order_id: u64,
    pub side: String,
    pub price: String,
    pub quantity: String,
    pub filled: String,
    pub status: String,
    pub user: String,
    pub timestamp: u64,
    pub tx_hash: String,
}

/// Ticker message (debounced)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerMessage {
    pub pool_id: String,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
    pub last_price: Option<String>,
    pub volume_24h: Option<String>,
    pub price_change_24h: Option<f64>,
    pub timestamp: u64,
}

/// Pool stats message (debounced)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStatsMessage {
    pub pool_id: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub total_orders: u64,
    pub total_trades: u64,
    pub volume_24h: String,
    pub last_price: Option<String>,
    pub timestamp: u64,
}

/// Candlestick message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleMessage {
    pub pool_id: String,
    pub interval: String,
    pub open_time: u64,
    pub close_time: u64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub count: u32,
}

/// Balance update message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceMessage {
    pub user: String,
    pub currency: String,
    pub amount: String,
    pub locked_amount: String,
    pub event_type: String, // "deposit", "withdrawal", "lock", "unlock", "transfer"
    pub timestamp: u64,
    pub tx_hash: String,
}

/// Stream message envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMessage {
    pub channel: String,
    pub data: String, // JSON stringified payload
    pub source: String,
    pub timestamp: u64,
}

impl StreamMessage {
    pub fn new(channel: String, data: impl Serialize) -> Result<Self, serde_json::Error> {
        Ok(Self {
            channel,
            data: serde_json::to_string(&data)?,
            source: "clob-indexer".to_string(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        })
    }
}
