use crate::interval::CandleInterval;
use indexer_db::models::DbCandle;
use serde::{Deserialize, Serialize};

/// In-memory candlestick bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandleBucket {
    /// Pool ID (hex)
    pub pool_id: String,
    /// Interval for this bucket
    pub interval: CandleInterval,
    /// Chain ID
    pub chain_id: i64,
    /// Bucket open timestamp
    pub open_time: u64,
    /// Bucket close timestamp
    pub close_time: u64,
    /// Open price
    pub open: f64,
    /// High price
    pub high: f64,
    /// Low price
    pub low: f64,
    /// Close price (latest)
    pub close: f64,
    /// Base volume
    pub volume: f64,
    /// Quote volume
    pub quote_volume: f64,
    /// Volume in USD
    pub volume_usd: f64,
    /// Trade count
    pub count: u32,
    /// Taker buy base volume
    pub taker_buy_base_volume: f64,
    /// Taker buy quote volume
    pub taker_buy_quote_volume: f64,
    /// Whether this bucket has been modified (dirty)
    pub dirty: bool,
}

impl CandleBucket {
    /// Create a new bucket from first trade
    pub fn new(
        pool_id: String,
        interval: CandleInterval,
        chain_id: i64,
        timestamp: u64,
        price: f64,
        base_quantity: f64,
        quote_value: f64,
        is_taker_buy: bool,
    ) -> Self {
        let open_time = interval.bucket_open_time(timestamp);
        let close_time = interval.bucket_close_time(timestamp);

        let mut bucket = Self {
            pool_id,
            interval,
            chain_id,
            open_time,
            close_time,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: base_quantity,
            quote_volume: quote_value,
            volume_usd: 0.0, // TODO: Calculate from USD price
            count: 1,
            taker_buy_base_volume: 0.0,
            taker_buy_quote_volume: 0.0,
            dirty: true,
        };

        if is_taker_buy {
            bucket.taker_buy_base_volume = base_quantity;
            bucket.taker_buy_quote_volume = quote_value;
        }

        bucket
    }

    /// Update bucket with new trade
    pub fn update(
        &mut self,
        price: f64,
        base_quantity: f64,
        quote_value: f64,
        is_taker_buy: bool,
    ) {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.volume += base_quantity;
        self.quote_volume += quote_value;
        self.count += 1;

        if is_taker_buy {
            self.taker_buy_base_volume += base_quantity;
            self.taker_buy_quote_volume += quote_value;
        }

        self.dirty = true;
    }

    /// Calculate VWAP (Volume Weighted Average Price)
    pub fn vwap(&self) -> f64 {
        if self.volume > 0.0 {
            self.quote_volume / self.volume
        } else {
            0.0
        }
    }

    /// Generate unique ID for this bucket
    pub fn id(&self) -> String {
        format!("{}_{}_{}", self.chain_id, self.pool_id, self.open_time)
    }

    /// Check if a timestamp belongs to this bucket
    pub fn contains(&self, timestamp: u64) -> bool {
        timestamp >= self.open_time && timestamp < self.close_time
    }

    /// Convert to database model
    pub fn to_db_candle(&self) -> DbCandle {
        DbCandle {
            id: self.id(),
            chain_id: self.chain_id,
            open_time: self.open_time as i32,
            close_time: self.close_time as i32,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            quote_volume: self.quote_volume,
            volume_usd: self.volume_usd,
            count: self.count as i32,
            taker_buy_base_volume: self.taker_buy_base_volume,
            taker_buy_quote_volume: self.taker_buy_quote_volume,
            average: self.vwap(),
            pool_id: self.pool_id.clone(),
        }
    }

    /// Convert to Redis message
    pub fn to_redis_message(&self) -> indexer_redis::messages::CandleMessage {
        indexer_redis::messages::CandleMessage {
            pool_id: self.pool_id.clone(),
            interval: self.interval.channel_suffix().to_string(),
            open_time: self.open_time,
            close_time: self.close_time,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            quote_volume: self.quote_volume,
            count: self.count,
        }
    }

    /// Mark as clean (after flushing to DB)
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }
}
