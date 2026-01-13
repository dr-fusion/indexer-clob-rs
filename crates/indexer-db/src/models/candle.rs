use serde::{Deserialize, Serialize};
use sqlx::FromRow;

/// Candlestick interval enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CandleInterval {
    Minute1,
    Minute5,
    Minute15,
    Hour1,
    Hour2,
    Hour4,
    Day1,
    Week1,
    Month1,
}

impl CandleInterval {
    /// Get interval duration in seconds
    pub fn duration_secs(&self) -> u64 {
        match self {
            CandleInterval::Minute1 => 60,
            CandleInterval::Minute5 => 300,
            CandleInterval::Minute15 => 900,
            CandleInterval::Hour1 => 3600,
            CandleInterval::Hour2 => 7200,
            CandleInterval::Hour4 => 14400,
            CandleInterval::Day1 => 86400,
            CandleInterval::Week1 => 604800,
            CandleInterval::Month1 => 2629746, // Average month
        }
    }

    /// Get table name for this interval
    pub fn table_name(&self) -> &'static str {
        match self {
            CandleInterval::Minute1 => "minute_buckets",
            CandleInterval::Minute5 => "five_minute_buckets",
            CandleInterval::Minute15 => "fifteen_minute_buckets",
            CandleInterval::Hour1 => "hour_buckets",
            CandleInterval::Hour2 => "two_hour_buckets",
            CandleInterval::Hour4 => "four_hour_buckets",
            CandleInterval::Day1 => "daily_buckets",
            CandleInterval::Week1 => "weekly_buckets",
            CandleInterval::Month1 => "monthly_buckets",
        }
    }

    /// Get Redis channel suffix for this interval
    pub fn channel_suffix(&self) -> &'static str {
        match self {
            CandleInterval::Minute1 => "1m",
            CandleInterval::Minute5 => "5m",
            CandleInterval::Minute15 => "15m",
            CandleInterval::Hour1 => "1h",
            CandleInterval::Hour2 => "2h",
            CandleInterval::Hour4 => "4h",
            CandleInterval::Day1 => "1d",
            CandleInterval::Week1 => "1w",
            CandleInterval::Month1 => "1M",
        }
    }

    /// Get all intervals
    pub fn all() -> &'static [CandleInterval] {
        &[
            CandleInterval::Minute1,
            CandleInterval::Minute5,
            CandleInterval::Minute15,
            CandleInterval::Hour1,
            CandleInterval::Hour2,
            CandleInterval::Hour4,
            CandleInterval::Day1,
            CandleInterval::Week1,
            CandleInterval::Month1,
        ]
    }

    /// Calculate bucket open time for a given timestamp
    pub fn bucket_open_time(&self, timestamp: u64) -> u64 {
        let duration = self.duration_secs();
        (timestamp / duration) * duration
    }

    /// Calculate bucket close time for a given timestamp
    pub fn bucket_close_time(&self, timestamp: u64) -> u64 {
        self.bucket_open_time(timestamp) + self.duration_secs()
    }
}

/// Database model for candlestick buckets
/// Used for all 9 interval tables (same schema)
#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct DbCandle {
    /// Primary key: chainId_poolId_openTime
    pub id: String,
    /// Chain ID
    pub chain_id: i64,
    /// Bucket open timestamp
    pub open_time: i32,
    /// Bucket close timestamp
    pub close_time: i32,
    /// Open price
    pub open: f64,
    /// High price
    pub high: f64,
    /// Low price
    pub low: f64,
    /// Close price
    pub close: f64,
    /// Base volume
    pub volume: f64,
    /// Quote volume
    pub quote_volume: f64,
    /// Volume in USD
    pub volume_usd: f64,
    /// Trade count in this bucket
    pub count: i32,
    /// Taker buy base volume
    pub taker_buy_base_volume: f64,
    /// Taker buy quote volume
    pub taker_buy_quote_volume: f64,
    /// VWAP (Volume Weighted Average Price)
    pub average: f64,
    /// Pool ID (hex)
    pub pool_id: String,
}

impl DbCandle {
    /// Create composite ID
    pub fn make_id(chain_id: i64, pool_id: &str, open_time: u64) -> String {
        format!("{}_{}_{}", chain_id, pool_id, open_time)
    }
}
