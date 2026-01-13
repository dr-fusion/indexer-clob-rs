use serde::{Deserialize, Serialize};

/// Candlestick interval enum - 9 intervals matching Ponder
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
    pub const fn duration_secs(&self) -> u64 {
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

    /// Get table name for database
    pub const fn table_name(&self) -> &'static str {
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

    /// Get Redis channel suffix
    pub const fn channel_suffix(&self) -> &'static str {
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
    pub const fn all() -> &'static [CandleInterval] {
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

    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "1m" | "minute" | "MINUTE_1" => Some(CandleInterval::Minute1),
            "5m" | "MINUTE_5" => Some(CandleInterval::Minute5),
            "15m" | "MINUTE_15" => Some(CandleInterval::Minute15),
            "1h" | "hour" | "HOUR_1" => Some(CandleInterval::Hour1),
            "2h" | "HOUR_2" => Some(CandleInterval::Hour2),
            "4h" | "HOUR_4" => Some(CandleInterval::Hour4),
            "1d" | "day" | "DAY_1" => Some(CandleInterval::Day1),
            "1w" | "week" | "WEEK_1" => Some(CandleInterval::Week1),
            "1M" | "month" | "MONTH_1" => Some(CandleInterval::Month1),
            _ => None,
        }
    }

    /// Convert to db interval enum
    pub fn to_db_interval(&self) -> indexer_db::models::CandleInterval {
        match self {
            CandleInterval::Minute1 => indexer_db::models::CandleInterval::Minute1,
            CandleInterval::Minute5 => indexer_db::models::CandleInterval::Minute5,
            CandleInterval::Minute15 => indexer_db::models::CandleInterval::Minute15,
            CandleInterval::Hour1 => indexer_db::models::CandleInterval::Hour1,
            CandleInterval::Hour2 => indexer_db::models::CandleInterval::Hour2,
            CandleInterval::Hour4 => indexer_db::models::CandleInterval::Hour4,
            CandleInterval::Day1 => indexer_db::models::CandleInterval::Day1,
            CandleInterval::Week1 => indexer_db::models::CandleInterval::Week1,
            CandleInterval::Month1 => indexer_db::models::CandleInterval::Month1,
        }
    }
}

impl std::fmt::Display for CandleInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.channel_suffix())
    }
}
