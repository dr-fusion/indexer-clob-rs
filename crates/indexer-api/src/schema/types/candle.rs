use async_graphql::{Enum, SimpleObject};
use indexer_db::models::DbCandle;

/// GraphQL Candle interval enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Enum)]
pub enum CandleIntervalInput {
    #[graphql(name = "MINUTE_1")]
    Minute1,
    #[graphql(name = "MINUTE_5")]
    Minute5,
    #[graphql(name = "MINUTE_15")]
    Minute15,
    #[graphql(name = "HOUR_1")]
    Hour1,
    #[graphql(name = "HOUR_2")]
    Hour2,
    #[graphql(name = "HOUR_4")]
    Hour4,
    #[graphql(name = "DAY_1")]
    Day1,
    #[graphql(name = "WEEK_1")]
    Week1,
    #[graphql(name = "MONTH_1")]
    Month1,
}

impl CandleIntervalInput {
    pub fn to_db_interval(&self) -> indexer_db::models::CandleInterval {
        match self {
            CandleIntervalInput::Minute1 => indexer_db::models::CandleInterval::Minute1,
            CandleIntervalInput::Minute5 => indexer_db::models::CandleInterval::Minute5,
            CandleIntervalInput::Minute15 => indexer_db::models::CandleInterval::Minute15,
            CandleIntervalInput::Hour1 => indexer_db::models::CandleInterval::Hour1,
            CandleIntervalInput::Hour2 => indexer_db::models::CandleInterval::Hour2,
            CandleIntervalInput::Hour4 => indexer_db::models::CandleInterval::Hour4,
            CandleIntervalInput::Day1 => indexer_db::models::CandleInterval::Day1,
            CandleIntervalInput::Week1 => indexer_db::models::CandleInterval::Week1,
            CandleIntervalInput::Month1 => indexer_db::models::CandleInterval::Month1,
        }
    }
}

/// GraphQL Candle type
#[derive(Debug, Clone, SimpleObject)]
pub struct GqlCandle {
    pub id: String,
    pub chain_id: i64,
    pub pool_id: String,
    pub open_time: i32,
    pub close_time: i32,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub volume_usd: f64,
    pub count: i32,
    pub taker_buy_base_volume: f64,
    pub taker_buy_quote_volume: f64,
    pub average: f64,
}

impl From<DbCandle> for GqlCandle {
    fn from(candle: DbCandle) -> Self {
        Self {
            id: candle.id,
            chain_id: candle.chain_id,
            pool_id: candle.pool_id,
            open_time: candle.open_time,
            close_time: candle.close_time,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
            volume: candle.volume,
            quote_volume: candle.quote_volume,
            volume_usd: candle.volume_usd,
            count: candle.count,
            taker_buy_base_volume: candle.taker_buy_base_volume,
            taker_buy_quote_volume: candle.taker_buy_quote_volume,
            average: candle.average,
        }
    }
}
