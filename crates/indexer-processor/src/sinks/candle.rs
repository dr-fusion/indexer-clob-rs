use super::traits::{EventSink, SinkEvent};
use async_trait::async_trait;
use indexer_candles::{CandleAggregator, TradeData};
use indexer_core::Result;
use std::sync::Arc;
use tracing::debug;

/// Candle sink for aggregating trade data into candlesticks
pub struct CandleSink {
    aggregator: Arc<CandleAggregator>,
    chain_id: i64,
}

impl CandleSink {
    pub fn new(aggregator: Arc<CandleAggregator>, chain_id: i64) -> Self {
        Self {
            aggregator,
            chain_id,
        }
    }
}

#[async_trait]
impl EventSink for CandleSink {
    async fn handle_event(&self, event: SinkEvent) -> Result<()> {
        // Only process trade events for candle aggregation
        if let SinkEvent::OrderMatched(trade) = event {
            let price: f64 = trade.execution_price.parse().unwrap_or(0.0);
            let quantity: f64 = trade.executed_quantity.parse().unwrap_or(0.0);

            // Skip if we couldn't parse the values
            if price == 0.0 || quantity == 0.0 {
                return Ok(());
            }

            let trade_data = TradeData {
                pool_id: trade.pool_id.clone(),
                chain_id: self.chain_id,
                timestamp: trade.timestamp,
                price,
                base_quantity: quantity,
                quote_value: price * quantity,
                is_taker_buy: trade.taker_side == "BUY",
            };

            debug!(
                pool_id = %trade.pool_id,
                price = price,
                quantity = quantity,
                "Processing trade for candlesticks"
            );

            self.aggregator.process_trade(&trade_data).await;
        }

        Ok(())
    }
}
