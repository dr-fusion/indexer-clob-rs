use crate::bucket::CandleBucket;
use crate::cache::CandleCache;
use crate::flusher::CandleFlusher;
use crate::interval::CandleInterval;
use indexer_db::DatabasePool;
use indexer_redis::channels::candles_channel;
use indexer_redis::RedisPublisher;
use std::sync::Arc;
use tracing::{debug, error};

/// Trade data for candlestick aggregation
#[derive(Debug, Clone)]
pub struct TradeData {
    pub pool_id: String,
    pub chain_id: i64,
    pub timestamp: u64,
    pub price: f64,
    pub base_quantity: f64,
    pub quote_value: f64,
    pub is_taker_buy: bool,
}

/// Main candlestick aggregator
/// Processes trades and updates all 9 interval buckets
pub struct CandleAggregator {
    cache: Arc<CandleCache>,
    flusher: CandleFlusher,
    publisher: Option<Arc<RedisPublisher>>,
}

impl CandleAggregator {
    /// Create a new aggregator
    pub fn new(db_pool: Arc<DatabasePool>) -> Self {
        let cache = Arc::new(CandleCache::new());
        let flusher = CandleFlusher::new(db_pool, cache.clone());

        Self {
            cache,
            flusher,
            publisher: None,
        }
    }

    /// Set Redis publisher for real-time streaming
    pub fn with_publisher(mut self, publisher: Arc<RedisPublisher>) -> Self {
        self.publisher = Some(publisher);
        self
    }

    /// Start the background flusher
    pub fn start(&mut self, flush_interval_secs: u64, retention_secs: u64) {
        self.flusher.start(flush_interval_secs, retention_secs);
    }

    /// Stop the aggregator and flush all data
    pub async fn stop(&mut self) {
        self.flusher.stop().await;
    }

    /// Process a trade and update all candlestick buckets
    pub async fn process_trade(&self, trade: &TradeData) {
        // Update all 9 intervals
        for interval in CandleInterval::all() {
            let bucket = self.cache.get_or_create(
                &trade.pool_id,
                *interval,
                trade.chain_id,
                trade.timestamp,
                trade.price,
                trade.base_quantity,
                trade.quote_value,
                trade.is_taker_buy,
            );

            // Publish to Redis if publisher is set
            if let Some(publisher) = &self.publisher {
                let channel = candles_channel(interval.channel_suffix(), &trade.pool_id);
                let message = bucket.to_redis_message();

                if let Err(e) = publisher.publish_candle(channel, &message).await {
                    error!(
                        error = %e,
                        interval = %interval,
                        pool_id = %trade.pool_id,
                        "Failed to publish candle update"
                    );
                }
            }
        }

        debug!(
            pool_id = %trade.pool_id,
            price = trade.price,
            quantity = trade.base_quantity,
            "Processed trade for candlesticks"
        );
    }

    /// Get current bucket for a pool and interval
    pub fn get_current_bucket(
        &self,
        pool_id: &str,
        interval: CandleInterval,
    ) -> Option<CandleBucket> {
        self.cache.get_current(pool_id, interval)
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> (usize, usize) {
        let total = self.cache.len();
        let dirty = self.cache.get_dirty_buckets().len();
        (total, dirty)
    }

    /// Force flush a specific pool's buckets
    pub async fn flush_pool(&self, pool_id: &str) -> crate::Result<()> {
        for interval in CandleInterval::all() {
            if let Some(bucket) = self.cache.get_current(pool_id, *interval) {
                self.flusher.flush_bucket(&bucket).await?;
            }
        }
        Ok(())
    }
}

/// Builder for CandleAggregator
pub struct CandleAggregatorBuilder {
    db_pool: Arc<DatabasePool>,
    publisher: Option<Arc<RedisPublisher>>,
    flush_interval_secs: u64,
    retention_secs: u64,
}

impl CandleAggregatorBuilder {
    pub fn new(db_pool: Arc<DatabasePool>) -> Self {
        Self {
            db_pool,
            publisher: None,
            flush_interval_secs: 60,
            retention_secs: 86400, // 24 hours
        }
    }

    pub fn with_publisher(mut self, publisher: Arc<RedisPublisher>) -> Self {
        self.publisher = Some(publisher);
        self
    }

    pub fn with_flush_interval(mut self, secs: u64) -> Self {
        self.flush_interval_secs = secs;
        self
    }

    pub fn with_retention(mut self, secs: u64) -> Self {
        self.retention_secs = secs;
        self
    }

    pub fn build(self) -> CandleAggregator {
        let cache = Arc::new(CandleCache::new());
        let mut flusher = CandleFlusher::new(self.db_pool, cache.clone());
        flusher.start(self.flush_interval_secs, self.retention_secs);

        CandleAggregator {
            cache,
            flusher,
            publisher: self.publisher,
        }
    }
}
