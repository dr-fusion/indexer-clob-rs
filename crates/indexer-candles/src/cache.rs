use crate::bucket::CandleBucket;
use crate::interval::CandleInterval;
use dashmap::DashMap;

/// Key for candle cache: (pool_id, interval, open_time)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CandleKey {
    pub pool_id: String,
    pub interval: CandleInterval,
    pub open_time: u64,
}

impl CandleKey {
    pub fn new(pool_id: String, interval: CandleInterval, open_time: u64) -> Self {
        Self {
            pool_id,
            interval,
            open_time,
        }
    }
}

/// In-memory cache for active candlestick buckets
#[derive(Debug)]
pub struct CandleCache {
    /// Active buckets: key -> bucket
    buckets: DashMap<CandleKey, CandleBucket>,

    /// Track current bucket for each (pool_id, interval) for fast lookup
    current_buckets: DashMap<(String, CandleInterval), u64>,
}

impl CandleCache {
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
            current_buckets: DashMap::new(),
        }
    }

    /// Get or create bucket for a trade
    pub fn get_or_create(
        &self,
        pool_id: &str,
        interval: CandleInterval,
        chain_id: i64,
        timestamp: u64,
        price: f64,
        base_quantity: f64,
        quote_value: f64,
        is_taker_buy: bool,
    ) -> CandleBucket {
        let open_time = interval.bucket_open_time(timestamp);
        let key = CandleKey::new(pool_id.to_string(), interval, open_time);

        // Check if we have this bucket
        if let Some(mut entry) = self.buckets.get_mut(&key) {
            entry.update(price, base_quantity, quote_value, is_taker_buy);
            return entry.clone();
        }

        // Check if there's a previous bucket to close
        let pool_key = (pool_id.to_string(), interval);
        if let Some(prev_open_time) = self.current_buckets.get(&pool_key) {
            if *prev_open_time != open_time {
                // Previous bucket exists, mark it for flushing
                let prev_key = CandleKey::new(pool_id.to_string(), interval, *prev_open_time);
                if let Some(mut prev_bucket) = self.buckets.get_mut(&prev_key) {
                    prev_bucket.dirty = true;
                }
            }
        }

        // Create new bucket
        let bucket = CandleBucket::new(
            pool_id.to_string(),
            interval,
            chain_id,
            timestamp,
            price,
            base_quantity,
            quote_value,
            is_taker_buy,
        );

        self.buckets.insert(key, bucket.clone());
        self.current_buckets.insert(pool_key, open_time);

        bucket
    }

    /// Get current bucket for a pool and interval
    pub fn get_current(&self, pool_id: &str, interval: CandleInterval) -> Option<CandleBucket> {
        let pool_key = (pool_id.to_string(), interval);
        if let Some(open_time) = self.current_buckets.get(&pool_key) {
            let key = CandleKey::new(pool_id.to_string(), interval, *open_time);
            return self.buckets.get(&key).map(|b| b.clone());
        }
        None
    }

    /// Get all dirty buckets (for flushing)
    pub fn get_dirty_buckets(&self) -> Vec<CandleBucket> {
        self.buckets
            .iter()
            .filter(|entry| entry.value().dirty)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get closed buckets (buckets where current time > close_time)
    pub fn get_closed_buckets(&self, current_time: u64) -> Vec<CandleBucket> {
        self.buckets
            .iter()
            .filter(|entry| entry.value().close_time <= current_time)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Mark bucket as clean
    pub fn mark_clean(&self, pool_id: &str, interval: CandleInterval, open_time: u64) {
        let key = CandleKey::new(pool_id.to_string(), interval, open_time);
        if let Some(mut entry) = self.buckets.get_mut(&key) {
            entry.mark_clean();
        }
    }

    /// Remove bucket from cache
    pub fn remove(&self, pool_id: &str, interval: CandleInterval, open_time: u64) {
        let key = CandleKey::new(pool_id.to_string(), interval, open_time);
        self.buckets.remove(&key);
    }

    /// Remove all closed buckets older than retention time
    pub fn cleanup_old_buckets(&self, current_time: u64, retention_secs: u64) {
        let cutoff = current_time.saturating_sub(retention_secs);

        let keys_to_remove: Vec<CandleKey> = self
            .buckets
            .iter()
            .filter(|entry| entry.value().close_time < cutoff && !entry.value().dirty)
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            self.buckets.remove(&key);
        }
    }

    /// Get total bucket count
    pub fn len(&self) -> usize {
        self.buckets.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }
}

impl Default for CandleCache {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CandleCache {
    fn clone(&self) -> Self {
        let new_cache = Self::new();
        for entry in self.buckets.iter() {
            new_cache.buckets.insert(entry.key().clone(), entry.value().clone());
        }
        for entry in self.current_buckets.iter() {
            new_cache.current_buckets.insert(entry.key().clone(), *entry.value());
        }
        new_cache
    }
}
