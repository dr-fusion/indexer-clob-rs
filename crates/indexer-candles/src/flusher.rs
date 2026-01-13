use crate::bucket::CandleBucket;
use crate::cache::CandleCache;
use indexer_db::repositories::CandleRepository;
use indexer_db::DatabasePool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

/// Lazy flusher for candlestick data
/// Writes buckets to database when they close or on periodic intervals
pub struct CandleFlusher {
    db_pool: Arc<DatabasePool>,
    cache: Arc<CandleCache>,
    shutdown_sender: Option<mpsc::Sender<()>>,
}

impl CandleFlusher {
    /// Create a new flusher
    pub fn new(db_pool: Arc<DatabasePool>, cache: Arc<CandleCache>) -> Self {
        Self {
            db_pool,
            cache,
            shutdown_sender: None,
        }
    }

    /// Start the background flusher task
    pub fn start(&mut self, flush_interval_secs: u64, retention_secs: u64) {
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        self.shutdown_sender = Some(shutdown_tx);

        let db_pool = self.db_pool.clone();
        let cache = self.cache.clone();

        tokio::spawn(Self::flush_loop(
            db_pool,
            cache,
            shutdown_rx,
            flush_interval_secs,
            retention_secs,
        ));
    }

    /// Stop the flusher and flush remaining data
    pub async fn stop(&mut self) {
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(()).await;
        }
    }

    /// Background flush loop
    async fn flush_loop(
        db_pool: Arc<DatabasePool>,
        cache: Arc<CandleCache>,
        mut shutdown_rx: mpsc::Receiver<()>,
        flush_interval_secs: u64,
        retention_secs: u64,
    ) {
        let flush_interval = Duration::from_secs(flush_interval_secs);

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Candle flusher shutting down, final flush...");
                    Self::flush_all(&db_pool, &cache).await;
                    break;
                }
                _ = tokio::time::sleep(flush_interval) => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    // Flush closed buckets
                    Self::flush_closed(&db_pool, &cache, now).await;

                    // Cleanup old buckets from cache
                    cache.cleanup_old_buckets(now, retention_secs);
                }
            }
        }
    }

    /// Flush closed buckets to database
    async fn flush_closed(db_pool: &DatabasePool, cache: &CandleCache, current_time: u64) {
        let closed = cache.get_closed_buckets(current_time);

        if closed.is_empty() {
            return;
        }

        debug!(count = closed.len(), "Flushing closed candle buckets");

        for bucket in closed {
            if let Err(e) = Self::write_bucket(db_pool, &bucket).await {
                error!(
                    error = %e,
                    pool_id = %bucket.pool_id,
                    interval = %bucket.interval,
                    "Failed to flush candle bucket"
                );
            } else {
                // Mark clean and potentially remove
                cache.mark_clean(&bucket.pool_id, bucket.interval, bucket.open_time);

                // Remove if old enough
                if bucket.close_time + 3600 < current_time {
                    cache.remove(&bucket.pool_id, bucket.interval, bucket.open_time);
                }
            }
        }
    }

    /// Flush all dirty buckets
    async fn flush_all(db_pool: &DatabasePool, cache: &CandleCache) {
        let dirty = cache.get_dirty_buckets();

        if dirty.is_empty() {
            return;
        }

        info!(count = dirty.len(), "Flushing all dirty candle buckets");

        for bucket in dirty {
            if let Err(e) = Self::write_bucket(db_pool, &bucket).await {
                error!(
                    error = %e,
                    pool_id = %bucket.pool_id,
                    interval = %bucket.interval,
                    "Failed to flush candle bucket"
                );
            } else {
                cache.mark_clean(&bucket.pool_id, bucket.interval, bucket.open_time);
            }
        }
    }

    /// Write a single bucket to database
    async fn write_bucket(db_pool: &DatabasePool, bucket: &CandleBucket) -> crate::Result<()> {
        let db_candle = bucket.to_db_candle();
        let db_interval = bucket.interval.to_db_interval();

        CandleRepository::upsert(db_pool.inner(), db_interval, &db_candle)
            .await
            .map_err(|e| crate::CandleError::Database(e.to_string()))?;

        debug!(
            pool_id = %bucket.pool_id,
            interval = %bucket.interval,
            open_time = bucket.open_time,
            "Flushed candle bucket to database"
        );

        Ok(())
    }

    /// Force flush a specific bucket immediately
    pub async fn flush_bucket(&self, bucket: &CandleBucket) -> crate::Result<()> {
        Self::write_bucket(&self.db_pool, bucket).await?;
        self.cache
            .mark_clean(&bucket.pool_id, bucket.interval, bucket.open_time);
        Ok(())
    }
}
