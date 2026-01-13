use metrics::{describe_gauge, gauge};

/// Initialize gauge descriptions
pub fn init() {
    describe_gauge!(
        "indexer_current_block",
        "Current block number being processed"
    );
    describe_gauge!(
        "indexer_sync_progress",
        "Sync progress as percentage (0-100)"
    );
    describe_gauge!(
        "indexer_memory_pools",
        "Number of pools in memory"
    );
    describe_gauge!(
        "indexer_memory_orders",
        "Number of orders in memory"
    );
    describe_gauge!(
        "indexer_memory_trades",
        "Number of trades in memory"
    );
    describe_gauge!(
        "indexer_candle_cache_size",
        "Number of candle buckets in cache"
    );
    describe_gauge!(
        "indexer_candle_dirty_count",
        "Number of dirty candle buckets"
    );
    describe_gauge!(
        "indexer_db_connections",
        "Number of database connections"
    );
}

/// Set current block gauge
pub fn set_current_block(block: u64) {
    gauge!("indexer_current_block").set(block as f64);
}

/// Set sync progress gauge
pub fn set_sync_progress(progress: f64) {
    gauge!("indexer_sync_progress").set(progress);
}

/// Set memory pools gauge
pub fn set_memory_pools(count: usize) {
    gauge!("indexer_memory_pools").set(count as f64);
}

/// Set memory orders gauge
pub fn set_memory_orders(count: usize) {
    gauge!("indexer_memory_orders").set(count as f64);
}

/// Set memory trades gauge
pub fn set_memory_trades(count: usize) {
    gauge!("indexer_memory_trades").set(count as f64);
}

/// Set candle cache size gauge
pub fn set_candle_cache_size(count: usize) {
    gauge!("indexer_candle_cache_size").set(count as f64);
}

/// Set candle dirty count gauge
pub fn set_candle_dirty_count(count: usize) {
    gauge!("indexer_candle_dirty_count").set(count as f64);
}

/// Set database connections gauge
pub fn set_db_connections(count: u32) {
    gauge!("indexer_db_connections").set(count as f64);
}
