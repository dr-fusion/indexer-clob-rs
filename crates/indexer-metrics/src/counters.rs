use metrics::{counter, describe_counter};

/// Initialize counter descriptions
pub fn init() {
    describe_counter!(
        "indexer_events_processed_total",
        "Total number of events processed"
    );
    describe_counter!(
        "indexer_blocks_processed_total",
        "Total number of blocks processed"
    );
    describe_counter!(
        "indexer_trades_total",
        "Total number of trades indexed"
    );
    describe_counter!(
        "indexer_orders_total",
        "Total number of orders indexed"
    );
    describe_counter!(
        "indexer_pools_total",
        "Total number of pools discovered"
    );
    describe_counter!(
        "indexer_db_writes_total",
        "Total number of database writes"
    );
    describe_counter!(
        "indexer_redis_publishes_total",
        "Total number of Redis publishes"
    );
    describe_counter!(
        "indexer_errors_total",
        "Total number of errors"
    );
}

/// Increment events processed counter
pub fn events_processed(count: u64) {
    counter!("indexer_events_processed_total").increment(count);
}

/// Increment blocks processed counter
pub fn blocks_processed(count: u64) {
    counter!("indexer_blocks_processed_total").increment(count);
}

/// Increment trades counter
pub fn trades_indexed(count: u64) {
    counter!("indexer_trades_total").increment(count);
}

/// Increment orders counter
pub fn orders_indexed(count: u64) {
    counter!("indexer_orders_total").increment(count);
}

/// Increment pools counter
pub fn pools_discovered(count: u64) {
    counter!("indexer_pools_total").increment(count);
}

/// Increment database writes counter
pub fn db_writes(count: u64) {
    counter!("indexer_db_writes_total").increment(count);
}

/// Increment Redis publishes counter
pub fn redis_publishes(count: u64) {
    counter!("indexer_redis_publishes_total").increment(count);
}

/// Increment errors counter
pub fn errors(count: u64, error_type: &str) {
    counter!("indexer_errors_total", "type" => error_type.to_string()).increment(count);
}
