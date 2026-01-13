use metrics::{describe_histogram, histogram};
use std::time::Duration;

/// Initialize histogram descriptions
pub fn init() {
    describe_histogram!(
        "indexer_event_processing_duration_seconds",
        "Time to process a single event"
    );
    describe_histogram!(
        "indexer_batch_processing_duration_seconds",
        "Time to process a batch of events"
    );
    describe_histogram!(
        "indexer_db_write_duration_seconds",
        "Time for database write operations"
    );
    describe_histogram!(
        "indexer_redis_publish_duration_seconds",
        "Time for Redis publish operations"
    );
    describe_histogram!(
        "indexer_rpc_request_duration_seconds",
        "Time for RPC requests"
    );
}

/// Record event processing duration
pub fn event_processing_duration(duration: Duration) {
    histogram!("indexer_event_processing_duration_seconds").record(duration.as_secs_f64());
}

/// Record batch processing duration
pub fn batch_processing_duration(duration: Duration) {
    histogram!("indexer_batch_processing_duration_seconds").record(duration.as_secs_f64());
}

/// Record database write duration
pub fn db_write_duration(duration: Duration) {
    histogram!("indexer_db_write_duration_seconds").record(duration.as_secs_f64());
}

/// Record Redis publish duration
pub fn redis_publish_duration(duration: Duration) {
    histogram!("indexer_redis_publish_duration_seconds").record(duration.as_secs_f64());
}

/// Record RPC request duration
pub fn rpc_request_duration(duration: Duration, method: &str) {
    histogram!("indexer_rpc_request_duration_seconds", "method" => method.to_string())
        .record(duration.as_secs_f64());
}
