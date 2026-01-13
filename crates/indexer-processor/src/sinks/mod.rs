mod candle;
mod database;
mod redis;
pub mod traits;

pub use candle::CandleSink;
pub use database::DatabaseSink;
pub use redis::RedisSink;
pub use traits::{EventSink, SinkEvent};

use indexer_core::Result;
use std::sync::Arc;

/// Composite sink that fans out to multiple sinks
pub struct CompositeSink {
    sinks: Vec<Arc<dyn EventSink>>,
}

impl CompositeSink {
    pub fn new() -> Self {
        Self { sinks: Vec::new() }
    }

    pub fn add_sink(&mut self, sink: Arc<dyn EventSink>) {
        self.sinks.push(sink);
    }

    pub fn with_sink(mut self, sink: Arc<dyn EventSink>) -> Self {
        self.sinks.push(sink);
        self
    }

    pub async fn emit(&self, event: SinkEvent) -> Result<()> {
        for sink in &self.sinks {
            sink.handle_event(event.clone()).await?;
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.sinks.is_empty()
    }
}

impl Default for CompositeSink {
    fn default() -> Self {
        Self::new()
    }
}
