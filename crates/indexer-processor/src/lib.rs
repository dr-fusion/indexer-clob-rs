mod handlers;
mod pipeline;
pub mod sinks;

pub use pipeline::EventProcessor;
pub use sinks::{CandleSink, CompositeSink, DatabaseSink, EventSink, RedisSink, SinkEvent};
