use crate::connection::RedisConnection;
use crate::messages::StreamMessage;
use crate::{RedisError, Result};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Redis stream publisher with async fire-and-forget support and batch publishing
pub struct RedisPublisher {
    connection: Arc<RedisConnection>,
    async_sender: Option<mpsc::Sender<StreamMessage>>,
}

impl RedisPublisher {
    /// Create a new publisher
    pub fn new(connection: Arc<RedisConnection>) -> Self {
        let async_mode = connection.config().async_mode;
        let queue_capacity = connection.config().queue_capacity;
        let batch_size = connection.config().batch_size;

        let async_sender = if async_mode {
            let (sender, receiver) = mpsc::channel(queue_capacity);
            let conn = connection.clone();

            info!(
                queue_capacity = queue_capacity,
                batch_size = batch_size,
                "Redis async publisher initialized"
            );

            // Spawn background publisher task with batching
            tokio::spawn(Self::async_publisher_loop(conn, receiver, batch_size));

            Some(sender)
        } else {
            None
        };

        Self {
            connection,
            async_sender,
        }
    }

    /// Publish a message to a channel (goes to stream)
    pub async fn publish<T: Serialize>(&self, channel: String, data: T) -> Result<()> {
        let message = StreamMessage::new(channel, data)?;

        if let Some(sender) = &self.async_sender {
            // Fire and forget mode
            if sender.try_send(message).is_err() {
                warn!("Redis publish queue full, dropping message");
            }
            Ok(())
        } else {
            // Synchronous mode
            self.publish_sync(message).await
        }
    }

    /// Publish directly (blocking)
    async fn publish_sync(&self, message: StreamMessage) -> Result<()> {
        let mut conn = self.connection.get_connection();
        let stream_key = &self.connection.config().stream_key;
        let max_len = self.connection.config().max_len;

        // Convert message to fields for XADD
        let fields: Vec<(&str, String)> = vec![
            ("channel", message.channel),
            ("data", message.data),
            ("source", message.source),
            ("timestamp", message.timestamp.to_string()),
        ];

        // XADD with MAXLEN
        redis::cmd("XADD")
            .arg(stream_key)
            .arg("MAXLEN")
            .arg("~")
            .arg(max_len)
            .arg("*")
            .arg(&fields[0].0)
            .arg(&fields[0].1)
            .arg(&fields[1].0)
            .arg(&fields[1].1)
            .arg(&fields[2].0)
            .arg(&fields[2].1)
            .arg(&fields[3].0)
            .arg(&fields[3].1)
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| RedisError::Publish(e.to_string()))?;

        debug!(channel = %fields[0].1, "Published message to Redis");
        Ok(())
    }

    /// Background publisher loop for async mode with batching
    async fn async_publisher_loop(
        connection: Arc<RedisConnection>,
        mut receiver: mpsc::Receiver<StreamMessage>,
        batch_size: usize,
    ) {
        let stream_key = connection.config().stream_key.clone();
        let max_len = connection.config().max_len;
        let mut batch: Vec<StreamMessage> = Vec::with_capacity(batch_size);

        loop {
            // Try to fill batch without blocking
            batch.clear();

            // Wait for first message
            match receiver.recv().await {
                Some(msg) => batch.push(msg),
                None => break, // Channel closed
            }

            // Collect more messages if available (non-blocking)
            while batch.len() < batch_size {
                match receiver.try_recv() {
                    Ok(msg) => batch.push(msg),
                    Err(_) => break, // No more messages ready
                }
            }

            // Publish batch using pipeline
            if !batch.is_empty() {
                let mut conn = connection.get_connection();
                let mut pipe = redis::pipe();

                for message in &batch {
                    pipe.cmd("XADD")
                        .arg(&stream_key)
                        .arg("MAXLEN")
                        .arg("~")
                        .arg(max_len)
                        .arg("*")
                        .arg("channel")
                        .arg(&message.channel)
                        .arg("data")
                        .arg(&message.data)
                        .arg("source")
                        .arg(&message.source)
                        .arg("timestamp")
                        .arg(message.timestamp.to_string())
                        .ignore();
                }

                let result: std::result::Result<(), redis::RedisError> =
                    pipe.query_async(&mut conn).await;

                if let Err(e) = result {
                    error!(error = %e, batch_size = batch.len(), "Failed to publish batch to Redis");
                } else {
                    debug!(batch_size = batch.len(), "Published batch to Redis");
                }
            }
        }
    }

    /// Publish trade event
    pub async fn publish_trade(&self, channel: String, trade: &crate::messages::TradeMessage) -> Result<()> {
        self.publish(channel, trade).await
    }

    /// Publish order event
    pub async fn publish_order(&self, channel: String, order: &crate::messages::OrderMessage) -> Result<()> {
        self.publish(channel, order).await
    }

    /// Publish candle update
    pub async fn publish_candle(&self, channel: String, candle: &crate::messages::CandleMessage) -> Result<()> {
        self.publish(channel, candle).await
    }

    /// Publish balance update
    pub async fn publish_balance(&self, channel: String, balance: &crate::messages::BalanceMessage) -> Result<()> {
        self.publish(channel, balance).await
    }
}

impl Clone for RedisPublisher {
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            async_sender: self.async_sender.clone(),
        }
    }
}
