//! Telegram notification module for alerting on missing events
//!
//! Sends alerts via Telegram Bot API when RPC verification detects
//! events that were missed by WebSocket.

use reqwest::Client;
use tracing::{debug, warn};

/// Telegram notifier for sending alerts
pub struct TelegramNotifier {
    client: Client,
    bot_token: String,
    chat_id: String,
}

impl TelegramNotifier {
    /// Create a new TelegramNotifier with the given bot token and chat ID
    pub fn new(bot_token: String, chat_id: String) -> Self {
        Self {
            client: Client::new(),
            bot_token,
            chat_id,
        }
    }

    /// Send a message to the configured Telegram chat
    ///
    /// Uses HTML parse mode for formatting. Does not fail on error,
    /// just logs a warning - we don't want notification failures to
    /// affect the indexer.
    pub async fn send_message(&self, message: &str) {
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token
        );

        let params = [
            ("chat_id", self.chat_id.as_str()),
            ("text", message),
            ("parse_mode", "HTML"),
        ];

        match self.client.post(&url).form(&params).send().await {
            Ok(resp) if resp.status().is_success() => {
                debug!("Telegram notification sent successfully");
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                warn!(
                    status = %status,
                    body = %body,
                    "Telegram API returned error"
                );
            }
            Err(e) => {
                warn!(error = %e, "Failed to send Telegram notification");
            }
        }
    }
}
