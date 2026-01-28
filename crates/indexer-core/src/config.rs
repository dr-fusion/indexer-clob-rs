use crate::error::{IndexerError, Result};
use alloy_primitives::Address;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

/// Deployment configuration loaded from JSON file
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct DeploymentConfig {
    pub proxy_poolmanager: Address,
    pub proxy_balancemanager: Address,
    #[serde(rename = "startBlock")]
    pub start_block: u64,
    #[serde(default)]
    pub stablecoins: HashMap<String, Address>,
    #[serde(rename = "feeReceiver")]
    pub fee_receiver: Option<Address>,
}

/// Runtime configuration from environment variables
#[derive(Debug, Clone)]
pub struct EnvConfig {
    pub chain_id: u64,
    pub rpc_url: String,
    pub ws_url: String,
}

/// Complete indexer configuration
#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub chain_id: u64,
    pub rpc_url: String,
    pub ws_url: String,
    pub pool_manager: Address,
    pub balance_manager: Address,
    pub start_block: u64,
    pub stablecoins: HashMap<String, Address>,
    pub fee_receiver: Option<Address>,
    pub sync: SyncConfig,
    /// Enable miniBlocks subscription (MegaETH-specific)
    pub miniblocks_enabled: bool,
    /// RPC verification configuration for catching missed WebSocket events
    pub verification: VerificationConfig,
    /// Telegram notification configuration for alerting on missing events
    pub telegram: TelegramConfig,
}

/// Sync-related configuration
/// Note: Batch sizing and concurrency are now handled by adaptive controllers
/// (AdaptiveBatchController and AdaptiveConcurrencyController) which use AIMD
#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub retry_attempts: u32,
    pub retry_delay_ms: u64,
}

impl SyncConfig {
    pub fn from_env() -> Self {
        let retry_attempts = env::var("SYNC_RETRY_ATTEMPTS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        let retry_delay_ms = env::var("SYNC_RETRY_DELAY_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        Self {
            retry_attempts,
            retry_delay_ms,
        }
    }
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

/// RPC verification configuration for catching missed WebSocket events
#[derive(Debug, Clone)]
pub struct VerificationConfig {
    /// Enable periodic RPC verification (default: true if VERIFICATION_RPC_URL is set)
    pub enabled: bool,
    /// Verification interval in seconds (default: 30)
    pub interval_secs: u64,
    /// Second RPC URL for cross-verification
    pub rpc_url: Option<String>,
}

impl VerificationConfig {
    pub fn from_env() -> Self {
        let rpc_url = env::var("VERIFICATION_RPC_URL").ok().map(|url| {
            // Reuse URL sanitization logic
            let trimmed = url.trim();
            let without_quotes = if trimmed.starts_with('"') && trimmed.ends_with('"') {
                &trimmed[1..trimmed.len() - 1]
            } else if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
                &trimmed[1..trimmed.len() - 1]
            } else {
                trimmed
            };
            without_quotes.to_string()
        });

        let enabled = env::var("VERIFICATION_ENABLED")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(rpc_url.is_some()); // Default: enabled if RPC URL is set

        let interval_secs = env::var("VERIFICATION_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        Self {
            enabled,
            interval_secs,
            rpc_url,
        }
    }
}

impl Default for VerificationConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

/// Telegram notification configuration for alerting on missing events
#[derive(Debug, Clone)]
pub struct TelegramConfig {
    /// Enable Telegram notifications (default: true if bot_token and chat_id are set)
    pub enabled: bool,
    /// Telegram bot token from @BotFather
    pub bot_token: Option<String>,
    /// Chat ID or channel ID to send messages to
    pub chat_id: Option<String>,
}

impl TelegramConfig {
    pub fn from_env() -> Self {
        let bot_token = env::var("TELEGRAM_BOT_TOKEN").ok();
        let chat_id = env::var("TELEGRAM_CHAT_ID").ok();
        let enabled = env::var("TELEGRAM_ENABLED")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(bot_token.is_some() && chat_id.is_some());

        Self {
            enabled,
            bot_token,
            chat_id,
        }
    }

    /// Check if Telegram is fully configured and enabled
    pub fn is_configured(&self) -> bool {
        self.enabled && self.bot_token.is_some() && self.chat_id.is_some()
    }
}

impl Default for TelegramConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl EnvConfig {
    /// Load configuration from environment variables.
    /// Panics if required variables are missing.
    pub fn load() -> Result<Self> {
        let chain_id = env::var("CHAIN_ID")
            .map_err(|_| IndexerError::MissingEnvVar("CHAIN_ID".to_string()))?
            .parse::<u64>()
            .map_err(|_| IndexerError::MissingEnvVar("CHAIN_ID (invalid format)".to_string()))?;

        let rpc_url = Self::sanitize_url(
            env::var("RPC_URL")
                .map_err(|_| IndexerError::MissingEnvVar("RPC_URL".to_string()))?,
        );

        let ws_url = Self::sanitize_url(
            env::var("WS_URL")
                .map_err(|_| IndexerError::MissingEnvVar("WS_URL".to_string()))?,
        );

        // Log the URLs being used (helpful for debugging connection issues)
        eprintln!("[Config] RPC_URL: {}", rpc_url);
        eprintln!("[Config] WS_URL: {}", ws_url);

        Ok(Self {
            chain_id,
            rpc_url,
            ws_url,
        })
    }

    /// Sanitize URL by removing surrounding quotes and whitespace
    fn sanitize_url(url: String) -> String {
        let trimmed = url.trim();
        // Remove surrounding double quotes if present
        let without_quotes = if trimmed.starts_with('"') && trimmed.ends_with('"') {
            &trimmed[1..trimmed.len() - 1]
        } else if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
            &trimmed[1..trimmed.len() - 1]
        } else {
            trimmed
        };
        without_quotes.to_string()
    }
}

impl DeploymentConfig {
    /// Load deployment configuration from JSON file
    pub fn load(chain_id: u64) -> Result<Self> {
        let path = Self::deployment_path(chain_id);
        let content = fs::read_to_string(&path).map_err(|_| {
            IndexerError::DeploymentFileNotFound(path.display().to_string())
        })?;

        serde_json::from_str(&content).map_err(|e| {
            IndexerError::DeploymentParseError(e.to_string())
        })
    }

    fn deployment_path(chain_id: u64) -> PathBuf {
        PathBuf::from(format!("deployments/{}.json", chain_id))
    }
}

impl IndexerConfig {
    /// Load complete configuration from environment and deployment file
    pub fn load() -> Result<Self> {
        let env_config = EnvConfig::load()?;
        let deployment = DeploymentConfig::load(env_config.chain_id)?;

        // MiniBlocks subscription (MegaETH-specific, default: false)
        let miniblocks_enabled = env::var("MINIBLOCKS_ENABLED")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        // RPC verification configuration
        let verification = VerificationConfig::from_env();
        if verification.enabled {
            if let Some(ref url) = verification.rpc_url {
                eprintln!("[Config] VERIFICATION_RPC_URL: {}", url);
                eprintln!(
                    "[Config] Verification enabled with {}s interval",
                    verification.interval_secs
                );
            } else {
                eprintln!("[Config] Verification enabled but no VERIFICATION_RPC_URL set - will use primary RPC only");
            }
        }

        // Telegram notification configuration
        let telegram = TelegramConfig::from_env();
        if telegram.is_configured() {
            eprintln!("[Config] Telegram notifications enabled");
        }

        Ok(Self {
            chain_id: env_config.chain_id,
            rpc_url: env_config.rpc_url,
            ws_url: env_config.ws_url,
            pool_manager: deployment.proxy_poolmanager,
            balance_manager: deployment.proxy_balancemanager,
            start_block: deployment.start_block,
            stablecoins: deployment.stablecoins,
            fee_receiver: deployment.fee_receiver,
            sync: SyncConfig::default(),
            miniblocks_enabled,
            verification,
            telegram,
        })
    }
}
