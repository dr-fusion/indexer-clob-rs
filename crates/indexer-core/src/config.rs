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
            .unwrap_or(3);

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

impl EnvConfig {
    /// Load configuration from environment variables.
    /// Panics if required variables are missing.
    pub fn load() -> Result<Self> {
        let chain_id = env::var("CHAIN_ID")
            .map_err(|_| IndexerError::MissingEnvVar("CHAIN_ID".to_string()))?
            .parse::<u64>()
            .map_err(|_| IndexerError::MissingEnvVar("CHAIN_ID (invalid format)".to_string()))?;

        let rpc_url = env::var("RPC_URL")
            .map_err(|_| IndexerError::MissingEnvVar("RPC_URL".to_string()))?;

        let ws_url = env::var("WS_URL")
            .map_err(|_| IndexerError::MissingEnvVar("WS_URL".to_string()))?;

        Ok(Self {
            chain_id,
            rpc_url,
            ws_url,
        })
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
        })
    }
}
