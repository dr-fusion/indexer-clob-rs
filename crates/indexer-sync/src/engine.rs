use indexer_core::{IndexerConfig, Result};
use indexer_processor::EventProcessor;
use indexer_store::IndexerStore;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

use crate::gap_detector::GapDetector;
use crate::historical::HistoricalSyncer;
use crate::provider::ProviderManager;
use crate::realtime::RealtimeSyncer;
use crate::rpc_verifier::RpcVerifier;

/// Main sync engine that orchestrates historical and real-time sync
pub struct SyncEngine {
    config: IndexerConfig,
    provider: Arc<ProviderManager>,
    store: Arc<IndexerStore>,
    processor: Arc<EventProcessor>,
    /// Shared shutdown flag for graceful termination
    shutdown_flag: Arc<AtomicBool>,
}

impl SyncEngine {
    /// Create a new sync engine
    pub async fn new(config: IndexerConfig, store: Arc<IndexerStore>) -> Result<Self> {
        let provider = Arc::new(
            ProviderManager::new(&config.rpc_url, &config.ws_url).await?,
        );
        let processor = Arc::new(EventProcessor::new(store.clone(), config.clone()));

        Ok(Self {
            config,
            provider,
            store,
            processor,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Get reference to the store
    pub fn store(&self) -> &Arc<IndexerStore> {
        &self.store
    }

    /// Get reference to the event processor
    pub fn processor(&self) -> &Arc<EventProcessor> {
        &self.processor
    }

    /// Run the sync engine
    pub async fn run(&mut self, mut shutdown: broadcast::Receiver<()>) -> Result<()> {
        // Spawn a task to set the shutdown flag when signal is received
        let shutdown_flag = Arc::clone(&self.shutdown_flag);
        let mut shutdown_listener = shutdown.resubscribe();
        tokio::spawn(async move {
            let _ = shutdown_listener.recv().await;
            shutdown_flag.store(true, Ordering::SeqCst);
            info!("Shutdown flag set");
        });

        // Phase 1: Historical sync
        info!(
            start_block = self.config.start_block,
            "Starting historical sync"
        );

        let historical_syncer = HistoricalSyncer::new(
            self.config.clone(),
            self.provider.clone(),
            self.processor.clone(),
            Arc::clone(&self.shutdown_flag),
        );

        // sync_to_head now returns the verified last synced block
        let verified_block = historical_syncer.sync_to_head().await?;

        // Check if shutdown was requested during historical sync
        if self.shutdown_flag.load(Ordering::Relaxed) {
            info!("Shutdown during historical sync, exiting");
            return Ok(());
        }

        // Mark historical sync as complete
        {
            let mut state = self.store.sync_state.write().await;
            state.complete_historical_sync();
        }

        info!(
            verified_block = verified_block,
            "Historical sync complete and verified, switching to real-time mode"
        );

        // Log stats
        {
            let state = self.store.sync_state.read().await;
            info!(
                pools = state.stats.pools_discovered,
                orders = state.stats.orders_indexed,
                trades = state.stats.trades_indexed,
                events = state.stats.total_events_processed,
                "Sync statistics"
            );
        }

        // Phase 2: Real-time sync with gap detection
        let (gap_tx, mut gap_rx) = mpsc::channel::<u64>(100);

        // Initialize real-time syncer with the verified block as starting point
        let mut realtime_syncer = RealtimeSyncer::new(
            self.config.clone(),
            self.provider.clone(),
            self.processor.clone(),
            verified_block,
        );

        let gap_detector = GapDetector::new(
            self.config.clone(),
            self.provider.clone(),
            self.processor.clone(),
        );

        // Spawn RPC verification task (runs in background at configurable intervals)
        let verifier = RpcVerifier::new(
            self.config.clone(),
            self.provider.clone(),
            self.processor.clone(),
            self.store.clone(),
            Arc::clone(&self.shutdown_flag),
        );
        let verification_handle = tokio::spawn(async move {
            if let Err(e) = verifier.run().await {
                error!(error = %e, "RPC verification task failed");
            }
        });

        loop {
            select! {
                _ = shutdown.recv() => {
                    info!("Shutdown signal received");
                    verification_handle.abort();
                    break;
                }

                result = realtime_syncer.run(gap_tx.clone()) => {
                    match result {
                        Ok(_) => info!("Real-time syncer completed"),
                        Err(e) => {
                            error!(error = %e, "Real-time syncer error");
                            // Wait before reconnecting
                            tokio::time::sleep(Duration::from_secs(5)).await;
                        }
                    }
                }

                Some(gap_block) = gap_rx.recv() => {
                    warn!(block = gap_block, "Gap detected, filling...");
                    if let Err(e) = gap_detector.fill_gap(gap_block).await {
                        error!(error = %e, "Failed to fill gap");
                    }
                }
            }
        }

        info!("Sync engine shutdown complete");
        Ok(())
    }

    /// Print current sync status
    pub async fn print_status(&self) {
        let state = self.store.sync_state.read().await;
        let pools = self.store.pools.count();
        let orders = self.store.orders.count();
        let trades = self.store.trades.count();

        info!(
            mode = ?state.mode,
            last_block = state.last_synced_block,
            pools = pools,
            orders = orders,
            trades = trades,
            events = state.stats.total_events_processed,
            "Current status"
        );
    }
}
