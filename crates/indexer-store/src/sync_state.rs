use std::collections::BTreeSet;

/// Current sync mode
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SyncMode {
    #[default]
    Historical,
    Realtime,
    GapFilling,
}

/// Sync statistics
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    pub total_blocks_processed: u64,
    pub total_events_processed: u64,
    pub pools_discovered: u64,
    pub orders_indexed: u64,
    pub trades_indexed: u64,
    pub last_block_timestamp: Option<u64>,
}

/// Sync state tracking
#[derive(Debug, Clone, Default)]
pub struct SyncState {
    /// Last fully synced block number
    pub last_synced_block: u64,

    /// Last synced miniblock index within current EVM block
    pub last_miniblock_index: Option<u32>,

    /// Blocks with known gaps (need reprocessing)
    pub gap_blocks: BTreeSet<u64>,

    /// Whether historical sync is complete
    pub historical_sync_complete: bool,

    /// Current sync mode
    pub mode: SyncMode,

    /// Statistics
    pub stats: SyncStats,
}

impl SyncState {
    pub fn new(start_block: u64) -> Self {
        Self {
            last_synced_block: start_block.saturating_sub(1),
            ..Default::default()
        }
    }

    /// Update last synced block
    pub fn set_last_synced_block(&mut self, block: u64) {
        self.last_synced_block = block;
        self.stats.total_blocks_processed += 1;
    }

    /// Mark historical sync as complete
    pub fn complete_historical_sync(&mut self) {
        self.historical_sync_complete = true;
        self.mode = SyncMode::Realtime;
    }

    /// Record a gap that needs filling
    pub fn record_gap(&mut self, block: u64) {
        self.gap_blocks.insert(block);
    }

    /// Mark a gap as filled
    pub fn fill_gap(&mut self, block: u64) {
        self.gap_blocks.remove(&block);
    }

    /// Check if there are gaps to fill
    pub fn has_gaps(&self) -> bool {
        !self.gap_blocks.is_empty()
    }

    /// Increment event counter
    pub fn record_event(&mut self) {
        self.stats.total_events_processed += 1;
    }

    /// Increment pool counter
    pub fn record_pool(&mut self) {
        self.stats.pools_discovered += 1;
    }

    /// Increment order counter
    pub fn record_order(&mut self) {
        self.stats.orders_indexed += 1;
    }

    /// Increment trade counter
    pub fn record_trade(&mut self) {
        self.stats.trades_indexed += 1;
    }

    /// Get last synced block
    pub fn last_synced_block(&self) -> u64 {
        self.last_synced_block
    }

    /// Check if currently syncing (not in realtime mode)
    pub fn is_syncing(&self) -> bool {
        !matches!(self.mode, SyncMode::Realtime) || !self.historical_sync_complete
    }
}
