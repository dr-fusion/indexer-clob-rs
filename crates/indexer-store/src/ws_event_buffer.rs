//! WebSocket event buffer for RPC verification
//!
//! Stores event content identifiers received via WebSocket,
//! organized by block number, for comparison with RPC-fetched events.
//!
//! Uses content-based comparison (tx_hash + address + topics + data) instead
//! of log_index, because MiniBlocks may have different log_index values than
//! the finalized EVM block.

use alloy::rpc::types::Log;
use alloy_primitives::{keccak256, Address, B256};
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

/// Legacy event identifier - kept for backwards compatibility
/// Only stores tx_hash and log_index
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventId {
    pub tx_hash: B256,
    pub log_index: u64,
}

impl EventId {
    pub fn new(tx_hash: B256, log_index: u64) -> Self {
        Self { tx_hash, log_index }
    }
}

/// Content-based event identifier for WebSocket verification
///
/// Identifies events by their actual content rather than position (log_index),
/// which may differ between MiniBlocks and finalized EVM blocks.
///
/// Fields:
/// - tx_hash: Transaction hash (stable across miniBlock and finalized block)
/// - address: Contract address that emitted the event
/// - topics_hash: Keccak256 hash of all topics concatenated
/// - data_hash: Keccak256 hash of event data
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventContentId {
    pub tx_hash: B256,
    pub address: Address,
    pub topics_hash: B256,
    pub data_hash: B256,
}

impl EventContentId {
    /// Create an EventContentId from a Log
    ///
    /// Returns None if the log doesn't have a transaction hash
    pub fn from_log(log: &Log) -> Option<Self> {
        let tx_hash = log.transaction_hash?;
        let address = log.address();

        // Hash topics together
        let mut topics_bytes = Vec::new();
        for topic in log.topics() {
            topics_bytes.extend_from_slice(topic.as_slice());
        }
        let topics_hash = keccak256(&topics_bytes);

        // Hash data
        let data_hash = keccak256(log.data().data.as_ref());

        Some(Self {
            tx_hash,
            address,
            topics_hash,
            data_hash,
        })
    }
}

/// Buffer storing WebSocket event content identifiers organized by block number
///
/// Optimized for:
/// - Fast insertion (O(1) with DashMap)
/// - Efficient range-based retrieval and clearing
/// - Content-based comparison (works with MiniBlocks that have different log_index)
pub struct WebSocketEventBuffer {
    /// Block number -> Set of EventContentIds in that block
    /// Using DashMap for concurrent access without global locks
    events_by_block: DashMap<u64, HashSet<EventContentId>>,

    /// Starting block for current verification cycle
    verification_start_block: AtomicU64,

    /// Statistics for monitoring
    total_events_buffered: AtomicU64,
    total_missing_found: AtomicU64,
}

impl WebSocketEventBuffer {
    pub fn new() -> Self {
        Self {
            events_by_block: DashMap::new(),
            verification_start_block: AtomicU64::new(0),
            total_events_buffered: AtomicU64::new(0),
            total_missing_found: AtomicU64::new(0),
        }
    }

    /// Set the starting block for verification tracking
    pub fn set_verification_start(&self, block: u64) {
        self.verification_start_block.store(block, Ordering::SeqCst);
    }

    /// Get the current verification start block
    pub fn get_verification_start(&self) -> u64 {
        self.verification_start_block.load(Ordering::SeqCst)
    }

    /// Record an event received via WebSocket
    /// Called after process_log() to track what WebSocket has received
    pub fn record_event(&self, block_number: u64, content_id: EventContentId) {
        self.events_by_block
            .entry(block_number)
            .or_insert_with(HashSet::new)
            .insert(content_id);
        self.total_events_buffered.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if an event exists in the buffer by content
    pub fn contains(&self, block_number: u64, content_id: &EventContentId) -> bool {
        self.events_by_block
            .get(&block_number)
            .map(|set| set.contains(content_id))
            .unwrap_or(false)
    }

    /// Get all event content IDs for a block range (inclusive)
    pub fn get_events_in_range(&self, from_block: u64, to_block: u64) -> HashSet<EventContentId> {
        let mut result = HashSet::new();
        for block in from_block..=to_block {
            if let Some(events) = self.events_by_block.get(&block) {
                result.extend(events.iter().cloned());
            }
        }
        result
    }

    /// Clear events for a block range (inclusive) and update start block
    /// Should only be called after successful verification of both RPCs
    pub fn clear_range(&self, from_block: u64, to_block: u64) {
        for block in from_block..=to_block {
            self.events_by_block.remove(&block);
        }
        // Update verification start to next block
        self.verification_start_block
            .store(to_block + 1, Ordering::SeqCst);
    }

    /// Get the number of blocks currently tracked
    pub fn blocks_tracked(&self) -> usize {
        self.events_by_block.len()
    }

    /// Get total events buffered (lifetime counter)
    pub fn total_events_buffered(&self) -> u64 {
        self.total_events_buffered.load(Ordering::Relaxed)
    }

    /// Increment missing events counter
    pub fn record_missing(&self, count: u64) {
        self.total_missing_found.fetch_add(count, Ordering::Relaxed);
    }

    /// Get total missing events found (lifetime counter)
    pub fn total_missing_found(&self) -> u64 {
        self.total_missing_found.load(Ordering::Relaxed)
    }

    /// Get buffer statistics
    pub fn stats(&self) -> BufferStats {
        BufferStats {
            blocks_tracked: self.events_by_block.len(),
            total_events_buffered: self.total_events_buffered.load(Ordering::Relaxed),
            total_missing_found: self.total_missing_found.load(Ordering::Relaxed),
            verification_start_block: self.verification_start_block.load(Ordering::Relaxed),
        }
    }
}

impl Default for WebSocketEventBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for WebSocketEventBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebSocketEventBuffer")
            .field("blocks_tracked", &self.events_by_block.len())
            .field(
                "verification_start_block",
                &self.verification_start_block.load(Ordering::Relaxed),
            )
            .field(
                "total_events_buffered",
                &self.total_events_buffered.load(Ordering::Relaxed),
            )
            .field(
                "total_missing_found",
                &self.total_missing_found.load(Ordering::Relaxed),
            )
            .finish()
    }
}

/// Statistics from the WebSocket event buffer
#[derive(Debug, Clone, Default)]
pub struct BufferStats {
    pub blocks_tracked: usize,
    pub total_events_buffered: u64,
    pub total_missing_found: u64,
    pub verification_start_block: u64,
}
