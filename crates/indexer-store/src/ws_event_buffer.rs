//! WebSocket event buffer for RPC verification
//!
//! Stores event identifiers (tx_hash, log_index) received via WebSocket,
//! organized by block number, for comparison with RPC-fetched events.

use alloy_primitives::B256;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

/// Lightweight event identifier for WebSocket verification
/// Only stores tx_hash and log_index to minimize memory usage
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

/// Buffer storing WebSocket event identifiers organized by block number
///
/// Optimized for:
/// - Fast insertion (O(1) with DashMap)
/// - Efficient range-based retrieval and clearing
/// - Memory efficiency (only stores identifiers, not full events)
pub struct WebSocketEventBuffer {
    /// Block number -> Set of EventIds in that block
    /// Using DashMap for concurrent access without global locks
    events_by_block: DashMap<u64, HashSet<EventId>>,

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
    pub fn record_event(&self, block_number: u64, event_id: EventId) {
        self.events_by_block
            .entry(block_number)
            .or_insert_with(HashSet::new)
            .insert(event_id);
        self.total_events_buffered.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if an event exists in the buffer
    pub fn contains(&self, block_number: u64, event_id: &EventId) -> bool {
        self.events_by_block
            .get(&block_number)
            .map(|set| set.contains(event_id))
            .unwrap_or(false)
    }

    /// Get all event IDs for a block range (inclusive)
    pub fn get_events_in_range(&self, from_block: u64, to_block: u64) -> HashSet<EventId> {
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
