use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

/// Configuration for adaptive batch sizing
#[derive(Debug, Clone)]
pub struct AdaptiveBatchConfig {
    /// Initial batch size (default: 10000)
    pub initial_size: u64,
    /// Minimum batch size (floor, default: 100)
    pub min_size: u64,
    /// Maximum batch size (ceiling, default: 10000)
    pub max_size: u64,
    /// Factor to multiply by on error (default: 0.5 = halve)
    pub decrease_factor: f64,
    /// Factor to increase by on success (default: 0.1 = 10% increase)
    pub increase_factor: f64,
    /// Number of consecutive successes before increasing (default: 10)
    pub success_threshold: u32,
}

impl AdaptiveBatchConfig {
    pub fn from_env() -> Self {
        let initial_size = std::env::var("BATCH_SIZE_INITIAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10000);

        let min_size = std::env::var("BATCH_SIZE_MIN")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        let max_size = std::env::var("BATCH_SIZE_MAX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10000);

        let decrease_factor = std::env::var("BATCH_SIZE_DECREASE_FACTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.5);

        let increase_factor = std::env::var("BATCH_SIZE_INCREASE_FACTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.1);

        let success_threshold = std::env::var("BATCH_SIZE_SUCCESS_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        Self {
            initial_size,
            min_size,
            max_size,
            decrease_factor,
            increase_factor,
            success_threshold,
        }
    }
}

impl Default for AdaptiveBatchConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

/// AIMD (Additive Increase, Multiplicative Decrease) batch controller
/// Same algorithm as TCP congestion control
pub struct AdaptiveBatchController {
    current_size: AtomicU64,
    success_count: AtomicU32,
    config: AdaptiveBatchConfig,
}

impl AdaptiveBatchController {
    pub fn new(config: AdaptiveBatchConfig) -> Self {
        let initial = config.initial_size.clamp(config.min_size, config.max_size);
        info!(
            initial_size = initial,
            min = config.min_size,
            max = config.max_size,
            decrease_factor = config.decrease_factor,
            increase_factor = config.increase_factor,
            success_threshold = config.success_threshold,
            "AdaptiveBatchController initialized"
        );
        Self {
            current_size: AtomicU64::new(initial),
            success_count: AtomicU32::new(0),
            config,
        }
    }

    /// Get the current batch size
    pub fn get_size(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }

    /// Report a successful batch operation
    /// After success_threshold consecutive successes, increases batch size by factor (e.g., 10%)
    pub fn report_success(&self) {
        let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;

        if count >= self.config.success_threshold {
            self.success_count.store(0, Ordering::Relaxed);
            let current = self.current_size.load(Ordering::Relaxed);
            // Increase by factor (e.g., 0.1 = 10% increase means multiply by 1.1)
            let increase = (current as f64 * self.config.increase_factor).max(1.0) as u64;
            let new_size = (current + increase).min(self.config.max_size);

            if new_size > current {
                self.current_size.store(new_size, Ordering::Relaxed);
                debug!(
                    from = current,
                    to = new_size,
                    increase_pct = self.config.increase_factor * 100.0,
                    "Batch size increased"
                );
            }
        }
    }

    /// Report a failed batch operation
    /// Immediately reduces batch size by multiplicative factor
    pub fn report_error(&self) {
        self.success_count.store(0, Ordering::Relaxed);
        let current = self.current_size.load(Ordering::Relaxed);
        let new_size = ((current as f64 * self.config.decrease_factor) as u64)
            .max(self.config.min_size);

        self.current_size.store(new_size, Ordering::Relaxed);
        warn!(
            from = current,
            to = new_size,
            "Batch size decreased (multiplicative)"
        );
    }

    /// Report a partial success (e.g., some requests failed but batch completed)
    /// Does not count toward success threshold, but doesn't decrease either
    pub fn report_partial_success(&self) {
        self.success_count.store(0, Ordering::Relaxed);
        debug!("Partial success - success counter reset");
    }
}

// =============================================================================
// Adaptive Concurrency Controller (AIMD)
// =============================================================================

/// Configuration for adaptive concurrency control
#[derive(Debug, Clone)]
pub struct AdaptiveConcurrencyConfig {
    /// Initial concurrency level (default: 10)
    pub initial: usize,
    /// Minimum concurrency (floor, default: 1)
    pub min: usize,
    /// Maximum concurrency (ceiling, default: 20)
    pub max: usize,
    /// Factor to multiply by on error (default: 0.5 = halve)
    pub decrease_factor: f64,
    /// Factor to increase by on success (default: 0.1 = 10% increase)
    pub increase_factor: f64,
    /// Number of consecutive successes before increasing (default: 20)
    pub success_threshold: u32,
}

impl AdaptiveConcurrencyConfig {
    pub fn from_env() -> Self {
        let initial = std::env::var("CONCURRENCY_INITIAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        let min = std::env::var("CONCURRENCY_MIN")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        let max = std::env::var("CONCURRENCY_MAX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20);

        let decrease_factor = std::env::var("CONCURRENCY_DECREASE_FACTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.5);

        let increase_factor = std::env::var("CONCURRENCY_INCREASE_FACTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.1);

        let success_threshold = std::env::var("CONCURRENCY_SUCCESS_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(20);

        Self {
            initial,
            min,
            max,
            decrease_factor,
            increase_factor,
            success_threshold,
        }
    }
}

impl Default for AdaptiveConcurrencyConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

/// AIMD (Additive Increase, Multiplicative Decrease) concurrency controller
/// Dynamically adjusts the number of concurrent requests based on RPC responses
pub struct AdaptiveConcurrencyController {
    current: AtomicUsize,
    success_count: AtomicU32,
    config: AdaptiveConcurrencyConfig,
    /// Semaphore for limiting concurrent operations
    semaphore: Arc<Semaphore>,
}

impl AdaptiveConcurrencyController {
    pub fn new(config: AdaptiveConcurrencyConfig) -> Self {
        let initial = config.initial.clamp(config.min, config.max);
        info!(
            initial = initial,
            min = config.min,
            max = config.max,
            decrease_factor = config.decrease_factor,
            increase_factor = config.increase_factor,
            success_threshold = config.success_threshold,
            "AdaptiveConcurrencyController initialized"
        );
        Self {
            current: AtomicUsize::new(initial),
            success_count: AtomicU32::new(0),
            config,
            // Start with max permits - we'll control via current value
            semaphore: Arc::new(Semaphore::new(initial)),
        }
    }

    /// Get the current concurrency level
    pub fn get(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// Report a successful operation
    /// After success_threshold consecutive successes, increases concurrency by factor (e.g., 10%)
    pub fn report_success(&self) {
        let count = self.success_count.fetch_add(1, Ordering::Relaxed) + 1;

        if count >= self.config.success_threshold {
            self.success_count.store(0, Ordering::Relaxed);
            let current = self.current.load(Ordering::Relaxed);
            // Increase by factor (e.g., 0.1 = 10% increase), minimum 1
            let increase = (current as f64 * self.config.increase_factor).max(1.0) as usize;
            let new_value = (current + increase).min(self.config.max);

            if new_value > current {
                self.current.store(new_value, Ordering::Relaxed);
                // Add permits to semaphore
                self.semaphore.add_permits(increase);
                debug!(
                    from = current,
                    to = new_value,
                    increase_pct = self.config.increase_factor * 100.0,
                    "Concurrency increased"
                );
            }
        }
    }

    /// Report a failed operation (e.g., rate limit, timeout)
    /// Immediately reduces concurrency by multiplicative factor
    pub fn report_error(&self) {
        self.success_count.store(0, Ordering::Relaxed);
        let current = self.current.load(Ordering::Relaxed);
        let new_value = ((current as f64 * self.config.decrease_factor) as usize)
            .max(self.config.min);

        if new_value < current {
            self.current.store(new_value, Ordering::Relaxed);
            // Note: We can't easily remove permits from a tokio Semaphore
            // The reduction will take effect as permits are released
            warn!(
                from = current,
                to = new_value,
                "Concurrency decreased (multiplicative)"
            );
        }
    }

}

/// Tracks processed block ranges to ensure no gaps
pub struct BlockRangeTracker {
    /// Processed ranges (sorted by start block)
    processed: tokio::sync::RwLock<Vec<(u64, u64)>>,
    /// Target end block
    target_end: u64,
}

impl BlockRangeTracker {
    pub fn new(target_end: u64) -> Self {
        Self {
            processed: tokio::sync::RwLock::new(Vec::new()),
            target_end,
        }
    }

    /// Mark a block range as successfully processed
    pub async fn mark_processed(&self, from: u64, to: u64) {
        let mut ranges = self.processed.write().await;
        ranges.push((from, to));
        // Keep sorted by start block
        ranges.sort_by_key(|r| r.0);

        // Merge adjacent/overlapping ranges
        let mut merged: Vec<(u64, u64)> = Vec::new();
        for (from, to) in ranges.drain(..) {
            if let Some(last) = merged.last_mut() {
                // Check if ranges overlap or are adjacent
                if from <= last.1 + 1 {
                    last.1 = last.1.max(to);
                } else {
                    merged.push((from, to));
                }
            } else {
                merged.push((from, to));
            }
        }
        *ranges = merged;
    }

    /// Get any gaps in processed ranges
    /// Returns list of (from, to) ranges that haven't been processed
    pub async fn get_gaps(&self, start_block: u64) -> Vec<(u64, u64)> {
        let ranges = self.processed.read().await;
        let mut gaps = Vec::new();
        let mut expected_start = start_block;

        for (from, to) in ranges.iter() {
            if *from > expected_start {
                gaps.push((expected_start, from - 1));
            }
            expected_start = to + 1;
        }

        // Check if there's a gap at the end
        if expected_start <= self.target_end {
            gaps.push((expected_start, self.target_end));
        }

        gaps
    }

    /// Check if all blocks have been processed
    pub async fn is_complete(&self, start_block: u64) -> bool {
        let ranges = self.processed.read().await;
        if ranges.is_empty() {
            return false;
        }
        // Should have exactly one merged range covering start_block to target_end
        ranges.len() == 1 && ranges[0].0 <= start_block && ranges[0].1 >= self.target_end
    }

    /// Get the highest block that is contiguously synced from start_block
    /// Returns the end of the first contiguous range, or start_block - 1 if no ranges
    pub async fn get_contiguous_end(&self, start_block: u64) -> u64 {
        let ranges = self.processed.read().await;
        if ranges.is_empty() {
            return start_block.saturating_sub(1);
        }
        // Check if first range starts at or before start_block
        if ranges[0].0 > start_block {
            // There's a gap at the beginning
            return start_block.saturating_sub(1);
        }
        // Return end of first contiguous range
        ranges[0].1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptive_batch_controller_aimd() {
        let config = AdaptiveBatchConfig {
            initial_size: 1000,
            min_size: 100,
            max_size: 10000,
            decrease_factor: 0.5,
            increase_factor: 0.1, // 10% increase
            success_threshold: 3,
        };
        let controller = AdaptiveBatchController::new(config);

        // Initial size
        assert_eq!(controller.get_size(), 1000);

        // Success 3 times should increase by 10% (1000 * 0.1 = 100)
        controller.report_success();
        controller.report_success();
        controller.report_success();
        assert_eq!(controller.get_size(), 1100);

        // Error should halve
        controller.report_error();
        assert_eq!(controller.get_size(), 550);

        // Can't go below min
        controller.report_error();
        controller.report_error();
        controller.report_error();
        assert_eq!(controller.get_size(), 100); // min
    }

    #[tokio::test]
    async fn test_block_range_tracker() {
        let tracker = BlockRangeTracker::new(1000);

        // Process some ranges
        tracker.mark_processed(0, 100).await;
        tracker.mark_processed(200, 300).await;
        tracker.mark_processed(500, 600).await;

        // Check gaps
        let gaps = tracker.get_gaps(0).await;
        assert_eq!(gaps, vec![(101, 199), (301, 499), (601, 1000)]);

        // Fill a gap
        tracker.mark_processed(101, 199).await;
        let gaps = tracker.get_gaps(0).await;
        assert_eq!(gaps, vec![(301, 499), (601, 1000)]);
    }

    #[tokio::test]
    async fn test_range_merging() {
        let tracker = BlockRangeTracker::new(100);

        // Overlapping ranges should merge
        tracker.mark_processed(0, 50).await;
        tracker.mark_processed(40, 80).await;
        tracker.mark_processed(81, 100).await;

        assert!(tracker.is_complete(0).await);
    }

    #[test]
    fn test_adaptive_concurrency_controller_aimd() {
        let config = AdaptiveConcurrencyConfig {
            initial: 10,
            min: 1,
            max: 20,
            decrease_factor: 0.5,
            increase_factor: 0.1, // 10% increase
            success_threshold: 3,
        };
        let controller = AdaptiveConcurrencyController::new(config);

        // Initial concurrency
        assert_eq!(controller.get(), 10);

        // Success 3 times should increase by 10% (10 * 0.1 = 1, min 1)
        controller.report_success();
        controller.report_success();
        controller.report_success();
        assert_eq!(controller.get(), 11);

        // Error should halve
        controller.report_error();
        assert_eq!(controller.get(), 5);

        // Can't go below min
        controller.report_error();
        controller.report_error();
        controller.report_error();
        assert_eq!(controller.get(), 1); // min
    }
}
