use alloy_primitives::{Address, U256};
use dashmap::DashMap;
use indexer_core::types::{BalanceEvent, BalanceEventType, BalanceKey, UserBalance};
use std::collections::HashSet;
use std::time::Instant;
use tracing::debug;

/// Thread-safe store for user balances
#[derive(Debug)]
pub struct BalanceStore {
    /// (user, token_id) -> UserBalance
    balances: DashMap<BalanceKey, UserBalance>,

    /// user -> set of token_ids (for listing user's tokens)
    user_tokens: DashMap<Address, HashSet<U256>>,
}

impl BalanceStore {
    pub fn new() -> Self {
        Self {
            balances: DashMap::new(),
            user_tokens: DashMap::new(),
        }
    }

    /// Apply a balance event
    pub fn apply_event(&self, event: &BalanceEvent) {
        let start = Instant::now();
        let event_type = event.event_type;
        let user = event.user;
        let token_id = event.token_id;
        let amount = event.amount;

        let key = BalanceKey {
            user: event.user,
            token_id: event.token_id,
        };

        // Ensure user-token index exists
        self.user_tokens
            .entry(event.user)
            .or_insert_with(HashSet::new)
            .insert(event.token_id);

        // Get or create balance
        let mut balance = self
            .balances
            .entry(key)
            .or_insert_with(|| UserBalance::new(event.user, event.token_id));

        let prev_available = balance.available;
        let prev_locked = balance.locked;

        match event.event_type {
            BalanceEventType::Deposit => {
                balance.available = balance.available.saturating_add(event.amount);
            }
            BalanceEventType::Withdrawal => {
                balance.available = balance.available.saturating_sub(event.amount);
            }
            BalanceEventType::Lock => {
                balance.available = balance.available.saturating_sub(event.amount);
                balance.locked = balance.locked.saturating_add(event.amount);
            }
            BalanceEventType::Unlock => {
                balance.locked = balance.locked.saturating_sub(event.amount);
                balance.available = balance.available.saturating_add(event.amount);
            }
            BalanceEventType::TransferOut => {
                balance.available = balance.available.saturating_sub(event.amount);
            }
            BalanceEventType::TransferIn => {
                balance.available = balance.available.saturating_add(event.amount);
            }
        }

        balance.update_total();

        let duration_us = start.elapsed().as_micros();
        debug!(
            event_type = ?event_type,
            user = ?user,
            token_id = ?token_id,
            amount = ?amount,
            prev_available = ?prev_available,
            new_available = ?balance.available,
            prev_locked = ?prev_locked,
            new_locked = ?balance.locked,
            total_balances = self.balances.len(),
            apply_us = duration_us,
            "Balance event applied in memory store"
        );
    }

    /// Get balance for user and token
    pub fn get(&self, user: &Address, token_id: &U256) -> Option<UserBalance> {
        let key = BalanceKey {
            user: *user,
            token_id: *token_id,
        };
        self.balances.get(&key).map(|b| b.clone())
    }

    /// Get all balances for a user
    pub fn get_user_balances(&self, user: &Address) -> Vec<UserBalance> {
        self.user_tokens
            .get(user)
            .map(|tokens| {
                tokens
                    .iter()
                    .filter_map(|token_id| self.get(user, token_id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get total number of balance entries
    pub fn count(&self) -> usize {
        self.balances.len()
    }
}

impl Default for BalanceStore {
    fn default() -> Self {
        Self::new()
    }
}
