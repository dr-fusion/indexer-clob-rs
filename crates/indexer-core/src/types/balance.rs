use alloy_primitives::{Address, B256, U256};

/// Key for balance lookup: (user, token_id)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BalanceKey {
    pub user: Address,
    pub token_id: U256,
}

/// User balance for a specific token
#[derive(Debug, Clone, Default)]
pub struct UserBalance {
    /// User address
    pub user: Address,
    /// Token ID (currency address as U256)
    pub token_id: U256,
    /// Available (unlocked) balance
    pub available: U256,
    /// Locked balance (reserved for orders)
    pub locked: U256,
    /// Total balance (available + locked)
    pub total: U256,
}

impl UserBalance {
    pub fn new(user: Address, token_id: U256) -> Self {
        Self {
            user,
            token_id,
            available: U256::ZERO,
            locked: U256::ZERO,
            total: U256::ZERO,
        }
    }

    /// Recalculate total from available + locked
    pub fn update_total(&mut self) {
        self.total = self.available.saturating_add(self.locked);
    }
}

/// Type of balance event
#[derive(Debug, Clone, Copy)]
pub enum BalanceEventType {
    Deposit,
    Withdrawal,
    Lock,
    Unlock,
    TransferOut,
    TransferIn,
}

/// Balance change event
#[derive(Debug, Clone)]
pub struct BalanceEvent {
    pub event_type: BalanceEventType,
    pub user: Address,
    pub token_id: U256,
    pub amount: U256,
    pub block_number: u64,
    pub tx_hash: B256,
    pub log_index: u64,
}
