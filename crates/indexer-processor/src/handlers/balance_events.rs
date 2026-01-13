use alloy::rpc::types::Log;
use alloy_sol_types::SolEvent;
use indexer_core::events::{Deposit, Lock, TransferFrom, TransferLockedFrom, Unlock, Withdrawal};
use indexer_core::types::{BalanceEvent, BalanceEventType};
use indexer_core::{IndexerError, Result};
use indexer_store::IndexerStore;
use std::sync::Arc;
use tracing::debug;

pub struct BalanceEventHandler {
    store: Arc<IndexerStore>,
}

impl BalanceEventHandler {
    pub fn new(store: Arc<IndexerStore>) -> Self {
        Self { store }
    }

    pub async fn handle_deposit(&self, log: &Log) -> Result<()> {
        let event = Deposit::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;

        let balance_event = BalanceEvent {
            event_type: BalanceEventType::Deposit,
            user: event.user,
            token_id: event.id,
            amount: event.amount,
            block_number: log.block_number.unwrap_or_default(),
            tx_hash: log.transaction_hash.unwrap_or_default(),
            log_index: log.log_index.unwrap_or_default() as u64,
        };

        debug!(
            user = ?event.user,
            token = ?event.id,
            amount = ?event.amount,
            "Deposit"
        );

        self.store.balances.apply_event(&balance_event);
        self.record_event().await;

        Ok(())
    }

    pub async fn handle_withdrawal(&self, log: &Log) -> Result<()> {
        let event = Withdrawal::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;

        let balance_event = BalanceEvent {
            event_type: BalanceEventType::Withdrawal,
            user: event.user,
            token_id: event.id,
            amount: event.amount,
            block_number: log.block_number.unwrap_or_default(),
            tx_hash: log.transaction_hash.unwrap_or_default(),
            log_index: log.log_index.unwrap_or_default() as u64,
        };

        debug!(
            user = ?event.user,
            token = ?event.id,
            amount = ?event.amount,
            "Withdrawal"
        );

        self.store.balances.apply_event(&balance_event);
        self.record_event().await;

        Ok(())
    }

    pub async fn handle_lock(&self, log: &Log) -> Result<()> {
        let event = Lock::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;

        let balance_event = BalanceEvent {
            event_type: BalanceEventType::Lock,
            user: event.user,
            token_id: event.id,
            amount: event.amount,
            block_number: log.block_number.unwrap_or_default(),
            tx_hash: log.transaction_hash.unwrap_or_default(),
            log_index: log.log_index.unwrap_or_default() as u64,
        };

        debug!(
            user = ?event.user,
            token = ?event.id,
            amount = ?event.amount,
            "Lock"
        );

        self.store.balances.apply_event(&balance_event);
        self.record_event().await;

        Ok(())
    }

    pub async fn handle_unlock(&self, log: &Log) -> Result<()> {
        let event = Unlock::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;

        let balance_event = BalanceEvent {
            event_type: BalanceEventType::Unlock,
            user: event.user,
            token_id: event.id,
            amount: event.amount,
            block_number: log.block_number.unwrap_or_default(),
            tx_hash: log.transaction_hash.unwrap_or_default(),
            log_index: log.log_index.unwrap_or_default() as u64,
        };

        debug!(
            user = ?event.user,
            token = ?event.id,
            amount = ?event.amount,
            "Unlock"
        );

        self.store.balances.apply_event(&balance_event);
        self.record_event().await;

        Ok(())
    }

    pub async fn handle_transfer_from(&self, log: &Log) -> Result<()> {
        let event = TransferFrom::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;

        let block_number = log.block_number.unwrap_or_default();
        let tx_hash = log.transaction_hash.unwrap_or_default();
        let log_index = log.log_index.unwrap_or_default() as u64;

        // Sender loses funds
        let sender_event = BalanceEvent {
            event_type: BalanceEventType::TransferOut,
            user: event.sender,
            token_id: event.id,
            amount: event.amount,
            block_number,
            tx_hash,
            log_index,
        };

        // Receiver gains funds (minus fee)
        let amount_after_fee = event.amount.saturating_sub(event.feeAmount);
        let receiver_event = BalanceEvent {
            event_type: BalanceEventType::TransferIn,
            user: event.receiver,
            token_id: event.id,
            amount: amount_after_fee,
            block_number,
            tx_hash,
            log_index,
        };

        debug!(
            sender = ?event.sender,
            receiver = ?event.receiver,
            amount = ?event.amount,
            fee = ?event.feeAmount,
            "TransferFrom"
        );

        self.store.balances.apply_event(&sender_event);
        self.store.balances.apply_event(&receiver_event);
        self.record_event().await;

        Ok(())
    }

    pub async fn handle_transfer_locked_from(&self, log: &Log) -> Result<()> {
        let event = TransferLockedFrom::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;

        let block_number = log.block_number.unwrap_or_default();
        let tx_hash = log.transaction_hash.unwrap_or_default();
        let log_index = log.log_index.unwrap_or_default() as u64;

        // For locked transfers, the sender's locked balance is reduced
        // and receiver gets available balance
        let sender_event = BalanceEvent {
            event_type: BalanceEventType::Unlock, // Unlock then transfer out
            user: event.sender,
            token_id: event.id,
            amount: event.amount,
            block_number,
            tx_hash,
            log_index,
        };

        let sender_transfer_event = BalanceEvent {
            event_type: BalanceEventType::TransferOut,
            user: event.sender,
            token_id: event.id,
            amount: event.amount,
            block_number,
            tx_hash,
            log_index,
        };

        // Receiver gains funds (minus fee)
        let amount_after_fee = event.amount.saturating_sub(event.feeAmount);
        let receiver_event = BalanceEvent {
            event_type: BalanceEventType::TransferIn,
            user: event.receiver,
            token_id: event.id,
            amount: amount_after_fee,
            block_number,
            tx_hash,
            log_index,
        };

        debug!(
            sender = ?event.sender,
            receiver = ?event.receiver,
            amount = ?event.amount,
            fee = ?event.feeAmount,
            "TransferLockedFrom"
        );

        self.store.balances.apply_event(&sender_event);
        self.store.balances.apply_event(&sender_transfer_event);
        self.store.balances.apply_event(&receiver_event);
        self.record_event().await;

        Ok(())
    }

    async fn record_event(&self) {
        let mut state = self.store.sync_state.write().await;
        state.record_event();
    }
}
