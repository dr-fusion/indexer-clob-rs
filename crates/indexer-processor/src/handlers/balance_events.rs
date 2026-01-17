use alloy::rpc::types::Log;
use alloy_sol_types::SolEvent;
use indexer_core::events::{Deposit, Lock, TransferFrom, TransferLockedFrom, Unlock, Withdrawal};
use indexer_core::types::{BalanceEvent, BalanceEventType};
use indexer_core::{IndexerError, Result};
use indexer_store::IndexerStore;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

pub struct BalanceEventHandler {
    store: Arc<IndexerStore>,
}

impl BalanceEventHandler {
    pub fn new(store: Arc<IndexerStore>) -> Self {
        Self { store }
    }

    pub async fn handle_deposit(&self, log: &Log) -> Result<()> {
        let start = Instant::now();

        let decode_start = Instant::now();
        let event = Deposit::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;
        let decode_duration_us = decode_start.elapsed().as_micros();

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
            block = log.block_number.unwrap_or_default(),
            "Deposit"
        );

        let store_start = Instant::now();
        self.store.balances.apply_event(&balance_event);
        let store_duration_us = store_start.elapsed().as_micros();

        let stats_start = Instant::now();
        self.record_event().await;
        let stats_duration_us = stats_start.elapsed().as_micros();

        let total_duration_us = start.elapsed().as_micros();
        debug!(
            decode_us = decode_duration_us,
            store_us = store_duration_us,
            stats_us = stats_duration_us,
            total_us = total_duration_us,
            "Deposit handler timing"
        );

        Ok(())
    }

    pub async fn handle_withdrawal(&self, log: &Log) -> Result<()> {
        let start = Instant::now();

        let decode_start = Instant::now();
        let event = Withdrawal::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;
        let decode_duration_us = decode_start.elapsed().as_micros();

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
            block = log.block_number.unwrap_or_default(),
            "Withdrawal"
        );

        let store_start = Instant::now();
        self.store.balances.apply_event(&balance_event);
        let store_duration_us = store_start.elapsed().as_micros();

        let stats_start = Instant::now();
        self.record_event().await;
        let stats_duration_us = stats_start.elapsed().as_micros();

        let total_duration_us = start.elapsed().as_micros();
        debug!(
            decode_us = decode_duration_us,
            store_us = store_duration_us,
            stats_us = stats_duration_us,
            total_us = total_duration_us,
            "Withdrawal handler timing"
        );

        Ok(())
    }

    pub async fn handle_lock(&self, log: &Log) -> Result<()> {
        let start = Instant::now();

        let decode_start = Instant::now();
        let event = Lock::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;
        let decode_duration_us = decode_start.elapsed().as_micros();

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
            block = log.block_number.unwrap_or_default(),
            "Lock"
        );

        let store_start = Instant::now();
        self.store.balances.apply_event(&balance_event);
        let store_duration_us = store_start.elapsed().as_micros();

        let stats_start = Instant::now();
        self.record_event().await;
        let stats_duration_us = stats_start.elapsed().as_micros();

        let total_duration_us = start.elapsed().as_micros();
        debug!(
            decode_us = decode_duration_us,
            store_us = store_duration_us,
            stats_us = stats_duration_us,
            total_us = total_duration_us,
            "Lock handler timing"
        );

        Ok(())
    }

    pub async fn handle_unlock(&self, log: &Log) -> Result<()> {
        let start = Instant::now();

        let decode_start = Instant::now();
        let event = Unlock::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;
        let decode_duration_us = decode_start.elapsed().as_micros();

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
            block = log.block_number.unwrap_or_default(),
            "Unlock"
        );

        let store_start = Instant::now();
        self.store.balances.apply_event(&balance_event);
        let store_duration_us = store_start.elapsed().as_micros();

        let stats_start = Instant::now();
        self.record_event().await;
        let stats_duration_us = stats_start.elapsed().as_micros();

        let total_duration_us = start.elapsed().as_micros();
        debug!(
            decode_us = decode_duration_us,
            store_us = store_duration_us,
            stats_us = stats_duration_us,
            total_us = total_duration_us,
            "Unlock handler timing"
        );

        Ok(())
    }

    pub async fn handle_transfer_from(&self, log: &Log) -> Result<()> {
        let start = Instant::now();

        let decode_start = Instant::now();
        let event = TransferFrom::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;
        let decode_duration_us = decode_start.elapsed().as_micros();

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
            block = block_number,
            "TransferFrom"
        );

        let store_start = Instant::now();
        self.store.balances.apply_event(&sender_event);
        self.store.balances.apply_event(&receiver_event);
        let store_duration_us = store_start.elapsed().as_micros();

        let stats_start = Instant::now();
        self.record_event().await;
        let stats_duration_us = stats_start.elapsed().as_micros();

        let total_duration_us = start.elapsed().as_micros();
        debug!(
            decode_us = decode_duration_us,
            store_us = store_duration_us,
            stats_us = stats_duration_us,
            total_us = total_duration_us,
            "TransferFrom handler timing"
        );

        Ok(())
    }

    pub async fn handle_transfer_locked_from(&self, log: &Log) -> Result<()> {
        let start = Instant::now();

        let decode_start = Instant::now();
        let event = TransferLockedFrom::decode_log(&log.inner)
            .map_err(|e| IndexerError::EventDecode(e.to_string()))?;
        let decode_duration_us = decode_start.elapsed().as_micros();

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
            block = block_number,
            "TransferLockedFrom"
        );

        let store_start = Instant::now();
        self.store.balances.apply_event(&sender_event);
        self.store.balances.apply_event(&sender_transfer_event);
        self.store.balances.apply_event(&receiver_event);
        let store_duration_us = store_start.elapsed().as_micros();

        let stats_start = Instant::now();
        self.record_event().await;
        let stats_duration_us = stats_start.elapsed().as_micros();

        let total_duration_us = start.elapsed().as_micros();
        debug!(
            decode_us = decode_duration_us,
            store_us = store_duration_us,
            stats_us = stats_duration_us,
            total_us = total_duration_us,
            "TransferLockedFrom handler timing"
        );

        Ok(())
    }

    async fn record_event(&self) {
        let mut state = self.store.sync_state.write().await;
        state.record_event();
    }
}
