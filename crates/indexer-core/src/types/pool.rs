use alloy_primitives::{Address, FixedBytes, B256};

/// Represents a trading pool
#[derive(Debug, Clone)]
pub struct Pool {
    /// Unique pool identifier (keccak256 hash)
    pub pool_id: FixedBytes<32>,
    /// Address of the OrderBook contract for this pool
    pub order_book_address: Address,
    /// Base currency token address
    pub base_currency: Address,
    /// Quote currency token address
    pub quote_currency: Address,
    /// Block number when pool was created
    pub created_at_block: u64,
    /// Transaction hash of pool creation
    pub created_at_tx: B256,
}
