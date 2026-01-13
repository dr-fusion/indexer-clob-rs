use alloy_primitives::{Address, FixedBytes, B256, U128};

/// Order side (buy or sell)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum OrderSide {
    Buy = 0,
    Sell = 1,
}

/// Order status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OrderStatus {
    None = 0,
    Open = 1,
    PartiallyFilled = 2,
    Filled = 3,
    Cancelled = 4,
    Expired = 5,
}

impl From<u8> for OrderStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::None,
            1 => Self::Open,
            2 => Self::PartiallyFilled,
            3 => Self::Filled,
            4 => Self::Cancelled,
            5 => Self::Expired,
            _ => Self::None,
        }
    }
}

impl From<u8> for OrderSide {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Buy,
            1 => Self::Sell,
            _ => Self::Buy,
        }
    }
}

/// Composite key for order lookup
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OrderKey {
    pub pool_id: FixedBytes<32>,
    pub order_id: u64,
}

/// Represents an order in the order book
#[derive(Debug, Clone)]
pub struct Order {
    /// Order ID (uint48 fits in u64)
    pub order_id: u64,
    /// Pool this order belongs to
    pub pool_id: FixedBytes<32>,
    /// Order owner address
    pub user: Address,
    /// Buy or Sell
    pub side: OrderSide,
    /// Limit price
    pub price: U128,
    /// Original quantity when order was placed
    pub original_quantity: U128,
    /// Amount already filled
    pub filled_quantity: U128,
    /// Remaining quantity (original - filled)
    pub remaining_quantity: U128,
    /// Order expiration timestamp
    pub expiry: u64,
    /// Whether this is a market order
    pub is_market_order: bool,
    /// Current order status
    pub status: OrderStatus,
    /// Block when order was created
    pub created_at_block: u64,
    /// Transaction hash of order creation
    pub created_at_tx: B256,
    /// Block when order was last updated
    pub last_updated_block: u64,
}

impl Order {
    /// Check if order is still active (can be matched)
    pub fn is_active(&self) -> bool {
        matches!(self.status, OrderStatus::Open | OrderStatus::PartiallyFilled)
    }
}
