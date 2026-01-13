use alloy_primitives::{Address, FixedBytes, U128};
use dashmap::DashMap;
use indexer_core::types::{Order, OrderKey, OrderSide};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashSet};

/// Thread-safe store for orders with multiple indexes
#[derive(Debug)]
pub struct OrderStore {
    /// Primary storage: OrderKey -> Order
    orders: DashMap<OrderKey, Order>,

    /// Index: user -> set of OrderKeys
    user_orders: DashMap<Address, HashSet<OrderKey>>,

    /// Index: pool_id -> set of OrderKeys (only active orders)
    pool_active_orders: DashMap<FixedBytes<32>, HashSet<OrderKey>>,

    /// Index for orderbook queries: (pool_id, side) -> price -> OrderKeys
    orderbook_index: DashMap<(FixedBytes<32>, OrderSide), RwLock<BTreeMap<U128, HashSet<OrderKey>>>>,
}

/// Price level in orderbook
#[derive(Debug, Clone)]
pub struct PriceLevel {
    pub price: U128,
    pub quantity: U128,
    pub order_count: usize,
}

/// Orderbook snapshot with bids and asks
#[derive(Debug, Clone)]
pub struct OrderbookSnapshot {
    pub pool_id: FixedBytes<32>,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

impl OrderStore {
    pub fn new() -> Self {
        Self {
            orders: DashMap::new(),
            user_orders: DashMap::new(),
            pool_active_orders: DashMap::new(),
            orderbook_index: DashMap::new(),
        }
    }

    /// Insert a new order
    pub fn insert(&self, order: Order) {
        let key = OrderKey {
            pool_id: order.pool_id,
            order_id: order.order_id,
        };

        // Update user index
        self.user_orders
            .entry(order.user)
            .or_insert_with(HashSet::new)
            .insert(key);

        // Update active orders index if order is active
        if order.is_active() {
            self.pool_active_orders
                .entry(order.pool_id)
                .or_insert_with(HashSet::new)
                .insert(key);

            // Update orderbook price index
            let index_key = (order.pool_id, order.side);
            self.orderbook_index
                .entry(index_key)
                .or_insert_with(|| RwLock::new(BTreeMap::new()))
                .write()
                .entry(order.price)
                .or_insert_with(HashSet::new)
                .insert(key);
        }

        self.orders.insert(key, order);
    }

    /// Get order by key
    pub fn get(&self, key: &OrderKey) -> Option<Order> {
        self.orders.get(key).map(|o| o.clone())
    }

    /// Get order by pool_id and order_id
    pub fn get_by_ids(&self, pool_id: FixedBytes<32>, order_id: u64) -> Option<Order> {
        self.get(&OrderKey { pool_id, order_id })
    }

    /// Update an order
    pub fn update<F>(&self, key: &OrderKey, update_fn: F)
    where
        F: FnOnce(&mut Order),
    {
        if let Some(mut order_ref) = self.orders.get_mut(key) {
            let old_was_active = order_ref.is_active();

            update_fn(&mut order_ref);

            let is_now_active = order_ref.is_active();

            // Handle index updates if active status changed
            if old_was_active && !is_now_active {
                self.remove_from_active_indexes(key, &order_ref);
            } else if !old_was_active && is_now_active {
                self.add_to_active_indexes(key, &order_ref);
            }
        }
    }

    fn remove_from_active_indexes(&self, key: &OrderKey, order: &Order) {
        // Remove from pool active orders
        if let Some(mut active_orders) = self.pool_active_orders.get_mut(&order.pool_id) {
            active_orders.remove(key);
        }

        // Remove from orderbook index
        let index_key = (order.pool_id, order.side);
        if let Some(price_map) = self.orderbook_index.get(&index_key) {
            let mut price_map = price_map.write();
            if let Some(orders_at_price) = price_map.get_mut(&order.price) {
                orders_at_price.remove(key);
                if orders_at_price.is_empty() {
                    price_map.remove(&order.price);
                }
            }
        }
    }

    fn add_to_active_indexes(&self, key: &OrderKey, order: &Order) {
        // Add to pool active orders
        self.pool_active_orders
            .entry(order.pool_id)
            .or_insert_with(HashSet::new)
            .insert(*key);

        // Add to orderbook index
        let index_key = (order.pool_id, order.side);
        self.orderbook_index
            .entry(index_key)
            .or_insert_with(|| RwLock::new(BTreeMap::new()))
            .write()
            .entry(order.price)
            .or_insert_with(HashSet::new)
            .insert(*key);
    }

    /// Get all orders for a user
    pub fn get_user_orders(&self, user: &Address) -> Vec<Order> {
        self.user_orders
            .get(user)
            .map(|keys| {
                keys.iter()
                    .filter_map(|k| self.orders.get(k).map(|o| o.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get active orders for a user
    pub fn get_user_active_orders(&self, user: &Address) -> Vec<Order> {
        self.get_user_orders(user)
            .into_iter()
            .filter(|o| o.is_active())
            .collect()
    }

    /// Get orderbook depth snapshot
    pub fn get_orderbook(&self, pool_id: &FixedBytes<32>, depth: usize) -> OrderbookSnapshot {
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        // Get bids (buy orders) - sorted by price descending
        if let Some(bid_index) = self.orderbook_index.get(&(*pool_id, OrderSide::Buy)) {
            let price_map = bid_index.read();
            for (price, keys) in price_map.iter().rev().take(depth) {
                let total_quantity: U128 = keys
                    .iter()
                    .filter_map(|k| self.orders.get(k))
                    .map(|o| o.remaining_quantity)
                    .fold(U128::ZERO, |a, b| a.saturating_add(b));
                bids.push(PriceLevel {
                    price: *price,
                    quantity: total_quantity,
                    order_count: keys.len(),
                });
            }
        }

        // Get asks (sell orders) - sorted by price ascending
        if let Some(ask_index) = self.orderbook_index.get(&(*pool_id, OrderSide::Sell)) {
            let price_map = ask_index.read();
            for (price, keys) in price_map.iter().take(depth) {
                let total_quantity: U128 = keys
                    .iter()
                    .filter_map(|k| self.orders.get(k))
                    .map(|o| o.remaining_quantity)
                    .fold(U128::ZERO, |a, b| a.saturating_add(b));
                asks.push(PriceLevel {
                    price: *price,
                    quantity: total_quantity,
                    order_count: keys.len(),
                });
            }
        }

        OrderbookSnapshot {
            pool_id: *pool_id,
            bids,
            asks,
        }
    }

    /// Get total order count
    pub fn count(&self) -> usize {
        self.orders.len()
    }

    /// Get active order count for a pool
    pub fn active_count(&self, pool_id: &FixedBytes<32>) -> usize {
        self.pool_active_orders
            .get(pool_id)
            .map(|s| s.len())
            .unwrap_or(0)
    }
}

impl Default for OrderStore {
    fn default() -> Self {
        Self::new()
    }
}
