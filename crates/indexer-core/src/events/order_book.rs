use alloy_sol_types::sol;

sol! {
    /// Emitted when a new order is placed
    /// Note: Side and Status are uint8 in the ABI signature
    #[derive(Debug)]
    event OrderPlaced(
        uint48 indexed orderId,
        address indexed user,
        uint8 indexed side,
        uint128 price,
        uint128 quantity,
        uint48 expiry,
        bool isMarketOrder,
        uint8 status
    );

    /// Emitted when two orders match (trade execution)
    #[derive(Debug)]
    event OrderMatched(
        address indexed user,
        uint48 indexed buyOrderId,
        uint48 indexed sellOrderId,
        uint8 side,
        uint48 timestamp,
        uint128 executionPrice,
        uint128 takerLimitPrice,
        uint128 executedQuantity
    );

    /// Emitted when order status or filled amount changes
    #[derive(Debug)]
    event UpdateOrder(
        uint48 indexed orderId,
        uint48 timestamp,
        uint128 filled,
        uint8 status
    );

    /// Emitted when an order is cancelled
    #[derive(Debug)]
    event OrderCancelled(
        uint48 indexed orderId,
        address indexed user,
        uint48 timestamp,
        uint8 status
    );
}
