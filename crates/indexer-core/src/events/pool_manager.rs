use alloy_sol_types::sol;

sol! {
    /// Emitted when a new trading pool is created
    #[derive(Debug)]
    event PoolCreated(
        bytes32 indexed poolId,
        address orderBook,
        address baseCurrency,
        address quoteCurrency
    );

    /// Emitted when a new currency is registered
    #[derive(Debug)]
    event CurrencyAdded(address currency);

    /// Emitted when pool liquidity is updated
    #[derive(Debug)]
    event PoolLiquidityUpdated(bytes32 poolId, uint256 newLiquidity);
}
