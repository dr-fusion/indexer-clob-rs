use alloy_sol_types::sol;

sol! {
    /// Emitted when a user deposits funds
    #[derive(Debug)]
    event Deposit(
        address indexed user,
        uint256 indexed id,
        uint256 amount
    );

    /// Emitted when a user withdraws funds
    #[derive(Debug)]
    event Withdrawal(
        address indexed user,
        uint256 indexed id,
        uint256 amount
    );

    /// Emitted when funds are locked for an order
    #[derive(Debug)]
    event Lock(
        address indexed user,
        uint256 indexed id,
        uint256 amount
    );

    /// Emitted when locked funds are unlocked
    #[derive(Debug)]
    event Unlock(
        address indexed user,
        uint256 indexed id,
        uint256 amount
    );

    /// Emitted when funds are transferred between users (maker receives)
    #[derive(Debug)]
    event TransferFrom(
        address indexed operator,
        address indexed sender,
        address indexed receiver,
        uint256 id,
        uint256 amount,
        uint256 feeAmount
    );

    /// Emitted when locked funds are transferred (taker receives)
    #[derive(Debug)]
    event TransferLockedFrom(
        address indexed operator,
        address indexed sender,
        address indexed receiver,
        uint256 id,
        uint256 amount,
        uint256 feeAmount
    );
}
