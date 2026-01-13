-- Initial schema for CLOB Indexer
-- Matches Ponder reference implementation

-- Pools table
CREATE TABLE IF NOT EXISTS pools (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    coin VARCHAR,
    order_book TEXT NOT NULL,
    base_currency TEXT NOT NULL,
    quote_currency TEXT NOT NULL,
    base_decimals SMALLINT DEFAULT 18,
    quote_decimals SMALLINT DEFAULT 18,
    timestamp INTEGER,
    token0_price REAL,
    token1_price REAL
);
CREATE INDEX IF NOT EXISTS idx_pools_chain ON pools(chain_id);
CREATE INDEX IF NOT EXISTS idx_pools_orderbook ON pools(order_book);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    pool_id TEXT NOT NULL,
    order_id BIGINT NOT NULL,
    transaction_id TEXT,
    "user" TEXT,
    side VARCHAR,
    timestamp INTEGER,
    price BIGINT,
    quantity BIGINT,
    filled BIGINT DEFAULT 0,
    total_filled_value BIGINT DEFAULT 0,
    type VARCHAR,
    status VARCHAR,
    expiry INTEGER
);
CREATE INDEX IF NOT EXISTS idx_orders_user ON orders("user");
CREATE INDEX IF NOT EXISTS idx_orders_pool ON orders(pool_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

-- Order history table
CREATE TABLE IF NOT EXISTS order_history (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    pool_id TEXT NOT NULL,
    order_id TEXT,
    transaction_id TEXT,
    timestamp INTEGER,
    filled BIGINT,
    status VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_order_history_order ON order_history(order_id);

-- Trades table (per-order fills)
CREATE TABLE IF NOT EXISTS trades (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    transaction_id TEXT,
    pool_id TEXT NOT NULL,
    order_id TEXT,
    price BIGINT,
    quantity BIGINT,
    timestamp INTEGER,
    log_index INTEGER
);
CREATE INDEX IF NOT EXISTS idx_trades_pool ON trades(pool_id);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);

-- Order book trades table (market-level)
CREATE TABLE IF NOT EXISTS order_book_trades (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    price BIGINT,
    taker_limit_price BIGINT,
    quantity BIGINT,
    timestamp INTEGER,
    log_index INTEGER,
    transaction_id TEXT,
    side VARCHAR,
    pool_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ob_trades_pool ON order_book_trades(pool_id);
CREATE INDEX IF NOT EXISTS idx_ob_trades_timestamp ON order_book_trades(timestamp);

-- Balances table
CREATE TABLE IF NOT EXISTS balances (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    "user" TEXT,
    currency TEXT,
    amount BIGINT DEFAULT 0,
    locked_amount BIGINT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_balances_user ON balances("user");

-- Users table
CREATE TABLE IF NOT EXISTS users (
    "user" TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    created_at INTEGER
);

-- Currencies table
CREATE TABLE IF NOT EXISTS currencies (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    address TEXT NOT NULL,
    name VARCHAR,
    symbol VARCHAR,
    decimals SMALLINT,
    price_usd REAL
);

-- Candlestick bucket tables (9 intervals, same schema)
CREATE TABLE IF NOT EXISTS minute_buckets (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    open_time INTEGER NOT NULL,
    close_time INTEGER NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    average DOUBLE PRECISION NOT NULL DEFAULT 0,
    pool_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_minute_opentime ON minute_buckets(open_time);
CREATE INDEX IF NOT EXISTS idx_minute_pool ON minute_buckets(pool_id);

CREATE TABLE IF NOT EXISTS five_minute_buckets (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    open_time INTEGER NOT NULL,
    close_time INTEGER NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    average DOUBLE PRECISION NOT NULL DEFAULT 0,
    pool_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_five_minute_opentime ON five_minute_buckets(open_time);
CREATE INDEX IF NOT EXISTS idx_five_minute_pool ON five_minute_buckets(pool_id);

CREATE TABLE IF NOT EXISTS fifteen_minute_buckets (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    open_time INTEGER NOT NULL,
    close_time INTEGER NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    average DOUBLE PRECISION NOT NULL DEFAULT 0,
    pool_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fifteen_minute_opentime ON fifteen_minute_buckets(open_time);
CREATE INDEX IF NOT EXISTS idx_fifteen_minute_pool ON fifteen_minute_buckets(pool_id);

CREATE TABLE IF NOT EXISTS hour_buckets (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    open_time INTEGER NOT NULL,
    close_time INTEGER NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    average DOUBLE PRECISION NOT NULL DEFAULT 0,
    pool_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_hour_opentime ON hour_buckets(open_time);
CREATE INDEX IF NOT EXISTS idx_hour_pool ON hour_buckets(pool_id);

CREATE TABLE IF NOT EXISTS two_hour_buckets (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    open_time INTEGER NOT NULL,
    close_time INTEGER NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    average DOUBLE PRECISION NOT NULL DEFAULT 0,
    pool_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_two_hour_opentime ON two_hour_buckets(open_time);
CREATE INDEX IF NOT EXISTS idx_two_hour_pool ON two_hour_buckets(pool_id);

CREATE TABLE IF NOT EXISTS four_hour_buckets (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    open_time INTEGER NOT NULL,
    close_time INTEGER NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    average DOUBLE PRECISION NOT NULL DEFAULT 0,
    pool_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_four_hour_opentime ON four_hour_buckets(open_time);
CREATE INDEX IF NOT EXISTS idx_four_hour_pool ON four_hour_buckets(pool_id);

CREATE TABLE IF NOT EXISTS daily_buckets (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    open_time INTEGER NOT NULL,
    close_time INTEGER NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    average DOUBLE PRECISION NOT NULL DEFAULT 0,
    pool_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_daily_opentime ON daily_buckets(open_time);
CREATE INDEX IF NOT EXISTS idx_daily_pool ON daily_buckets(pool_id);

CREATE TABLE IF NOT EXISTS weekly_buckets (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    open_time INTEGER NOT NULL,
    close_time INTEGER NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    average DOUBLE PRECISION NOT NULL DEFAULT 0,
    pool_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_weekly_opentime ON weekly_buckets(open_time);
CREATE INDEX IF NOT EXISTS idx_weekly_pool ON weekly_buckets(pool_id);

CREATE TABLE IF NOT EXISTS monthly_buckets (
    id TEXT PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    open_time INTEGER NOT NULL,
    close_time INTEGER NOT NULL,
    open DOUBLE PRECISION NOT NULL,
    high DOUBLE PRECISION NOT NULL,
    low DOUBLE PRECISION NOT NULL,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
    count INTEGER NOT NULL DEFAULT 0,
    taker_buy_base_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    taker_buy_quote_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    average DOUBLE PRECISION NOT NULL DEFAULT 0,
    pool_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_monthly_opentime ON monthly_buckets(open_time);
CREATE INDEX IF NOT EXISTS idx_monthly_pool ON monthly_buckets(pool_id);

-- Sync state table (for tracking indexer progress)
CREATE TABLE IF NOT EXISTS sync_state (
    id TEXT PRIMARY KEY DEFAULT 'main',
    last_synced_block BIGINT NOT NULL DEFAULT 0,
    last_synced_timestamp BIGINT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert default sync state
INSERT INTO sync_state (id, last_synced_block) VALUES ('main', 0) ON CONFLICT (id) DO NOTHING;
