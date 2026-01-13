# CLOB Indexer Rust

A high-performance Rust-based indexer for the CLOB (Central Limit Order Book) DEX on MegaETH chain.

## Features

- **Historical Sync**: RPC-based backfill using `eth_getLogs` with adaptive batch sizing
- **Real-time Sync**: Polling mode with 100ms intervals optimized for MegaETH's fast block times
- **Adaptive AIMD Algorithm**: Dynamic batch size and concurrency adjustment based on RPC responses
- **PostgreSQL Persistence**: Bulk inserts with UNNEST for high throughput
- **Redis Streaming**: Real-time event streaming with batch publishing
- **GraphQL API**: Query pools, orders, trades, balances, and candles
- **Candlestick Aggregation**: OHLCV candles at 1m, 5m, 15m, 1h, 4h, 1d intervals
- **Prometheus Metrics**: Built-in metrics endpoint for monitoring
- **In-memory Storage**: Fast DashMap-based concurrent storage with indexes
- **Factory Pattern Support**: Automatic discovery of OrderBook contracts via PoolCreated events
- **Gap Detection**: Automatic detection and recovery of missed blocks
- **Graceful Shutdown**: Clean shutdown handling with Ctrl+C
- **Smart Resume**: Automatically resumes from last synced block stored in database

## Project Structure

```
clob-indexer-rust/
├── Cargo.toml                     # Workspace configuration
├── deployments/
│   └── 6343.json                  # MegaETH contract addresses
├── crates/
│   ├── indexer-core/              # Types, events, config, errors
│   ├── indexer-store/             # In-memory storage
│   ├── indexer-processor/         # Event processing & sinks
│   ├── indexer-sync/              # Sync engine with adaptive batching
│   ├── indexer-db/                # PostgreSQL persistence
│   ├── indexer-redis/             # Redis streaming
│   ├── indexer-api/               # GraphQL API server
│   ├── indexer-candles/           # Candlestick aggregation
│   └── indexer-metrics/           # Prometheus metrics
└── src/
    └── main.rs                    # Entry point
```

## Prerequisites

- Rust 1.75+ (with cargo)
- Access to MegaETH RPC endpoint
- PostgreSQL (optional, for persistence)
- Redis (optional, for streaming)

## Installation

```bash
# Clone the repository
git clone https://github.com/dr-fusion/indexer-clob-rs.git
cd indexer-clob-rs

# Build the project
cargo build --release
```

## Configuration

Copy the example environment file and configure:

```bash
cp .env.example .env
```

### Required Configuration

| Variable | Description | Example |
|----------|-------------|---------|
| `CHAIN_ID` | MegaETH chain ID | `6343` |
| `RPC_URL` | HTTP RPC endpoint | `https://carrot.megaeth.com/rpc` |
| `WS_URL` | WebSocket endpoint | `wss://carrot.megaeth.com/ws` |

### Optional Configuration

#### PostgreSQL (enables persistence)
```bash
DATABASE_URL=postgres://user:pass@localhost:5432/indexer
DATABASE_MAX_CONNECTIONS=10
```

#### Redis (enables real-time streaming)
```bash
REDIS_URL=redis://127.0.0.1:6379
REDIS_STREAM_KEY=events:stream
REDIS_ASYNC=true
REDIS_QUEUE_CAPACITY=100000
REDIS_BATCH_SIZE=100
```

#### Adaptive Batch Sizing (AIMD)
```bash
BATCH_SIZE_INITIAL=10000      # Starting batch size
BATCH_SIZE_MIN=10             # Minimum (floor)
BATCH_SIZE_MAX=10000          # Maximum (ceiling)
BATCH_SIZE_DECREASE_FACTOR=0.5  # Multiply on error (halve)
BATCH_SIZE_INCREASE_FACTOR=0.1  # Increase by 10% after successes
BATCH_SIZE_SUCCESS_THRESHOLD=10
```

#### Adaptive Concurrency (AIMD)
```bash
CONCURRENCY_INITIAL=10        # Starting concurrency
CONCURRENCY_MIN=1             # Minimum (floor)
CONCURRENCY_MAX=20            # Maximum (ceiling)
CONCURRENCY_DECREASE_FACTOR=0.5
CONCURRENCY_INCREASE_FACTOR=0.1
CONCURRENCY_SUCCESS_THRESHOLD=20
```

#### API Server
```bash
API_HOST=0.0.0.0
API_PORT=4000
```

#### Metrics
```bash
METRICS_HOST=0.0.0.0
METRICS_PORT=9090
METRICS_ENABLED=true
```

### Deployment Configuration

Contract addresses are loaded from `deployments/{CHAIN_ID}.json`:
- `PROXY_POOLMANAGER`: PoolManager contract address
- `PROXY_BALANCEMANAGER`: BalanceManager contract address
- `startBlock`: Block number to start indexing from

## Usage

```bash
# Run with debug logging
cargo run

# Run optimized release build
cargo run --release

# Set custom log level
RUST_LOG=info cargo run --release
```

## API Endpoints

### GraphQL API (port 4000)
- `GET /graphql` - GraphQL Playground
- `POST /graphql` - GraphQL queries

### Prometheus Metrics (port 9090)
- `GET /metrics` - Prometheus metrics

## Events Indexed

### PoolManager
| Event | Description |
|-------|-------------|
| `PoolCreated` | New trading pool created |

### OrderBook (per pool)
| Event | Description |
|-------|-------------|
| `OrderPlaced` | New order placed |
| `OrderMatched` | Trade executed |
| `UpdateOrder` | Order state changed |
| `OrderCancelled` | Order cancelled |

### BalanceManager
| Event | Description |
|-------|-------------|
| `Deposit` | User deposits funds |
| `Withdrawal` | User withdraws funds |
| `Lock` | Funds locked for order |
| `Unlock` | Funds unlocked |
| `TransferFrom` | Transfer with fees |
| `TransferLockedFrom` | Locked transfer |

## Architecture

### Sync Flow

1. **Startup**
   - Loads configuration from env and deployment JSON
   - Connects to PostgreSQL (if configured)
   - Checks `sync_state` table for last synced block to resume from
   - Falls back to deployment JSON's `startBlock` if no previous state

2. **Historical Sync**
   - Fetches events using `eth_getLogs` with adaptive batch sizes
   - Processes batches in parallel with adaptive concurrency
   - Automatically splits batches on "too many logs" errors
   - Backs off on rate limit (429) errors
   - Persists sync state periodically

3. **Real-time Sync**
   - Polls for new blocks every 100ms
   - Processes events from all discovered contracts
   - Detects and fills gaps automatically

### Data Flow

```
RPC Events → EventProcessor → [Sinks]
                                ├── DatabaseSink → PostgreSQL (bulk inserts)
                                ├── RedisSink → Redis Streams
                                └── CandleSink → Candle Aggregator → PostgreSQL
```

### Storage

- **In-memory**: DashMap-based concurrent storage for fast access
- **PostgreSQL**: Persistent storage with bulk UNNEST inserts
- **Redis**: Real-time event streaming with pipeline batching

## Status Output

The indexer prints status every 30 seconds:

```
INFO Status mode=Realtime last_block=12345678 pools=5 orders=1000 trades=500 events=5000
```

## License

MIT
