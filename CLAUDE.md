# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Build
cargo build              # Debug build
cargo build --release    # Optimized build

# Run
cargo run                # Run with debug logging
cargo run --release      # Run optimized
RUST_LOG=info cargo run --release  # Custom log level

# Test
cargo test                          # Run all tests
cargo test -p indexer-sync          # Run tests for specific crate
cargo test test_name                # Run single test by name
cargo test -p indexer-db -- --nocapture  # Run with stdout visible

# Check/Lint
cargo check              # Fast type checking without full build
cargo clippy             # Run linter
cargo fmt                # Format code
```

## Architecture Overview

This is a CLOB (Central Limit Order Book) DEX indexer for MegaETH chain, organized as a Cargo workspace with 9 crates:

```
crates/
├── indexer-core/       # Foundation: types, events, config (no deps on other crates)
├── indexer-store/      # In-memory concurrent storage (DashMap-based)
├── indexer-sync/       # Dual-phase sync engine (historical + real-time)
├── indexer-processor/  # Event routing, handlers, and sinks
├── indexer-db/         # PostgreSQL persistence with SQLx
├── indexer-redis/      # Redis streaming publisher
├── indexer-api/        # GraphQL API server (async-graphql + Axum)
├── indexer-candles/    # OHLCV candlestick aggregation
└── indexer-metrics/    # Prometheus metrics exposition
```

### Core Data Flow

```
Blockchain → SyncEngine → EventProcessor → [Sinks] → Storage
                                              ├── DatabaseSink → PostgreSQL
                                              ├── RedisSink → Redis Streams
                                              └── CandleSink → Candle Aggregator
```

### Sync Engine (indexer-sync)

The sync engine uses a dual-phase approach:

1. **Historical Sync** (`historical.rs`): Fetches past events via `eth_getLogs` with adaptive AIMD batch sizing
2. **Real-time Sync** (`realtime.rs`): WebSocket subscription with ~10ms latency for MegaETH mini-blocks
3. **Gap Detection** (`gap_detector.rs`): Monitors and fills missed blocks during WebSocket operation

Key patterns:
- AIMD (Additive Increase, Multiplicative Decrease) for batch sizing and concurrency control
- Verification loop ensures no gaps before switching to real-time mode
- BlockRangeTracker (`adaptive_batch.rs`) tracks contiguous synced ranges

### Event Processing (indexer-processor)

- `pipeline.rs`: EventProcessor routes logs to handlers based on event signature
- `handlers/`: Individual handlers for each event type (pool_created, order_placed, order_matched, etc.)
- `sinks/`: Output destinations (database, redis, candle) with CompositeSink for fan-out

### Database Layer (indexer-db)

- Migrations: `crates/indexer-db/src/migrations/0001_initial_schema.sql`
- BatchWriter: Uses mpsc channel with background flush task and PostgreSQL UNNEST for bulk inserts
- 13 main tables + 9 candle interval tables (minute through monthly)

## Key Implementation Details

- **Startup sequence**: Pools are loaded from database BEFORE WebSocket subscription to ensure filter accuracy
- **Shutdown coordination**: AtomicBool flag checked throughout async tasks for graceful shutdown
- **Concurrency**: DashMap for lock-free concurrent access, no mutexes needed for stores
- **Contract addresses**: Loaded from `deployments/{CHAIN_ID}.json`
- **Environment config**: See `.env.example` for all configuration options

## Indexed Events

- **PoolManager**: PoolCreated
- **OrderBook**: OrderPlaced, OrderMatched, UpdateOrder, OrderCancelled
- **BalanceManager**: Deposit, Withdrawal, Lock, Unlock, TransferFrom, TransferLockedFrom

## Crate Dependency Order

Build dependencies flow in this direction (no cycles):
```
indexer-core (foundation - no internal deps)
    ↓
indexer-store (depends on core)
    ↓
indexer-db, indexer-redis, indexer-candles, indexer-metrics (depend on core)
    ↓
indexer-processor (depends on core, store, db, redis, candles)
    ↓
indexer-sync (depends on core, store, processor)
    ↓
indexer-api (depends on core, store, db)
```

## Key External Dependencies

- **alloy**: Ethereum RPC/types (replaces ethers-rs)
- **tokio**: Async runtime
- **sqlx**: PostgreSQL with compile-time checked queries
- **async-graphql + axum**: GraphQL API
- **dashmap**: Lock-free concurrent HashMap
