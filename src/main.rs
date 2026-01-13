use alloy::primitives::{Address, FixedBytes, B256};
use indexer_api::{ApiConfig, ApiServer};
use indexer_candles::CandleAggregatorBuilder;
use indexer_core::{types::Pool, IndexerConfig};
use indexer_db::{
    repositories::{PoolRepository, SyncStateRepository},
    BatchWriter, BatchWriterConfig, DatabaseConfig, DatabasePool,
};
use indexer_metrics::{MetricsConfig, MetricsServer};
use indexer_processor::{CandleSink, CompositeSink, DatabaseSink, RedisSink};
use indexer_redis::{RedisConfig, RedisConnection, RedisPublisher};
use indexer_store::IndexerStore;
use indexer_sync::SyncEngine;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{error, info, warn, Level};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file (ignore if not found)
    dotenvy::dotenv().ok();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env()
                .add_directive(Level::INFO.into())
                .add_directive("indexer_sync=info".parse()?)
                .add_directive("indexer_processor=info".parse()?),
        )
        .init();

    info!("CLOB Indexer starting (Phase 2)...");

    // Load configuration (validates env vars and loads deployment file)
    let mut config = match IndexerConfig::load() {
        Ok(config) => {
            info!(
                chain_id = config.chain_id,
                pool_manager = ?config.pool_manager,
                balance_manager = ?config.balance_manager,
                deployment_start_block = config.start_block,
                "Configuration loaded from deployment"
            );
            config
        }
        Err(e) => {
            error!(error = %e, "Failed to load configuration");
            std::process::exit(1);
        }
    };

    // Initialize database (optional - only if DATABASE_URL is set)
    let (db_pool, batch_writer) = match std::env::var("DATABASE_URL") {
        Ok(_) => {
            let db_config = DatabaseConfig::from_env();
            match DatabasePool::new(&db_config).await {
                Ok(pool) => {
                    // Run migrations
                    if let Err(e) = pool.migrate().await {
                        error!(error = %e, "Failed to run database migrations");
                        std::process::exit(1);
                    }
                    info!("Database connected and migrations applied");

                    // Check for last synced block in DB (resume from where we left off)
                    match SyncStateRepository::get_last_synced_block(pool.inner()).await {
                        Ok(Some(last_block)) => {
                            info!(
                                last_synced_block = last_block,
                                deployment_start_block = config.start_block,
                                "Resuming from database sync state"
                            );
                            // Resume from the next block after the last synced one
                            config.start_block = last_block + 1;
                        }
                        Ok(None) => {
                            info!(
                                start_block = config.start_block,
                                "No previous sync state found, starting from deployment config"
                            );
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to query sync state, using deployment config");
                        }
                    }

                    let pool = Arc::new(pool);
                    // Create batch writer with config from env
                    let writer_config = BatchWriterConfig::from_env();
                    info!(
                        batch_size = writer_config.batch_size,
                        flush_interval_ms = writer_config.flush_interval_ms,
                        channel_capacity = writer_config.channel_capacity,
                        "BatchWriter configuration"
                    );
                    let writer = Arc::new(BatchWriter::with_config(pool.clone(), writer_config));
                    (Some(pool), Some(writer))
                }
                Err(e) => {
                    error!(error = %e, "Failed to connect to database");
                    std::process::exit(1);
                }
            }
        }
        Err(_) => {
            warn!("DATABASE_URL not set, running without persistence");
            (None, None)
        }
    };

    // Initialize Redis (optional - only if REDIS_URL is set)
    let redis_publisher = match std::env::var("REDIS_URL") {
        Ok(_) => {
            let redis_config = RedisConfig::from_env();
            match RedisConnection::new(redis_config).await {
                Ok(conn) => {
                    let publisher = RedisPublisher::new(Arc::new(conn));
                    info!("Redis connected for streaming");
                    Some(Arc::new(publisher))
                }
                Err(e) => {
                    error!(error = %e, "Failed to connect to Redis");
                    std::process::exit(1);
                }
            }
        }
        Err(_) => {
            warn!("REDIS_URL not set, running without Redis streaming");
            None
        }
    };

    // Create in-memory store
    let store = Arc::new(IndexerStore::new());

    // Load existing pools from database (critical for resuming sync)
    // Without this, WebSocket filtering would miss all orderbook events on restart
    if let Some(db) = db_pool.as_ref() {
        match PoolRepository::get_all_by_chain(db.inner(), config.chain_id as i64).await {
            Ok(db_pools) => {
                if !db_pools.is_empty() {
                    // Convert DbPool to core Pool type
                    let pools = db_pools.into_iter().filter_map(|db_pool| {
                        // Parse hex strings to alloy types
                        let pool_id: FixedBytes<32> = db_pool.id.parse().ok()?;

                        let order_book_address: Address = db_pool.order_book.parse().ok()?;
                        let base_currency: Address = db_pool.base_currency.parse().ok()?;
                        let quote_currency: Address = db_pool.quote_currency.parse().ok()?;

                        Some(Pool {
                            pool_id,
                            order_book_address,
                            base_currency,
                            quote_currency,
                            created_at_block: 0, // Not stored in DB, use 0
                            created_at_tx: B256::ZERO, // Not stored in DB, use zero
                        })
                    });

                    let count = store.pools.bulk_insert(pools);
                    info!(
                        pools_loaded = count,
                        "Restored pools from database into memory"
                    );
                } else {
                    info!("No existing pools found in database");
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to load pools from database, starting with empty pool store");
            }
        }
    }

    // Create sync engine
    let mut engine = match SyncEngine::new(config.clone(), store.clone()).await {
        Ok(engine) => engine,
        Err(e) => {
            error!(error = %e, "Failed to create sync engine");
            std::process::exit(1);
        }
    };

    // Wire up sinks to the event processor
    let mut composite_sink = CompositeSink::new();

    // Add database sink if available
    if let Some(writer) = batch_writer.clone() {
        let db_sink = DatabaseSink::new(writer, config.chain_id as i64);
        composite_sink.add_sink(Arc::new(db_sink));
        info!("Database sink configured");
    }

    // Add Redis sink if available
    if let Some(publisher) = redis_publisher.clone() {
        let redis_sink = RedisSink::new(publisher);
        composite_sink.add_sink(Arc::new(redis_sink));
        info!("Redis sink configured");
    }

    // Add Candle sink if database is available
    if let Some(db) = db_pool.clone() {
        let flush_interval = std::env::var("CANDLE_FLUSH_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(60u64);
        let retention = std::env::var("CANDLE_RETENTION_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(86400u64);

        let mut builder = CandleAggregatorBuilder::new(db)
            .with_flush_interval(flush_interval)
            .with_retention(retention);

        if let Some(pub_) = redis_publisher.clone() {
            builder = builder.with_publisher(pub_);
        }

        let candle_aggregator = Arc::new(builder.build());
        let candle_sink = CandleSink::new(candle_aggregator, config.chain_id as i64);
        composite_sink.add_sink(Arc::new(candle_sink));
        info!(
            flush_interval_secs = flush_interval,
            retention_secs = retention,
            "Candle sink configured"
        );
    }

    // Set sinks on processor
    if !composite_sink.is_empty() {
        engine.processor().set_sinks(composite_sink).await;
        info!("Event sinks configured and wired to processor");
    }

    // Setup shutdown signal
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);

    // Handle Ctrl+C
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received (Ctrl+C)");
        shutdown_tx_clone.send(()).ok();
    });

    // Start Metrics server (if enabled)
    if std::env::var("METRICS_PORT").is_ok() || std::env::var("METRICS_ENABLED").is_ok() {
        let metrics_config = MetricsConfig::from_env();
        let metrics_server = MetricsServer::new(metrics_config);
        tokio::spawn(async move {
            if let Err(e) = metrics_server.run().await {
                error!(error = %e, "Metrics server error");
            }
        });
        info!("Metrics server started");
    }

    // Start GraphQL API server (if database is available)
    if let Some(db) = db_pool.clone() {
        let api_config = ApiConfig::from_env();
        let api_server = ApiServer::new(api_config, db, store.clone());
        tokio::spawn(async move {
            if let Err(e) = api_server.run().await {
                error!(error = %e, "API server error");
            }
        });
        info!("GraphQL API server started");
    }

    // Spawn status printer and sync state persistence
    let store_clone = store.clone();
    let db_pool_for_status = db_pool.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        let mut last_persisted_block: u64 = 0;
        loop {
            interval.tick().await;
            let state = store_clone.sync_state.read().await;
            let current_block = state.last_synced_block;

            info!(
                mode = ?state.mode,
                last_block = current_block,
                pools = store_clone.pools.count(),
                orders = store_clone.orders.count(),
                trades = store_clone.trades.count(),
                events = state.stats.total_events_processed,
                "Status"
            );

            // Update metrics gauges
            indexer_metrics::gauges::set_current_block(state.last_synced_block());
            indexer_metrics::gauges::set_memory_pools(store_clone.pools.count());
            indexer_metrics::gauges::set_memory_orders(store_clone.orders.count());
            indexer_metrics::gauges::set_memory_trades(store_clone.trades.count());

            // Persist sync state to database (if block advanced)
            if let Some(db) = db_pool_for_status.as_ref() {
                if current_block > last_persisted_block {
                    match SyncStateRepository::set_last_synced_block(db.inner(), current_block).await
                    {
                        Ok(()) => {
                            last_persisted_block = current_block;
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to persist sync state to database");
                        }
                    }
                }
            }
        }
    });

    // Run sync engine
    if let Err(e) = engine.run(shutdown_rx).await {
        error!(error = %e, "Sync engine error");
        std::process::exit(1);
    }

    // Graceful shutdown
    info!("Shutting down...");

    // Persist final sync state to database before closing
    if let Some(db) = db_pool.as_ref() {
        let state = store.sync_state.read().await;
        let final_block = state.last_synced_block;
        if final_block > 0 {
            match SyncStateRepository::set_last_synced_block(db.inner(), final_block).await {
                Ok(()) => info!(block = final_block, "Final sync state persisted"),
                Err(e) => warn!(error = %e, "Failed to persist final sync state"),
            }
        }
    }

    // Close database connections
    if let Some(db) = db_pool {
        db.close().await;
        info!("Database connections closed");
    }

    info!("CLOB Indexer shutdown complete");
    Ok(())
}
