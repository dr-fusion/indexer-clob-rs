use crate::config::ApiConfig;
use crate::schema::{build_schema, ApiContext, ApiSchema};
use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use indexer_db::DatabasePool;
use indexer_store::IndexerStore;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;

/// GraphQL API Server
pub struct ApiServer {
    config: ApiConfig,
    schema: ApiSchema,
}

impl ApiServer {
    /// Create a new API server
    pub fn new(config: ApiConfig, db_pool: Arc<DatabasePool>, store: Arc<IndexerStore>) -> Self {
        let ctx = ApiContext::new(db_pool, store);
        let schema = build_schema(ctx);

        Self { config, schema }
    }

    /// Start the server
    pub async fn run(self) -> crate::Result<()> {
        let addr = self.config.address();

        // Build CORS layer
        let cors = if self.config.cors_enabled {
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any)
        } else {
            CorsLayer::new()
        };

        // Build router
        let app = Router::new()
            .route("/", get(graphiql).post(graphql_handler))
            .route("/graphql", get(graphiql).post(graphql_handler))
            .route("/health", get(health_check))
            .with_state(self.schema)
            .layer(cors)
            .layer(TraceLayer::new_for_http());

        info!(address = %addr, "Starting GraphQL API server");

        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| crate::ApiError::Server(e.to_string()))?;

        axum::serve(listener, app)
            .await
            .map_err(|e| crate::ApiError::Server(e.to_string()))?;

        Ok(())
    }
}

/// GraphQL handler
async fn graphql_handler(
    State(schema): State<ApiSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

/// GraphiQL playground
async fn graphiql() -> impl IntoResponse {
    Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .finish(),
    )
}

/// Health check endpoint
async fn health_check() -> impl IntoResponse {
    "OK"
}
