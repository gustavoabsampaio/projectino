//! Historical-query API skeleton: starts an empty Axum server with a /health
//! route and creates a DataFusion session. No real queries yet.

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use datafusion::prelude::SessionContext;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    common::config::load_dotenv();
    common::config::init_tracing();

    let addr = common::config::optional("API_LISTEN_ADDR", "127.0.0.1:8081");

    // TODO: register the MinIO object store and lake tables on this context,
    // then serve historical queries from it.
    let _ctx = SessionContext::new();
    info!("DataFusion session context created");

    let app = Router::new().route("/health", get(health));

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    info!(%addr, "api listening");
    axum::serve(listener, app).await.context("server error")?;
    Ok(())
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}
