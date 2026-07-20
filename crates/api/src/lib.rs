//! Historical-query API: DataFusion over the Parquet lake on MinIO, served as
//! a small read-only REST service (the cold path's reader).
//!
//! At startup it registers the MinIO bucket as an S3 object store and one
//! Parquet table per topic, then serves typed query endpoints. Queries are
//! built with the DataFrame API (filters use `lit(...)`, so user-supplied
//! symbols/intervals are not string-interpolated into SQL).

pub mod config;

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::json::ArrayWriter;
use axum::extract::{Query, State};
use axum::http::{HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use serde::Deserialize;
use tower_http::cors::CorsLayer;
use tracing::{info, warn};
use url::Url;

use crate::config::Config;
use common::topics;

/// DataFusion table names (distinct from the Kafka topic names).
const TRADES: &str = "trades";
const BOOK_TICKERS: &str = "book_tickers";
const KLINES: &str = "klines";

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

type AppState = Arc<SessionContext>;

pub async fn run(cfg: Config) -> Result<()> {
    let ctx = build_context(&cfg).await?;

    // The browser frontend runs on a different port, so it needs CORS. Scoped
    // to one configured origin (not wide open) and read-only methods.
    let cors = CorsLayer::new()
        .allow_origin(
            cfg.cors_origin
                .parse::<HeaderValue>()
                .with_context(|| format!("invalid API_CORS_ORIGIN `{}`", cfg.cors_origin))?,
        )
        .allow_methods([Method::GET]);

    let app = Router::new()
        .route("/health", get(health))
        .route("/trades", get(trades))
        .route("/book_tickers", get(book_tickers))
        .route("/klines", get(klines))
        .layer(cors)
        .with_state(Arc::new(ctx));

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr)
        .await
        .with_context(|| format!("failed to bind {}", cfg.listen_addr))?;
    info!(addr = %cfg.listen_addr, "api listening");
    axum::serve(listener, app).await.context("server error")?;
    Ok(())
}

/// Build a SessionContext with the MinIO object store and lake tables.
async fn build_context(cfg: &Config) -> Result<SessionContext> {
    let ctx = SessionContext::new();

    // `allow_http` is required because local MinIO serves plain HTTP.
    let store = AmazonS3Builder::new()
        .with_endpoint(&cfg.minio_endpoint)
        .with_bucket_name(&cfg.bucket)
        .with_access_key_id(&cfg.minio_access_key)
        .with_secret_access_key(&cfg.minio_secret_key)
        .with_region(&cfg.minio_region)
        .with_allow_http(true)
        .build()
        .context("failed to build MinIO (S3) client")?;

    let base = format!("s3://{}", cfg.bucket);
    let url = Url::parse(&base).with_context(|| format!("parsing object-store URL {base}"))?;
    ctx.register_object_store(&url, Arc::new(store));

    // One Parquet table per topic. Schema is inferred from existing files, so a
    // table with no data yet is skipped (re-run after the cold-consumer writes).
    register_table(&ctx, TRADES, topics::TRADES, cfg).await;
    register_table(&ctx, BOOK_TICKERS, topics::BOOK_TICKERS, cfg).await;
    register_table(&ctx, KLINES, topics::KLINES, cfg).await;
    Ok(ctx)
}

async fn register_table(ctx: &SessionContext, name: &str, topic: &str, cfg: &Config) {
    let path = format!("s3://{}/{}/", cfg.bucket, topic);
    match ctx
        .register_parquet(name, &path, ParquetReadOptions::default())
        .await
    {
        Ok(()) => info!(table = name, path, "registered lake table"),
        Err(error) => warn!(
            table = name,
            path,
            error = %error,
            "table not registered — no data in the lake yet? run the cold-consumer, then restart the api"
        ),
    }
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

#[derive(Debug, Deserialize)]
struct Params {
    symbol: Option<String>,
    interval: Option<String>,
    limit: Option<usize>,
}

fn clamp_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

/// Optionally filter by symbol.
fn with_symbol(df: DataFrame, symbol: &Option<String>) -> Result<DataFrame, AppError> {
    match symbol {
        Some(sym) => Ok(df.filter(col("symbol").eq(lit(sym.as_str())))?),
        None => Ok(df),
    }
}

async fn trades(
    State(ctx): State<AppState>,
    Query(p): Query<Params>,
) -> Result<Json<serde_json::Value>, AppError> {
    let df = with_symbol(ctx.table(TRADES).await?, &p.symbol)?
        .sort(vec![col("trade_time").sort(false, false)])?
        .limit(0, Some(clamp_limit(p.limit)))?;
    Ok(Json(batches_to_json(&df.collect().await?)?))
}

async fn book_tickers(
    State(ctx): State<AppState>,
    Query(p): Query<Params>,
) -> Result<Json<serde_json::Value>, AppError> {
    let df = with_symbol(ctx.table(BOOK_TICKERS).await?, &p.symbol)?
        .sort(vec![col("update_id").sort(false, false)])?
        .limit(0, Some(clamp_limit(p.limit)))?;
    Ok(Json(batches_to_json(&df.collect().await?)?))
}

async fn klines(
    State(ctx): State<AppState>,
    Query(p): Query<Params>,
) -> Result<Json<serde_json::Value>, AppError> {
    let mut df = with_symbol(ctx.table(KLINES).await?, &p.symbol)?;
    if let Some(interval) = &p.interval {
        df = df.filter(col("interval").eq(lit(interval.as_str())))?;
    }
    let df = df
        .sort(vec![col("open_time").sort(false, false)])?
        .limit(0, Some(clamp_limit(p.limit)))?;
    Ok(Json(batches_to_json(&df.collect().await?)?))
}

/// Serialize Arrow record batches to a JSON array of row objects.
fn batches_to_json(batches: &[RecordBatch]) -> Result<serde_json::Value, AppError> {
    let mut buf = Vec::new();
    {
        let mut writer = ArrayWriter::new(&mut buf);
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
    }
    if buf.is_empty() {
        return Ok(serde_json::json!([]));
    }
    Ok(serde_json::from_slice(&buf)?)
}

/// Any handler error → 500 with a JSON error body. Uses the axum + anyhow
/// pattern so `?` works with DataFusion/Arrow/serde errors.
#[derive(Debug)]
struct AppError(anyhow::Error);

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        warn!(error = %format!("{:#}", self.0), "request failed");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_limit_applies_default_and_ceiling() {
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(10)), 10);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(5_000)), MAX_LIMIT);
    }

    #[test]
    fn empty_batches_serialize_to_empty_array() {
        let json = batches_to_json(&[]).expect("empty serializes");
        assert_eq!(json, serde_json::json!([]));
    }
}
