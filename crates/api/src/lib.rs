//! Historical-query API: DataFusion over the Parquet lake on MinIO, served as
//! a small read-only REST service (the cold path's reader).
//!
//! At startup it registers the MinIO bucket as an S3 object store and one
//! Parquet table per topic, then serves typed query endpoints. Queries are
//! built with the DataFrame API (filters use `lit(...)`, so user-supplied
//! symbols/intervals are not string-interpolated into SQL).

pub mod config;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
use tokio::sync::Mutex;
use tower_http::cors::CorsLayer;
use tracing::{debug, info, warn};
use url::Url;

use crate::config::Config;
use common::topics;

/// DataFusion table names (distinct from the Kafka topic names).
const TRADES: &str = "trades";
const BOOK_TICKERS: &str = "book_tickers";
const KLINES: &str = "klines";

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

/// The lake tables: (DataFusion table name, Kafka topic / lake prefix).
fn lake_tables() -> [(&'static str, &'static str); 3] {
    [
        (TRADES, topics::TRADES),
        (BOOK_TICKERS, topics::BOOK_TICKERS),
        (KLINES, topics::KLINES),
    ]
}

/// Handler state: the DataFusion context plus the config needed to refresh
/// table registrations.
struct AppState {
    ctx: SessionContext,
    cfg: Config,
    /// When each table's file listing was last refreshed, `None` if it has
    /// never registered successfully. Keys are fixed at startup (one per
    /// [`lake_tables`] entry), so the map itself never needs locking.
    ///
    /// The per-table `Mutex` is held across the refresh, which also collapses
    /// concurrent requests for the same table onto a single re-listing instead
    /// of each starting its own.
    refreshed: HashMap<&'static str, Mutex<Option<Instant>>>,
}

type SharedState = Arc<AppState>;

pub async fn run(cfg: Config) -> Result<()> {
    let state = build_state(cfg.clone()).await?;

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
        .with_state(Arc::new(state));

    let listener = tokio::net::TcpListener::bind(&cfg.listen_addr)
        .await
        .with_context(|| format!("failed to bind {}", cfg.listen_addr))?;
    info!(addr = %cfg.listen_addr, "api listening");
    axum::serve(listener, app).await.context("server error")?;
    Ok(())
}

/// Build the handler state: a SessionContext with the MinIO object store and
/// the lake tables registered.
async fn build_state(cfg: Config) -> Result<AppState> {
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

    // One Parquet table per topic. A table with no data yet simply fails to
    // register here; queries refresh the registration anyway (see below), and
    // a `None` timestamp makes the next query retry immediately.
    let mut refreshed = HashMap::new();
    for (name, topic) in lake_tables() {
        let at = match register_table(&ctx, name, topic, &cfg).await {
            Ok(()) => {
                info!(table = name, "registered lake table");
                Some(Instant::now())
            }
            Err(error) => {
                warn!(
                    table = name,
                    error = %error,
                    "table not registered yet — no data in the lake? it will register on first query once the cold-consumer writes"
                );
                None
            }
        };
        refreshed.insert(name, Mutex::new(at));
    }

    Ok(AppState {
        ctx,
        cfg,
        refreshed,
    })
}

/// (Re)register a Parquet table over its lake prefix.
///
/// DataFusion resolves the set of files when the table is registered, so a
/// long-running api would never see Parquet written after startup. Re-running
/// this refreshes that listing.
async fn register_table(ctx: &SessionContext, name: &str, topic: &str, cfg: &Config) -> Result<()> {
    let path = format!("s3://{}/{}/", cfg.bucket, topic);
    // `register_parquet` errors with "table already exists" rather than
    // replacing, so drop the previous registration first (no-op on first call).
    let _ = ctx.deregister_table(name);
    ctx.register_parquet(name, &path, ParquetReadOptions::default())
        .await
        .with_context(|| format!("registering {name} from {path}"))
}

/// Is a listing last refreshed at `last` due for a refresh?
fn is_stale(last: Option<Instant>, ttl: Duration) -> bool {
    // Never registered → always due, so an empty lake picks up its first
    // Parquet as soon as the cold-consumer writes it.
    last.is_none_or(|at| at.elapsed() >= ttl)
}

/// Refresh the table registration if its listing has gone stale, so newly
/// written Parquet becomes visible, then return a DataFrame over it.
///
/// Re-listing costs a full `LIST` over the lake prefix, which grows with the
/// lake (measured: ~5–7s at 1,611 files). Doing that per request made any
/// client polling faster than the response time — the 1s chart — never see a
/// response that wasn't already superseded. The TTL caps re-listing at once
/// per `listing_ttl` no matter the request rate, so a fast poller mostly hits
/// the cached listing and pays only for the query.
async fn fresh_table(
    state: &AppState,
    name: &'static str,
    topic: &str,
) -> Result<DataFrame, AppError> {
    match state.refreshed.get(name) {
        Some(cell) => {
            let mut last = cell.lock().await;
            // Re-checked under the lock: while we waited, another request may
            // have just refreshed this table, and then there's nothing to do.
            if is_stale(*last, state.cfg.listing_ttl) {
                match register_table(&state.ctx, name, topic, &state.cfg).await {
                    Ok(()) => *last = Some(Instant::now()),
                    // Don't fail a table that is already registered just
                    // because a refresh hiccuped; fall through to whatever
                    // registration we have, and leave the timestamp alone so
                    // the next request retries.
                    Err(error) => {
                        debug!(table = name, error = %format!("{error:#}"), "table refresh failed");
                    }
                }
            }
        }
        // Unreachable while every caller passes a `lake_tables()` name, but a
        // missing entry shouldn't silently serve an unrefreshed table.
        None => warn!(table = name, "no refresh entry for table"),
    }
    Ok(state.ctx.table(name).await?)
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
    State(state): State<SharedState>,
    Query(p): Query<Params>,
) -> Result<Json<serde_json::Value>, AppError> {
    let table = fresh_table(&state, TRADES, topics::TRADES).await?;
    let df = with_symbol(table, &p.symbol)?
        .sort(vec![col("trade_time").sort(false, false)])?
        .limit(0, Some(clamp_limit(p.limit)))?;
    Ok(Json(batches_to_json(&df.collect().await?)?))
}

async fn book_tickers(
    State(state): State<SharedState>,
    Query(p): Query<Params>,
) -> Result<Json<serde_json::Value>, AppError> {
    let table = fresh_table(&state, BOOK_TICKERS, topics::BOOK_TICKERS).await?;
    let df = with_symbol(table, &p.symbol)?
        .sort(vec![col("update_id").sort(false, false)])?
        .limit(0, Some(clamp_limit(p.limit)))?;
    Ok(Json(batches_to_json(&df.collect().await?)?))
}

/// Candles, densest-first.
///
/// The lake is append-only: the *forming* candle gets a new row every couple of
/// seconds, so a plain `ORDER BY open_time DESC LIMIT n` spends most of its row
/// budget on hundreds of copies of the newest bar (measured: 611 of 1000 rows
/// for one 1d candle). Closed candles have exactly one final row each, so we
/// take those for the history and append the single newest forming row — the
/// row budget then maps ~1:1 onto candles.
async fn klines(
    State(state): State<SharedState>,
    Query(p): Query<Params>,
) -> Result<Json<serde_json::Value>, AppError> {
    let table = fresh_table(&state, KLINES, topics::KLINES).await?;

    let scoped = |df: DataFrame| -> Result<DataFrame, AppError> {
        let df = with_symbol(df, &p.symbol)?;
        match &p.interval {
            Some(interval) => Ok(df.filter(col("interval").eq(lit(interval.as_str())))?),
            None => Ok(df),
        }
    };

    let closed = scoped(table.clone())?
        .filter(col("is_closed").eq(lit(true)))?
        .sort(vec![col("open_time").sort(false, false)])?
        .limit(0, Some(clamp_limit(p.limit)))?;
    let mut batches = closed.collect().await?;

    // The still-forming candle: newest open_time, newest update of it.
    let forming = scoped(table)?
        .filter(col("is_closed").eq(lit(false)))?
        .sort(vec![
            col("open_time").sort(false, false),
            col("kafka_offset").sort(false, false),
        ])?
        .limit(0, Some(1))?;
    batches.extend(forming.collect().await?);

    Ok(Json(batches_to_json(&batches)?))
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
    fn a_listing_that_never_registered_is_always_stale() {
        assert!(is_stale(None, Duration::from_secs(3)));
    }

    #[test]
    fn a_listing_is_stale_only_once_the_ttl_has_elapsed() {
        let now = Instant::now();
        assert!(!is_stale(Some(now), Duration::from_secs(3)));
        // A TTL of zero means "refresh every time", i.e. the old behaviour.
        assert!(is_stale(Some(now), Duration::ZERO));
        assert!(is_stale(
            now.checked_sub(Duration::from_secs(5)),
            Duration::from_secs(3)
        ));
    }

    #[test]
    fn every_lake_table_gets_a_refresh_entry() {
        // `fresh_table` can only refresh tables present in the map, so the two
        // must not drift apart as tables are added.
        let refreshed: HashMap<_, _> = lake_tables()
            .into_iter()
            .map(|(name, _)| (name, Mutex::new(None::<Instant>)))
            .collect();
        assert_eq!(refreshed.len(), lake_tables().len());
        for (name, _) in lake_tables() {
            assert!(refreshed.contains_key(name), "no refresh entry for {name}");
        }
    }

    #[test]
    fn empty_batches_serialize_to_empty_array() {
        let json = batches_to_json(&[]).expect("empty serializes");
        assert_eq!(json, serde_json::json!([]));
    }
}
