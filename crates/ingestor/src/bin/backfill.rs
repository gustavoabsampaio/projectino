//! Backfill historical klines from Binance's REST API into `market.klines`.
//!
//! The live websocket only ever sends the *current* candle, so a 1h/1d chart
//! would show a single in-progress bar until the pipeline had run for days.
//! This pulls real history (up to 1000 candles per symbol × interval) and
//! produces it to the same topic in the same envelope, so it flows through the
//! existing cold-consumer into the Parquet lake — the log is the spine, and a
//! backfill is just another producer.
//!
//! Endpoint verified 2026-07-20 (see the `binance-api-reference` skill,
//! `references/rest-klines.md`): `GET /api/v3/klines`, weight 2, limit ≤ 1000,
//! responding with an array of *positional arrays*, not objects.
//!
//! Run: `cargo run -p ingestor --bin backfill`
//! Config: SYMBOLS, KLINE_INTERVALS, KAFKA_BROKERS, BACKFILL_LIMIT,
//! BINANCE_REST_BASE.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use common::envelope::Envelope;
use common::events::{Kline, KlineEvent};
use common::topics;
use ingestor::config::Config;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rust_decimal::Decimal;
use serde_json::Value;
use tracing::{info, warn};

/// Binance caps `limit` at 1000 for this endpoint.
const MAX_LIMIT: usize = 1000;
/// Small pause between REST calls — each is weight 2, so this is politeness
/// rather than necessity.
const REQUEST_SPACING: Duration = Duration::from_millis(120);

#[tokio::main]
async fn main() -> Result<()> {
    // Must run before any TLS connection (reqwest's rustls needs a provider).
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("installing the rustls ring crypto provider should only be attempted once");

    common::config::load_dotenv();
    common::config::init_tracing();

    let cfg = Config::from_env()?;
    let rest_base = common::config::optional(
        "BINANCE_REST_BASE",
        // Market-data-only host, same rationale as the websocket endpoint.
        "https://data-api.binance.vision",
    );
    let limit: usize = common::config::optional("BACKFILL_LIMIT", "1000")
        .parse()
        .context("BACKFILL_LIMIT must be a positive integer")?;
    if limit == 0 || limit > MAX_LIMIT {
        bail!("BACKFILL_LIMIT must be between 1 and {MAX_LIMIT}");
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .context("building HTTP client")?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "30000")
        .create()
        .context("failed to create Kafka producer")?;

    info!(
        symbols = cfg.symbols.len(),
        intervals = cfg.kline_intervals.len(),
        limit,
        base = %rest_base,
        "backfill starting"
    );

    let now_ms = now_millis();
    let mut produced = 0u64;

    for symbol in &cfg.symbols {
        let exchange_symbol = symbol.as_exchange_symbol();
        for interval in &cfg.kline_intervals {
            match fetch_klines(&client, &rest_base, exchange_symbol, interval, limit).await {
                Ok(rows) => {
                    let count = enqueue(&producer, exchange_symbol, interval, &rows, now_ms)?;
                    produced += count;
                    info!(
                        symbol = exchange_symbol,
                        interval,
                        candles = count,
                        "backfilled"
                    );
                }
                // One bad interval shouldn't abort the whole backfill.
                Err(error) => warn!(
                    symbol = exchange_symbol,
                    interval,
                    error = %format!("{error:#}"),
                    "skipping interval"
                ),
            }
            tokio::time::sleep(REQUEST_SPACING).await;
        }
    }

    // Messages were queued with `send_result`; flush waits for delivery.
    info!(produced, "flushing producer");
    producer
        .flush(Duration::from_secs(60))
        .context("flushing backfilled klines")?;
    info!(produced, "backfill complete");
    Ok(())
}

/// GET /api/v3/klines → the raw positional rows.
async fn fetch_klines(
    client: &reqwest::Client,
    base: &str,
    symbol: &str,
    interval: &str,
    limit: usize,
) -> Result<Vec<Vec<Value>>> {
    let url = format!(
        "{}/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}",
        base.trim_end_matches('/')
    );
    let response = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("requesting {url}"))?
        .error_for_status()
        .with_context(|| format!("error status from {url}"))?;
    let body = response.text().await.context("reading response body")?;
    serde_json::from_str(&body).context("decoding klines response")
}

/// Convert rows to enveloped events and queue them on the producer.
fn enqueue(
    producer: &FutureProducer,
    symbol: &str,
    interval: &str,
    rows: &[Vec<Value>],
    now_ms: i64,
) -> Result<u64> {
    let mut queued = 0;
    for row in rows {
        let kline = parse_row(row, symbol, interval, now_ms)
            .with_context(|| format!("parsing a {symbol} {interval} kline"))?;
        let event = KlineEvent {
            event_type: "kline".to_string(),
            event_time: now_ms,
            symbol: symbol.to_string(),
            kline,
        };
        let payload =
            serde_json::to_vec(&Envelope::new("kline", &event)).context("serializing envelope")?;

        let record = FutureRecord::to(topics::KLINES)
            .key(symbol)
            .payload(&payload);
        // `send_result` queues without awaiting each delivery — the final
        // `flush` is what confirms them, which matters for bulk loads.
        if let Err((error, _)) = producer.send_result(record) {
            bail!("queueing a {symbol} {interval} kline failed: {error}");
        }
        queued += 1;
    }
    Ok(queued)
}

/// Map one positional REST row onto the same `Kline` the websocket produces.
fn parse_row(row: &[Value], symbol: &str, interval: &str, now_ms: i64) -> Result<Kline> {
    let int_at = |i: usize| -> Result<i64> {
        row.get(i)
            .and_then(Value::as_i64)
            .with_context(|| format!("field {i} is not an integer"))
    };
    let dec_at = |i: usize| -> Result<Decimal> {
        row.get(i)
            .and_then(Value::as_str)
            .with_context(|| format!("field {i} is not a string"))?
            .parse::<Decimal>()
            .with_context(|| format!("field {i} is not a decimal"))
    };

    let close_time = int_at(6)?;
    Ok(Kline {
        open_time: int_at(0)?,
        close_time,
        symbol: symbol.to_string(),
        interval: interval.to_string(),
        // REST klines carry no trade-id range; -1 marks "not provided".
        first_trade_id: -1,
        last_trade_id: -1,
        open: dec_at(1)?,
        high: dec_at(2)?,
        low: dec_at(3)?,
        close: dec_at(4)?,
        volume: dec_at(5)?,
        quote_volume: dec_at(7)?,
        trade_count: int_at(8)?,
        // The final row is usually the still-forming candle.
        is_closed: close_time < now_ms,
        taker_buy_base_volume: dec_at(9)?,
        taker_buy_quote_volume: dec_at(10)?,
    })
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact example row from the Binance REST docs.
    fn docs_row() -> Vec<Value> {
        serde_json::from_str(
            r#"[1499040000000,"0.01634790","0.80000000","0.01575800","0.01577100",
                "148976.11427815",1499644799999,"2434.19055334",308,"1756.87402397",
                "28.46694368","0"]"#,
        )
        .expect("docs row parses")
    }

    #[test]
    fn maps_positional_rest_row_onto_kline() {
        let k = parse_row(&docs_row(), "BTCUSDT", "1h", 1_500_000_000_000).expect("parses");
        assert_eq!(k.open_time, 1_499_040_000_000);
        assert_eq!(k.close_time, 1_499_644_799_999);
        assert_eq!(k.open, "0.01634790".parse::<Decimal>().unwrap());
        assert_eq!(k.high, "0.80000000".parse::<Decimal>().unwrap());
        assert_eq!(k.low, "0.01575800".parse::<Decimal>().unwrap());
        assert_eq!(k.close, "0.01577100".parse::<Decimal>().unwrap());
        assert_eq!(k.quote_volume, "2434.19055334".parse::<Decimal>().unwrap());
        assert_eq!(k.trade_count, 308);
        assert_eq!(k.interval, "1h");
        // close_time is in the past relative to `now`, so the candle is closed.
        assert!(k.is_closed);
    }

    #[test]
    fn marks_the_still_forming_candle_as_open() {
        // `now` before close_time → the candle has not closed yet.
        let k = parse_row(&docs_row(), "BTCUSDT", "1h", 1_499_000_000_000).expect("parses");
        assert!(!k.is_closed);
    }

    #[test]
    fn rejects_a_malformed_row() {
        let row: Vec<Value> = serde_json::from_str(r#"[1,"a"]"#).unwrap();
        assert!(parse_row(&row, "BTCUSDT", "1h", 0).is_err());
    }
}
