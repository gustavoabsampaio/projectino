//! Ingestor skeleton: proves connectivity to Redpanda (Kafka producer) and to
//! the Binance websocket endpoint, then exits. No business logic yet.

use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, Producer};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    common::config::load_dotenv();
    common::config::init_tracing();

    let brokers = common::config::required("KAFKA_BROKERS")?;
    let topic = common::config::optional("KAFKA_TOPIC", "market.events.raw");
    let ws_url = common::config::required("BINANCE_WS_URL")?;
    let symbols = common::config::optional("SYMBOLS", "btcusdt");

    info!(%brokers, %topic, %symbols, "ingestor starting");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .context("failed to create Kafka producer")?;

    let metadata = producer
        .client()
        .fetch_metadata(None, Duration::from_secs(10))
        .context("failed to reach Redpanda — is `docker compose up` running?")?;
    info!(broker_count = metadata.brokers().len(), "connected to Redpanda");

    // TODO: subscribe to the per-symbol streams (aggTrade / bookTicker / kline).
    // Stream names and the combined-stream path must be verified against
    // Binance's current websocket docs — do not guess them.
    let (_ws_stream, response) = tokio_tungstenite::connect_async(ws_url.as_str())
        .await
        .context("failed to connect to the Binance websocket endpoint")?;
    info!(status = %response.status(), url = %ws_url, "connected to Binance websocket");

    // TODO: read events, decode into `common::events` types, produce to Kafka.
    info!("plumbing verified; skeleton exiting");
    Ok(())
}
