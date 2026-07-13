//! Binance websocket → Redpanda ingestor.
//!
//! One task, two nested loops: an outer reconnect loop (jittered exponential
//! backoff, Ctrl-C aware) around an inner read loop that decodes each frame
//! and produces it, enveloped, to the per-event-type topic keyed by symbol.
//! The Kafka producer needs no separate task: `FutureProducer::send` enqueues
//! into librdkafka's buffer and actual network I/O happens on its own threads.

pub mod backoff;
pub mod config;
pub mod decode;

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::backoff::Backoff;
use crate::config::Config;
use crate::decode::{Decoded, MarketEvent};

/// A connection that survives this long is considered healthy: the next
/// failure restarts backoff from the base delay instead of escalating.
const HEALTHY_CONNECTION: Duration = Duration::from_secs(60);

pub async fn run(cfg: Config) -> Result<()> {
    let producer = connect_producer(&cfg)?;

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));
    let mut reconnects: u64 = 0;

    loop {
        let started = Instant::now();
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received");
                break;
            }
            result = stream_once(&cfg, &producer) => {
                if started.elapsed() >= HEALTHY_CONNECTION {
                    backoff.reset();
                }
                match result {
                    Ok(()) => info!(reconnects, "connection ended; will reconnect"),
                    Err(error) => warn!(error = %format!("{error:#}"), reconnects, "connection failed; will reconnect"),
                }
            }
        }

        reconnects += 1;
        let delay = backoff.next_delay();
        info!(
            delay_ms = delay.as_millis() as u64,
            reconnects, "backing off before reconnect"
        );
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received during backoff");
                break;
            }
            _ = tokio::time::sleep(delay) => {}
        }
    }

    // Drain librdkafka's in-memory buffer before exiting so acknowledged-to-us
    // events aren't dropped. Blocking is acceptable at shutdown.
    info!("flushing producer");
    producer
        .flush(Duration::from_secs(10))
        .context("flushing Kafka producer during shutdown")?;
    Ok(())
}

fn connect_producer(cfg: &Config) -> Result<FutureProducer> {
    // acks=all + idempotence per the producer conventions: no duplicated or
    // reordered messages from producer-side retries.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "10000")
        .create()
        .context("failed to create Kafka producer")?;

    let metadata = producer
        .client()
        .fetch_metadata(None, Duration::from_secs(10))
        .context("failed to reach Redpanda — is `docker compose up` running?")?;
    info!(
        broker_count = metadata.brokers().len(),
        "connected to Redpanda"
    );
    Ok(producer)
}

/// One connection lifetime: connect (the combined-stream URL carries the
/// subscriptions), then read frames until the server closes or errors.
async fn stream_once(cfg: &Config, producer: &FutureProducer) -> Result<()> {
    let url = cfg.combined_stream_url();
    let (mut ws, response) = connect_async(url.as_str())
        .await
        .context("failed to connect to the Binance websocket endpoint")?;
    info!(
        status = %response.status(),
        streams = cfg.stream_names().len(),
        endpoint = %cfg.ws_base,
        "connected to Binance combined stream"
    );

    let mut published: u64 = 0;
    while let Some(frame) = ws.next().await {
        match frame.context("websocket protocol error")? {
            Message::Text(text) => match decode::decode_combined_frame(text.as_str()) {
                Ok(Decoded::Market(event)) => {
                    publish(producer, &event).await?;
                    published += 1;
                }
                Ok(Decoded::ServerShutdown) => {
                    warn!(published, "server announced shutdown; reconnecting");
                    return Ok(());
                }
                // One undecodable frame must not tear down the connection.
                Err(error) => warn!(
                    error = %error,
                    frame = %truncate(text.as_str(), 256),
                    "dropping undecodable frame"
                ),
            },
            // tokio-tungstenite auto-queues a pong (copying the ping payload,
            // as Binance requires) while the stream is being polled.
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(reason) => {
                info!(?reason, published, "server closed the connection");
                return Ok(());
            }
            Message::Binary(payload) => {
                warn!(len = payload.len(), "ignoring unexpected binary frame");
            }
            Message::Frame(_) => {}
        }
    }
    bail!("websocket stream ended without a close frame ({published} events published)")
}

async fn publish(producer: &FutureProducer, event: &MarketEvent) -> Result<()> {
    let payload = event
        .to_envelope_json()
        .context("serializing event envelope")?;
    let record = FutureRecord::to(event.topic())
        .key(event.key())
        .payload(&payload);

    // Awaiting the delivery future applies natural backpressure: the read
    // loop can't outrun the broker. Revisit (pipelined sends) if throughput
    // ever demands it.
    let delivery = producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(error, _msg)| anyhow!(error))
        .with_context(|| format!("producing to {}", event.topic()))?;
    debug!(
        topic = event.topic(),
        key = event.key(),
        partition = delivery.partition,
        offset = delivery.offset,
        "event published"
    );
    Ok(())
}

/// Char-boundary-safe truncation for logging payload snippets.
fn truncate(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::truncate;

    #[test]
    fn truncate_respects_char_boundaries() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello", 3), "hel");
        // '€' is 3 bytes; cutting mid-char must back up, not panic.
        assert_eq!(truncate("€€", 4), "€");
    }
}
