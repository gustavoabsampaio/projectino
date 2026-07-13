//! Binance websocket → Redpanda ingestor.
//!
//! One task, two nested loops: an outer reconnect loop (jittered exponential
//! backoff, Ctrl-C aware) around an inner read loop that decodes each frame
//! and produces it, enveloped, to the per-event-type topic keyed by symbol.
//! The Kafka producer needs no separate task: `FutureProducer::send` enqueues
//! into librdkafka's buffer and actual network I/O happens on its own threads.
//!
//! Frame decoding/routing lives in [`handler`], shared with the replay
//! harness (`src/bin/replay.rs`) so tests exercise the real handling path.

pub mod backoff;
pub mod config;
pub mod decode;
pub mod handler;

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, Producer};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, warn};

use crate::backoff::Backoff;
use crate::config::Config;
use crate::handler::{FrameAction, Sink, Stats, handle_frame};

/// A connection that survives this long is considered healthy: the next
/// failure restarts backoff from the base delay instead of escalating.
const HEALTHY_CONNECTION: Duration = Duration::from_secs(60);

/// Optional raw-frame recorder. When `INGESTOR_DUMP_RAW=<path>` is set, every
/// raw text frame is appended (newline-delimited) to that file — this is how
/// replay fixtures are recorded (see the `replay-testing-harness` skill).
type RawDump = BufWriter<File>;

pub async fn run(cfg: Config) -> Result<()> {
    let producer = connect_producer(&cfg)?;
    let mut dump = open_dump().await?;

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut backoff = Backoff::new(Duration::from_secs(1), Duration::from_secs(60));
    let mut reconnects: u64 = 0;
    let mut stats = Stats::default();

    loop {
        let started = Instant::now();
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received");
                break;
            }
            result = stream_once(&cfg, &producer, dump.as_mut(), &mut stats) => {
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

    if let Some(mut writer) = dump {
        writer.flush().await.context("flushing raw dump")?;
    }

    // Drain librdkafka's in-memory buffer before exiting so acknowledged-to-us
    // events aren't dropped. Blocking is acceptable at shutdown.
    info!(%stats, "flushing producer");
    producer
        .flush(Duration::from_secs(10))
        .context("flushing Kafka producer during shutdown")?;
    info!(%stats, "ingestor stopped");
    Ok(())
}

async fn open_dump() -> Result<Option<RawDump>> {
    let Some(path) = std::env::var_os("INGESTOR_DUMP_RAW") else {
        return Ok(None);
    };
    let file = File::create(&path)
        .await
        .with_context(|| format!("creating raw dump file {path:?}"))?;
    warn!(path = ?path, "recording raw frames — dev/fixture capture mode");
    Ok(Some(BufWriter::new(file)))
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
async fn stream_once(
    cfg: &Config,
    producer: &FutureProducer,
    mut dump: Option<&mut RawDump>,
    stats: &mut Stats,
) -> Result<()> {
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

    let sink = Sink::Kafka(producer);
    while let Some(frame) = ws.next().await {
        match frame.context("websocket protocol error")? {
            Message::Text(text) => {
                if let Some(writer) = dump.as_deref_mut() {
                    writer.write_all(text.as_bytes()).await?;
                    writer.write_all(b"\n").await?;
                }
                match handle_frame(text.as_str(), &sink, stats).await? {
                    FrameAction::Continue => {}
                    FrameAction::ServerShutdown => {
                        warn!("server announced shutdown; reconnecting");
                        return Ok(());
                    }
                }
            }
            // tokio-tungstenite auto-queues a pong (copying the ping payload,
            // as Binance requires) while the stream is being polled.
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(reason) => {
                info!(?reason, "server closed the connection");
                return Ok(());
            }
            Message::Binary(payload) => {
                warn!(len = payload.len(), "ignoring unexpected binary frame");
            }
            Message::Frame(_) => {}
        }
    }
    bail!("websocket stream ended without a close frame")
}
