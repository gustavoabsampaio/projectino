//! Hot-path consumer: Kafka `market.*` topics → SpacetimeDB reducer calls,
//! driving the live-state tables the frontend subscribes to.
//!
//! Delivery semantics: reducer calls are fire-and-forget over the SDK
//! connection, and Kafka offsets are committed **after** a call is successfully
//! enqueued (never before). Combined with the module's idempotent upserts, a
//! crash replays safely — and because these tables hold *latest* state per
//! symbol, a lost in-flight update is self-healing (the next event overwrites
//! it). A stricter commit-after-apply (awaiting reducer confirmation via the
//! `_then` callbacks, batched to avoid per-message serialization) is a noted
//! follow-up — see the throughput TODO in the README.
//!
//! Failure handling splits on whether a retry can help (see [`translate::
//! DispatchError`]). A message that can't be *decoded* is a permanent poison
//! pill: it is routed to the topic's `.dlq` sibling (awaiting delivery) and
//! then committed past, never silently dropped. A reducer *enqueue* failure is
//! potentially transient (the SDK connection is down), so it is retried with
//! backoff and never committed until it applies — a persistent failure stops
//! the consumer with the offset uncommitted rather than dropping valid data.

pub mod config;
pub mod module_bindings;
mod translate;

use std::sync::mpsc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message};
use spacetimedb_sdk::{DbContext, Identity};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::module_bindings::DbConnection;
use crate::translate::DispatchError;
use common::topics;

/// How many times to retry a transient (enqueue) failure before giving up and
/// stopping the consumer with the offset uncommitted. Rides out a brief blip
/// (SpacetimeDB restart, momentary timeout) without dropping data.
const MAX_APPLY_RETRIES: u32 = 5;

pub async fn run(cfg: Config) -> Result<()> {
    let conn = connect_spacetimedb(&cfg)?;
    let producer = connect_producer(&cfg)?;

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("group.id", &cfg.group_id)
        // Manual commit: we commit only after a reducer call is enqueued.
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("failed to create Kafka consumer")?;
    let topics: Vec<&str> = cfg.topics.iter().map(String::as_str).collect();
    consumer
        .subscribe(&topics)
        .context("failed to subscribe to market topics")?;
    info!(topics = ?cfg.topics, group = %cfg.group_id, "hot-consumer started");

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    let mut applied: u64 = 0;
    let mut dead_lettered: u64 = 0;
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!(applied, dead_lettered, "shutdown signal received");
                break;
            }
            received = consumer.recv() => {
                let msg = received.context("Kafka receive error")?;
                let Some(bytes) = msg.payload() else {
                    warn!("empty Kafka message payload; skipping");
                    continue;
                };
                match translate::dispatch(&conn, bytes) {
                    Ok(kind) => {
                        // commit-after-enqueue (see module docs above).
                        consumer
                            .commit_message(&msg, CommitMode::Async)
                            .context("failed to commit offset")?;
                        applied += 1;
                        debug!(kind, applied, "reducer call enqueued");
                    }
                    Err(error) if error.is_permanent() => {
                        // Poison pill: park it in the DLQ (awaiting delivery),
                        // then commit past it. Never silently dropped.
                        warn!(
                            partition = msg.partition(),
                            offset = msg.offset(),
                            error = %error,
                            "undecodable message → dead-letter topic"
                        );
                        dead_letter(&producer, &msg, bytes, error.source()).await?;
                        consumer
                            .commit_message(&msg, CommitMode::Async)
                            .context("failed to commit offset after dead-lettering")?;
                        dead_lettered += 1;
                    }
                    Err(error) => {
                        // Transient apply failure (e.g. SpacetimeDB unreachable).
                        // Retry with backoff and only commit once it applies —
                        // committing now would drop valid data.
                        retry_apply(&conn, bytes, &msg, error, &mut applied).await?;
                        consumer
                            .commit_message(&msg, CommitMode::Async)
                            .context("failed to commit offset")?;
                    }
                }
            }
        }
    }

    conn.disconnect().ok();
    Ok(())
}

/// Build the DLQ producer. Mirrors the ingestor's producer conventions
/// (`acks=all`, idempotent) so a dead-lettered message is durably parked.
fn connect_producer(cfg: &Config) -> Result<FutureProducer> {
    ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("message.timeout.ms", "10000")
        .create()
        .context("failed to create DLQ Kafka producer")
}

/// Route a poison-pill message to its `.dlq` sibling topic, awaiting delivery so
/// the caller only commits the source offset once the message is durably
/// parked. The original bytes are preserved verbatim as the payload; the
/// failure reason and source coordinates ride along as headers for inspection
/// in the Redpanda console (per the kafka-schema-conventions skill).
async fn dead_letter(
    producer: &FutureProducer,
    msg: &BorrowedMessage<'_>,
    bytes: &[u8],
    error: &anyhow::Error,
) -> Result<()> {
    let source_topic = msg.topic();
    let dlq_topic = topics::dlq(source_topic);
    let partition = msg.partition().to_string();
    let offset = msg.offset().to_string();
    let reason = format!("{error:#}");

    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "dlq.error",
            value: Some(reason.as_str()),
        })
        .insert(Header {
            key: "dlq.source_topic",
            value: Some(source_topic),
        })
        .insert(Header {
            key: "dlq.partition",
            value: Some(partition.as_str()),
        })
        .insert(Header {
            key: "dlq.offset",
            value: Some(offset.as_str()),
        });

    // Preserve the symbol key so per-symbol ordering carries into the DLQ; an
    // empty key (should not happen from the ingestor) is acceptable there.
    let key = msg.key().unwrap_or_default();
    let record = FutureRecord::to(&dlq_topic)
        .key(key)
        .payload(bytes)
        .headers(headers);

    producer
        .send(record, Timeout::After(Duration::from_secs(10)))
        .await
        .map_err(|(error, _)| error)
        .with_context(|| format!("failed to produce to dead-letter topic {dlq_topic}"))?;
    Ok(())
}

/// Retry a transient apply failure with capped exponential backoff. Returns
/// `Ok` once the message applies; returns `Err` — so the caller stops the
/// consumer **without committing** — if it still fails after
/// [`MAX_APPLY_RETRIES`], leaving the offset to replay on the next run rather
/// than dropping valid data.
async fn retry_apply(
    conn: &DbConnection,
    bytes: &[u8],
    msg: &BorrowedMessage<'_>,
    first_error: DispatchError,
    applied: &mut u64,
) -> Result<()> {
    let mut backoff = Duration::from_millis(100);
    let mut last_error = first_error;
    for attempt in 1..=MAX_APPLY_RETRIES {
        warn!(
            partition = msg.partition(),
            offset = msg.offset(),
            attempt,
            error = %last_error,
            "reducer apply failed; retrying (offset not committed)"
        );
        tokio::time::sleep(backoff).await;
        match translate::dispatch(conn, bytes) {
            Ok(kind) => {
                *applied += 1;
                debug!(kind, attempt, "reducer call enqueued after retry");
                return Ok(());
            }
            // A message that decoded once shouldn't turn undecodable, but if it
            // does, fail hard rather than spin.
            Err(error) if error.is_permanent() => {
                bail!("message became undecodable on retry: {error}");
            }
            Err(error) => {
                last_error = error;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    }
    bail!(
        "reducer apply still failing after {MAX_APPLY_RETRIES} retries at partition {} offset {} \
         ({last_error}); stopping without committing so the message replays",
        msg.partition(),
        msg.offset(),
    )
}

/// Build the SDK connection and pump it on a background thread, blocking until
/// the connection is established (or timing out).
fn connect_spacetimedb(cfg: &Config) -> Result<DbConnection> {
    let (tx, rx) = mpsc::channel::<Identity>();
    let conn = DbConnection::builder()
        .with_uri(cfg.stdb_ws_uri.as_str())
        .with_database_name(&cfg.stdb_db_name)
        .on_connect(move |_conn, identity, _token| {
            let _ = tx.send(identity);
        })
        .on_connect_error(|_ctx, error| {
            error!(error = %error, "SpacetimeDB connection error");
        })
        .build()
        .context("failed to build SpacetimeDB connection (is the server up?)")?;

    // Advance the connection continuously on a background thread; reducer calls
    // made from the consumer loop are flushed by this pump.
    conn.run_threaded();

    match rx.recv_timeout(Duration::from_secs(15)) {
        Ok(identity) => {
            info!(
                db = %cfg.stdb_db_name,
                uri = %cfg.stdb_ws_uri,
                identity = %identity.to_hex(),
                "connected to SpacetimeDB"
            );
            Ok(conn)
        }
        Err(_) => bail!(
            "timed out waiting to connect to SpacetimeDB at {} — is it up and the module published?",
            cfg.stdb_ws_uri
        ),
    }
}
