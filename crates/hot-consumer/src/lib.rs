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

pub mod config;
pub mod module_bindings;
mod translate;

use std::sync::mpsc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message};
use spacetimedb_sdk::{DbContext, Identity};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::module_bindings::DbConnection;

pub async fn run(cfg: Config) -> Result<()> {
    let conn = connect_spacetimedb(&cfg)?;

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
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!(applied, "shutdown signal received");
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
                    Err(error) => {
                        // One bad message must not stall the group. Log, commit
                        // past it, and continue. TODO: route to a `.dlq` topic.
                        warn!(error = %format!("{error:#}"), "skipping undecodable/failed message");
                        consumer.commit_message(&msg, CommitMode::Async).ok();
                    }
                }
            }
        }
    }

    conn.disconnect().ok();
    Ok(())
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
