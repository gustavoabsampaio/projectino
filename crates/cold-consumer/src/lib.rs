//! Cold-path consumer: `market.*` topics → batched Parquet files on the MinIO
//! lake, for historical/analytical queries (served later by the `api` crate
//! via DataFusion).
//!
//! Batching + idempotency: events are buffered and flushed on a size or time
//! trigger. On flush, rows are grouped by Kafka partition and written one
//! Parquet file per (topic, partition), named deterministically by the offset
//! range it covers. Offsets are committed **only after** the upload succeeds
//! (never before). A crash between write and commit replays the same offsets,
//! re-deriving the same filenames — an overwrite, not duplicate rows.
//!
//! Undecodable messages are routed to the topic's `.dlq` sibling rather than
//! dropped (see [`dead_letter`]). Unlike the hot path there is no transient
//! failure class here: a decode failure is a permanent poison pill, and the
//! only other failure — a Parquet flush — is already handled by not committing
//! the batch, so those messages simply replay. Because offsets are committed in
//! batches (`max+1` per partition), a poison pill is dead-lettered *at decode
//! time*, awaiting delivery before a later flush can commit past it.
//!
//! Batch boundaries can shift across restarts, so overlapping files are
//! theoretically possible — acceptable for this stage.

pub mod config;
pub mod lake;

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use object_store::ObjectStoreExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectPath;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::Offset;
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use serde::Deserialize;
use tracing::{info, warn};

use crate::config::Config;
use crate::lake::Row;
use common::events::{AggTrade, BookTicker, KlineEvent};
use common::topics;

/// Per-event-type row buffers accumulated between flushes.
#[derive(Default)]
struct Buffers {
    trades: Vec<Row<AggTrade>>,
    tickers: Vec<Row<BookTicker>>,
    klines: Vec<Row<KlineEvent>>,
}

impl Buffers {
    fn len(&self) -> usize {
        self.trades.len() + self.tickers.len() + self.klines.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn clear(&mut self) {
        self.trades.clear();
        self.tickers.clear();
        self.klines.clear();
    }
}

pub async fn run(cfg: Config) -> Result<()> {
    let consumer = build_consumer(&cfg)?;
    let producer = connect_producer(&cfg)?;
    let store = build_store(&cfg)?;
    info!(
        bucket = %cfg.bucket,
        topics = ?cfg.topics,
        group = %cfg.group_id,
        batch_max_rows = cfg.batch_max_rows,
        "cold-consumer started"
    );

    let mut buffers = Buffers::default();
    let mut ticker = tokio::time::interval(cfg.flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received; flushing");
                flush(&store, &consumer, &mut buffers).await?;
                break;
            }
            _ = ticker.tick() => {
                flush(&store, &consumer, &mut buffers).await?;
            }
            received = consumer.recv() => {
                let msg = received.context("Kafka receive error")?;
                buffer_message(&producer, &msg, &mut buffers).await?;
                if buffers.len() >= cfg.batch_max_rows {
                    flush(&store, &consumer, &mut buffers).await?;
                }
            }
        }
    }
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

fn build_consumer(cfg: &Config) -> Result<StreamConsumer> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_brokers)
        .set("group.id", &cfg.group_id)
        // Manual commit: only after a Parquet flush succeeds.
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .context("failed to create Kafka consumer")?;
    let topics: Vec<&str> = cfg.topics.iter().map(String::as_str).collect();
    consumer
        .subscribe(&topics)
        .context("failed to subscribe to market topics")?;
    Ok(consumer)
}

fn build_store(cfg: &Config) -> Result<AmazonS3> {
    // `allow_http` is required because local MinIO serves plain HTTP.
    AmazonS3Builder::new()
        .with_endpoint(&cfg.minio_endpoint)
        .with_bucket_name(&cfg.bucket)
        .with_access_key_id(&cfg.minio_access_key)
        .with_secret_access_key(&cfg.minio_secret_key)
        .with_region(&cfg.minio_region)
        .with_allow_http(true)
        .build()
        .context("failed to build MinIO (S3) client")
}

/// Decode one message and append it to the matching buffer. An undecodable
/// message is a permanent poison pill: it is routed to the topic's `.dlq`
/// sibling (awaiting delivery) rather than dropped. Returns `Err` only if the
/// dead-letter produce itself fails, so the consumer stops with the offset
/// uncommitted rather than losing the message.
async fn buffer_message(
    producer: &FutureProducer,
    msg: &BorrowedMessage<'_>,
    buffers: &mut Buffers,
) -> Result<()> {
    let partition = msg.partition();
    let offset = msg.offset();
    let Some(bytes) = msg.payload() else {
        warn!(partition, offset, "empty Kafka payload; skipping");
        return Ok(());
    };
    match decode(bytes) {
        Ok(Decoded::Trade(event)) => buffers.trades.push(Row {
            partition,
            offset,
            event,
        }),
        Ok(Decoded::BookTicker(event)) => buffers.tickers.push(Row {
            partition,
            offset,
            event,
        }),
        Ok(Decoded::Kline(event)) => buffers.klines.push(Row {
            partition,
            offset,
            event,
        }),
        Err(error) => {
            warn!(
                partition,
                offset,
                error = %format!("{error:#}"),
                "undecodable message → dead-letter topic"
            );
            dead_letter(producer, msg, bytes, &error).await?;
        }
    }
    Ok(())
}

/// Route a poison-pill message to its `.dlq` sibling topic, awaiting delivery so
/// the message is durably parked before a later flush commits the source offset
/// past it. The original bytes are preserved verbatim as the payload; the
/// failure reason and source coordinates ride as headers for inspection in the
/// Redpanda console (per the kafka-schema-conventions skill).
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

    // Preserve the symbol key so per-symbol ordering carries into the DLQ.
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

/// Flush all buffers: write one Parquet file per (topic, partition), then
/// commit the covered offsets. No-op when empty.
async fn flush(store: &AmazonS3, consumer: &StreamConsumer, buffers: &mut Buffers) -> Result<()> {
    if buffers.is_empty() {
        return Ok(());
    }
    let mut commit: BTreeMap<(String, i32), i64> = BTreeMap::new();
    let mut total = 0;
    total += write_partitioned(
        store,
        topics::TRADES,
        &buffers.trades,
        lake::build_trades,
        &mut commit,
    )
    .await?;
    total += write_partitioned(
        store,
        topics::BOOK_TICKERS,
        &buffers.tickers,
        lake::build_book_tickers,
        &mut commit,
    )
    .await?;
    total += write_partitioned(
        store,
        topics::KLINES,
        &buffers.klines,
        lake::build_klines,
        &mut commit,
    )
    .await?;

    // Commit only after every upload succeeded.
    if !commit.is_empty() {
        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), max_offset) in &commit {
            tpl.add_partition_offset(topic, *partition, Offset::Offset(max_offset + 1))
                .context("building commit offset list")?;
        }
        consumer
            .commit(&tpl, CommitMode::Sync)
            .context("committing offsets after flush")?;
    }

    info!(rows = total, "flushed batch to lake and committed offsets");
    buffers.clear();
    Ok(())
}

/// Group a buffer by partition and write one Parquet file per partition,
/// recording the max committed offset per (topic, partition).
async fn write_partitioned<T>(
    store: &AmazonS3,
    topic: &str,
    rows: &[Row<T>],
    build: impl Fn(&[&Row<T>]) -> Result<arrow::array::RecordBatch>,
    commit: &mut BTreeMap<(String, i32), i64>,
) -> Result<usize> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut by_partition: BTreeMap<i32, Vec<&Row<T>>> = BTreeMap::new();
    for row in rows {
        by_partition.entry(row.partition).or_default().push(row);
    }

    let mut written = 0;
    for (partition, group) in by_partition {
        let min = group.iter().map(|r| r.offset).min().unwrap_or(0);
        let max = group.iter().map(|r| r.offset).max().unwrap_or(0);
        let batch = build(&group)?;
        let bytes = lake::to_parquet(&batch)?;
        // Deterministic, zero-padded so lexical order == offset order.
        let path = format!("{topic}/partition={partition}/off-{min:020}-{max:020}.parquet");
        store
            .put(&ObjectPath::from(path.as_str()), bytes.into())
            .await
            .with_context(|| format!("uploading {path}"))?;
        commit
            .entry((topic.to_string(), partition))
            .and_modify(|o| *o = (*o).max(max))
            .or_insert(max);
        written += group.len();
        info!(topic, partition, rows = group.len(), path, "wrote parquet");
    }
    Ok(written)
}

enum Decoded {
    Trade(AggTrade),
    BookTicker(BookTicker),
    Kline(KlineEvent),
}

#[derive(Deserialize)]
struct RawEnvelope<'a> {
    event_type: String,
    #[serde(borrow)]
    payload: &'a serde_json::value::RawValue,
}

fn decode(bytes: &[u8]) -> Result<Decoded> {
    let env: RawEnvelope = serde_json::from_slice(bytes).context("decoding envelope")?;
    let payload = env.payload.get();
    match env.event_type.as_str() {
        "agg_trade" => Ok(Decoded::Trade(
            serde_json::from_str(payload).context("decoding aggTrade payload")?,
        )),
        "book_ticker" => Ok(Decoded::BookTicker(
            serde_json::from_str(payload).context("decoding bookTicker payload")?,
        )),
        "kline" => Ok(Decoded::Kline(
            serde_json::from_str(payload).context("decoding kline payload")?,
        )),
        other => bail!("unknown event_type `{other}`"),
    }
}
