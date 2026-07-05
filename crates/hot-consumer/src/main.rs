//! Hot-path consumer skeleton: proves connectivity to Redpanda (Kafka
//! consumer) and reachability of SpacetimeDB, then exits. No business logic.

use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    common::config::load_dotenv();
    common::config::init_tracing();

    let brokers = common::config::required("KAFKA_BROKERS")?;
    let topic = common::config::optional("KAFKA_TOPIC", "market.events.raw");
    let group = common::config::optional("KAFKA_GROUP_HOT", "hot-consumer");
    let stdb_http = common::config::required("STDB_HTTP_URI")?;

    info!(%brokers, %topic, %group, "hot-consumer starting");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group)
        .set("auto.offset.reset", "earliest")
        .create()
        .context("failed to create Kafka consumer")?;
    consumer
        .subscribe(&[topic.as_str()])
        .context("failed to subscribe to topic")?;

    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .context("failed to reach Redpanda — is `docker compose up` running?")?;
    info!(
        broker_count = metadata.brokers().len(),
        "connected to Redpanda and subscribed"
    );

    // Plain TCP reachability check for SpacetimeDB. Real reducer calls go
    // through the `spacetimedb-sdk` client once bindings are generated:
    //   spacetime generate --lang rust --out-dir crates/hot-consumer/src/module_bindings \
    //     --project-path crates/spacetime-module
    // TODO: replace this check with a real SDK connection + reducer calls.
    let stdb_addr = stdb_http
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/');
    tokio::net::TcpStream::connect(stdb_addr)
        .await
        .with_context(|| format!("failed to reach SpacetimeDB at {stdb_addr}"))?;
    info!(addr = %stdb_addr, "SpacetimeDB is reachable");

    // TODO: consume events from Kafka and forward them to SpacetimeDB reducers.
    info!("plumbing verified; skeleton exiting");
    Ok(())
}
