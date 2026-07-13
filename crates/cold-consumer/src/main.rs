//! Cold-path consumer skeleton: proves connectivity to Redpanda (Kafka
//! consumer) and to the MinIO lake bucket, then exits. No business logic.

use std::time::Duration;

use anyhow::{Context, Result};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use rdkafka::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    common::config::load_dotenv();
    common::config::init_tracing();

    let brokers = common::config::required("KAFKA_BROKERS")?;
    let topic = common::config::optional("KAFKA_TOPIC", common::topics::TRADES);
    let group = common::config::optional("KAFKA_GROUP_COLD", "cold-consumer");
    let minio_endpoint = common::config::required("MINIO_ENDPOINT")?;
    let minio_access_key = common::config::required("MINIO_ACCESS_KEY")?;
    let minio_secret_key = common::config::required("MINIO_SECRET_KEY")?;
    let minio_region = common::config::optional("MINIO_REGION", "us-east-1");
    let bucket = common::config::required("LAKE_BUCKET")?;

    info!(%brokers, %topic, %group, %bucket, "cold-consumer starting");

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

    // `allow_http` is required because local MinIO serves plain HTTP.
    let store = AmazonS3Builder::new()
        .with_endpoint(&minio_endpoint)
        .with_bucket_name(&bucket)
        .with_access_key_id(&minio_access_key)
        .with_secret_access_key(&minio_secret_key)
        .with_region(&minio_region)
        .with_allow_http(true)
        .build()
        .context("failed to build MinIO (S3) client")?;

    let listing = store
        .list_with_delimiter(None)
        .await
        .context("failed to list the lake bucket — is MinIO up and the bucket created?")?;
    info!(
        objects = listing.objects.len(),
        prefixes = listing.common_prefixes.len(),
        "connected to MinIO; lake bucket is reachable"
    );

    // TODO: consume events from Kafka, batch them, and write Parquet files to
    // the lake (arrow/parquet writers are pinned in [workspace.dependencies]).
    info!("plumbing verified; skeleton exiting");
    Ok(())
}
