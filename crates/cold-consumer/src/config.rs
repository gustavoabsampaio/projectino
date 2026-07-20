//! Typed cold-consumer configuration, loaded once at startup.

use std::time::Duration;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_brokers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub minio_endpoint: String,
    pub minio_access_key: String,
    pub minio_secret_key: String,
    pub minio_region: String,
    pub bucket: String,
    /// Flush when the buffer reaches this many rows (across all event types).
    pub batch_max_rows: usize,
    /// Flush at least this often, even if the buffer is below `batch_max_rows`.
    pub flush_interval: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let batch_max_rows = common::config::optional("COLD_BATCH_MAX_ROWS", "500")
            .parse()
            .context("COLD_BATCH_MAX_ROWS must be a positive integer")?;
        let flush_secs = common::config::optional("COLD_FLUSH_SECS", "10")
            .parse()
            .context("COLD_FLUSH_SECS must be a positive integer")?;

        Ok(Self {
            kafka_brokers: common::config::required("KAFKA_BROKERS")?,
            group_id: common::config::optional("KAFKA_GROUP_COLD", "cold-consumer-group"),
            topics: vec![
                common::topics::TRADES.to_string(),
                common::topics::BOOK_TICKERS.to_string(),
                common::topics::KLINES.to_string(),
            ],
            minio_endpoint: common::config::required("MINIO_ENDPOINT")?,
            minio_access_key: common::config::required("MINIO_ACCESS_KEY")?,
            minio_secret_key: common::config::required("MINIO_SECRET_KEY")?,
            minio_region: common::config::optional("MINIO_REGION", "us-east-1"),
            bucket: common::config::required("LAKE_BUCKET")?,
            batch_max_rows,
            flush_interval: Duration::from_secs(flush_secs),
        })
    }
}
