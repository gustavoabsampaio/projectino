//! Typed api configuration, loaded once at startup.

use std::time::Duration;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub listen_addr: String,
    pub minio_endpoint: String,
    pub minio_access_key: String,
    pub minio_secret_key: String,
    pub minio_region: String,
    pub bucket: String,
    /// Browser origin allowed to call this api (the Vite dev server by default).
    pub cors_origin: String,
    /// How long a table's file listing stays usable before the next query
    /// re-lists the lake prefix. Bounds how stale a query's view of the lake
    /// can be, and caps re-listing to once per TTL however fast clients poll.
    pub listing_ttl: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            listen_addr: common::config::optional("API_LISTEN_ADDR", "127.0.0.1:8081"),
            minio_endpoint: common::config::required("MINIO_ENDPOINT")?,
            minio_access_key: common::config::required("MINIO_ACCESS_KEY")?,
            minio_secret_key: common::config::required("MINIO_SECRET_KEY")?,
            minio_region: common::config::optional("MINIO_REGION", "us-east-1"),
            bucket: common::config::required("LAKE_BUCKET")?,
            cors_origin: common::config::optional("API_CORS_ORIGIN", "http://localhost:5173"),
            listing_ttl: Duration::from_millis(
                common::config::optional("API_LISTING_TTL_MS", "3000")
                    .parse()
                    .context("API_LISTING_TTL_MS must be a whole number of milliseconds")?,
            ),
        })
    }
}
