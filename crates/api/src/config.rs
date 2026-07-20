//! Typed api configuration, loaded once at startup.

use anyhow::Result;

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
        })
    }
}
