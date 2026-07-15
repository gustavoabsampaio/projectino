//! Typed hot-consumer configuration, loaded once at startup.

use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_brokers: String,
    pub group_id: String,
    /// Topics to subscribe to — the three market event topics.
    pub topics: Vec<String>,
    /// SpacetimeDB websocket URI (e.g. `ws://localhost:3000`).
    pub stdb_ws_uri: String,
    /// Published module/database name.
    pub stdb_db_name: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            kafka_brokers: common::config::required("KAFKA_BROKERS")?,
            group_id: common::config::optional("KAFKA_GROUP_HOT", "hot-consumer-group"),
            topics: vec![
                common::topics::TRADES.to_string(),
                common::topics::BOOK_TICKERS.to_string(),
                common::topics::KLINES.to_string(),
            ],
            stdb_ws_uri: common::config::optional("STDB_WS_URI", "ws://localhost:3000"),
            stdb_db_name: common::config::optional("STDB_DB_NAME", "projectino"),
        })
    }
}
