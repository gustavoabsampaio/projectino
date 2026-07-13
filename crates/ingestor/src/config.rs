//! Typed ingestor configuration, loaded once at startup from the environment.

use anyhow::{Context, Result, ensure};
use common::symbol::Symbol;

/// Kline intervals accepted by Binance (verified 2026-07-05; see the
/// `binance-api-reference` skill, `references/websocket-streams.md`).
const KLINE_INTERVALS: &[&str] = &[
    "1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w",
    "1M",
];

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_brokers: String,
    /// Websocket base endpoint, no trailing slash and no path.
    pub ws_base: String,
    pub symbols: Vec<Symbol>,
    pub kline_interval: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let symbols = common::config::optional("SYMBOLS", "btcusdt,ethusdt")
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<Symbol>()
                    .with_context(|| format!("unsupported entry `{s}` in SYMBOLS"))
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(!symbols.is_empty(), "SYMBOLS must name at least one symbol");

        let kline_interval = common::config::optional("KLINE_INTERVAL", "1m");
        ensure!(
            KLINE_INTERVALS.contains(&kline_interval.as_str()),
            "KLINE_INTERVAL `{kline_interval}` is not a Binance kline interval"
        );

        Ok(Self {
            kafka_brokers: common::config::required("KAFKA_BROKERS")?,
            // data-stream.binance.vision serves market data only — exactly
            // this service's scope (verified against the websocket docs).
            ws_base: common::config::optional(
                "BINANCE_WS_BASE",
                "wss://data-stream.binance.vision",
            ),
            symbols,
            kline_interval,
        })
    }

    /// Stream names to subscribe to, one aggTrade + bookTicker + kline per
    /// symbol. Symbols are lowercase in stream names per the docs.
    pub fn stream_names(&self) -> Vec<String> {
        self.symbols
            .iter()
            .flat_map(|sym| {
                let s = sym.as_stream_symbol();
                [
                    format!("{s}@aggTrade"),
                    format!("{s}@bookTicker"),
                    format!("{s}@kline_{}", self.kline_interval),
                ]
            })
            .collect()
    }

    /// Combined-stream URL: `<base>/stream?streams=<a>/<b>/<c>`. Using the
    /// combined form means the subscription rides on the URL — no SUBSCRIBE
    /// control messages, so the 5-messages/second incoming limit is untouched.
    pub fn combined_stream_url(&self) -> String {
        format!(
            "{}/stream?streams={}",
            self.ws_base.trim_end_matches('/'),
            self.stream_names().join("/")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config {
            kafka_brokers: "localhost:19092".to_string(),
            ws_base: "wss://data-stream.binance.vision".to_string(),
            symbols: vec![Symbol::BtcUsdt],
            kline_interval: "1m".to_string(),
        }
    }

    #[test]
    fn builds_combined_stream_url() {
        assert_eq!(
            test_config().combined_stream_url(),
            "wss://data-stream.binance.vision/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@kline_1m"
        );
    }

    #[test]
    fn three_streams_per_symbol() {
        let mut cfg = test_config();
        cfg.symbols = vec![Symbol::BtcUsdt, Symbol::EthUsdt];
        assert_eq!(cfg.stream_names().len(), 6);
    }
}
