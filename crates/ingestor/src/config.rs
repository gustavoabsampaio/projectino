//! Typed ingestor configuration, loaded once at startup from the environment.

use anyhow::{Context, Result, ensure};
use common::symbol::Symbol;

/// Kline intervals accepted by Binance (verified 2026-07-20; see the
/// `binance-api-reference` skill, `references/websocket-streams.md` and
/// `references/rest-klines.md`). Note there is no `10m`, and a day is `1d`.
pub const VALID_INTERVALS: &[&str] = &[
    "1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w",
    "1M",
];

/// Intervals streamed when `KLINE_INTERVALS` is unset.
///
/// Deliberately a subset of [`VALID_INTERVALS`]. Streaming all 16 opens 32
/// kline streams for two symbols and mostly buys waste: every interval except
/// `1s` re-sends its forming candle every 2s, so a `1M` candle costs the same
/// bandwidth as a `1m` one while changing meaningfully once a month. The real
/// cost isn't the ~5 msg/s — it's that every update appends a row to the
/// Parquet lake, and lake size is what drives `/klines` scan latency.
///
/// `3m`/`2h`/`4h`/`8h`/`3d` are dropped as the least-used rungs; each is
/// adjacent to a neighbour that covers the same use. Anything omitted here is
/// still fetchable on demand via `make backfill`, and can be re-enabled by
/// setting `KLINE_INTERVALS`.
pub const DEFAULT_INTERVALS: &[&str] = &[
    "1s", "1m", "5m", "15m", "30m", "1h", "6h", "12h", "1d", "1w", "1M",
];

/// Binance allows at most this many streams on one connection.
const MAX_STREAMS_PER_CONNECTION: usize = 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub kafka_brokers: String,
    /// Websocket base endpoint, no trailing slash and no path.
    pub ws_base: String,
    pub symbols: Vec<Symbol>,
    /// One kline stream is opened per symbol × interval.
    pub kline_intervals: Vec<String>,
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

        // `KLINE_INTERVALS` (plural) is the current setting; the older singular
        // `KLINE_INTERVAL` still works so existing .env files keep running.
        let default_intervals = DEFAULT_INTERVALS.join(",");
        let raw = match common::config::optional("KLINE_INTERVALS", "").as_str() {
            "" => common::config::optional("KLINE_INTERVAL", &default_intervals),
            list => list.to_string(),
        };
        let kline_intervals = parse_intervals(&raw)?;

        let cfg = Self {
            kafka_brokers: common::config::required("KAFKA_BROKERS")?,
            // data-stream.binance.vision serves market data only — exactly
            // this service's scope (verified against the websocket docs).
            ws_base: common::config::optional(
                "BINANCE_WS_BASE",
                "wss://data-stream.binance.vision",
            ),
            symbols,
            kline_intervals,
        };

        let streams = cfg.stream_names().len();
        ensure!(
            streams <= MAX_STREAMS_PER_CONNECTION,
            "{streams} streams exceeds Binance's limit of {MAX_STREAMS_PER_CONNECTION} per connection — \
             reduce SYMBOLS or KLINE_INTERVALS"
        );
        Ok(cfg)
    }

    /// Stream names to subscribe to: aggTrade + bookTicker per symbol, plus one
    /// kline stream per symbol × interval. Symbols are lowercase per the docs.
    pub fn stream_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for symbol in &self.symbols {
            let s = symbol.as_stream_symbol();
            names.push(format!("{s}@aggTrade"));
            names.push(format!("{s}@bookTicker"));
            for interval in &self.kline_intervals {
                names.push(format!("{s}@kline_{interval}"));
            }
        }
        names
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

/// Parse and validate a comma-separated interval list.
pub fn parse_intervals(raw: &str) -> Result<Vec<String>> {
    let intervals: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    ensure!(
        !intervals.is_empty(),
        "at least one kline interval is required"
    );
    for interval in &intervals {
        ensure!(
            VALID_INTERVALS.contains(&interval.as_str()),
            "`{interval}` is not a Binance kline interval (valid: {})",
            VALID_INTERVALS.join(" ")
        );
    }
    Ok(intervals)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(intervals: &[&str]) -> Config {
        Config {
            kafka_brokers: "localhost:19092".to_string(),
            ws_base: "wss://data-stream.binance.vision".to_string(),
            symbols: vec![Symbol::BtcUsdt],
            kline_intervals: intervals.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn builds_combined_stream_url() {
        assert_eq!(
            test_config(&["1m"]).combined_stream_url(),
            "wss://data-stream.binance.vision/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@kline_1m"
        );
    }

    #[test]
    fn one_kline_stream_per_symbol_and_interval() {
        let mut cfg = test_config(&["1m", "1h", "1d"]);
        cfg.symbols = vec![Symbol::BtcUsdt, Symbol::EthUsdt];
        // 2 symbols × (aggTrade + bookTicker + 3 klines)
        assert_eq!(cfg.stream_names().len(), 2 * (2 + 3));
        assert!(cfg.stream_names().contains(&"ethusdt@kline_1d".to_string()));
    }

    #[test]
    fn rejects_intervals_binance_does_not_have() {
        // The two most tempting mistakes: 10m doesn't exist, and a day is `1d`.
        assert!(parse_intervals("10m").is_err());
        assert!(parse_intervals("24h").is_err());
        assert!(parse_intervals("1m,1d").is_ok());
    }

    #[test]
    fn full_binance_set_parses() {
        assert_eq!(
            parse_intervals(&VALID_INTERVALS.join(",")).unwrap().len(),
            VALID_INTERVALS.len()
        );
    }

    #[test]
    fn default_set_parses() {
        // The default is what runs when KLINE_INTERVALS is unset, so a typo
        // here fails at startup for exactly the users who configured nothing.
        assert_eq!(
            parse_intervals(&DEFAULT_INTERVALS.join(",")).unwrap().len(),
            DEFAULT_INTERVALS.len(),
        );
    }

    #[test]
    fn default_set_is_a_subset_of_the_valid_set() {
        for interval in DEFAULT_INTERVALS {
            assert!(
                VALID_INTERVALS.contains(interval),
                "`{interval}` is in the default set but not a Binance interval"
            );
        }
        assert!(
            DEFAULT_INTERVALS.len() < VALID_INTERVALS.len(),
            "the default is meant to be a trimmed subset, not everything"
        );
    }

    #[test]
    fn default_set_keeps_the_intervals_the_rest_of_the_system_assumes() {
        // `1s` backs the live SpacetimeDB chart and `1m` is the chart default;
        // dropping either from the default would break those out of the box.
        assert!(DEFAULT_INTERVALS.contains(&"1s"));
        assert!(DEFAULT_INTERVALS.contains(&"1m"));
    }
}
