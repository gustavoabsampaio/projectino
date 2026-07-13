//! Serde models for the Binance websocket payloads this pipeline consumes.
//!
//! Prices and quantities arrive as JSON *strings* and are decoded into
//! `Decimal` (never `f64`) via `rust_decimal::serde::str`.
//!
//! Field mappings verified 2026-07-05 against the canonical docs source
//! (binance/binance-spot-api-docs, web-socket-streams.md); a trimmed excerpt
//! is cached in the `binance-api-reference` skill under
//! `references/websocket-streams.md`. Documented "Ignore" fields (`M` on
//! aggTrade, `B` on kline) are deliberately omitted — serde skips unknown
//! fields by default.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Payload of the `<symbol>@aggTrade` stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggTrade {
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time (ms since epoch).
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "a")]
    pub agg_trade_id: i64,
    #[serde(rename = "p", with = "rust_decimal::serde::str")]
    pub price: Decimal,
    #[serde(rename = "q", with = "rust_decimal::serde::str")]
    pub quantity: Decimal,
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    #[serde(rename = "l")]
    pub last_trade_id: i64,
    /// Trade time (ms since epoch).
    #[serde(rename = "T")]
    pub trade_time: i64,
    /// True when the buyer is the market maker.
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

/// Payload of the `<symbol>@bookTicker` stream.
///
/// Verified: this stream's payload carries NO event-type/event-time envelope —
/// just the book fields below.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookTicker {
    #[serde(rename = "u")]
    pub update_id: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b", with = "rust_decimal::serde::str")]
    pub best_bid_price: Decimal,
    #[serde(rename = "B", with = "rust_decimal::serde::str")]
    pub best_bid_qty: Decimal,
    #[serde(rename = "a", with = "rust_decimal::serde::str")]
    pub best_ask_price: Decimal,
    #[serde(rename = "A", with = "rust_decimal::serde::str")]
    pub best_ask_qty: Decimal,
}

/// Envelope of the `<symbol>@kline_<interval>` stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KlineEvent {
    #[serde(rename = "e")]
    pub event_type: String,
    /// Event time (ms since epoch).
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub kline: Kline,
}

/// The nested candlestick object inside a kline event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Kline {
    /// Kline start time (ms since epoch).
    #[serde(rename = "t")]
    pub open_time: i64,
    /// Kline close time (ms since epoch).
    #[serde(rename = "T")]
    pub close_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    /// Interval, e.g. "1m".
    #[serde(rename = "i")]
    pub interval: String,
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    #[serde(rename = "L")]
    pub last_trade_id: i64,
    #[serde(rename = "o", with = "rust_decimal::serde::str")]
    pub open: Decimal,
    #[serde(rename = "c", with = "rust_decimal::serde::str")]
    pub close: Decimal,
    #[serde(rename = "h", with = "rust_decimal::serde::str")]
    pub high: Decimal,
    #[serde(rename = "l", with = "rust_decimal::serde::str")]
    pub low: Decimal,
    #[serde(rename = "v", with = "rust_decimal::serde::str")]
    pub volume: Decimal,
    #[serde(rename = "n")]
    pub trade_count: i64,
    /// True once the candle is closed/final.
    #[serde(rename = "x")]
    pub is_closed: bool,
    #[serde(rename = "q", with = "rust_decimal::serde::str")]
    pub quote_volume: Decimal,
    #[serde(rename = "V", with = "rust_decimal::serde::str")]
    pub taker_buy_base_volume: Decimal,
    #[serde(rename = "Q", with = "rust_decimal::serde::str")]
    pub taker_buy_quote_volume: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Fixtures below are the exact example payloads from the Binance docs
    // (see the module-level comment for provenance).

    #[test]
    fn decodes_agg_trade_docs_example() {
        let json = r#"{
            "e": "aggTrade", "E": 1672515782136, "s": "BNBBTC", "a": 12345,
            "p": "0.001", "q": "100", "f": 100, "l": 105,
            "T": 1672515782136, "m": true, "M": true
        }"#;
        let trade: AggTrade = serde_json::from_str(json).expect("docs example must decode");
        assert_eq!(trade.symbol, "BNBBTC");
        assert_eq!(
            trade.price,
            "0.001".parse::<Decimal>().expect("valid decimal literal")
        );
        assert_eq!(
            trade.quantity,
            "100".parse::<Decimal>().expect("valid decimal literal")
        );
        assert!(trade.is_buyer_maker);
    }

    #[test]
    fn decodes_book_ticker_docs_example() {
        let json = r#"{
            "u": 400900217, "s": "BNBUSDT",
            "b": "25.35190000", "B": "31.21000000",
            "a": "25.36520000", "A": "40.66000000"
        }"#;
        let tick: BookTicker = serde_json::from_str(json).expect("docs example must decode");
        assert_eq!(tick.update_id, 400_900_217);
        assert_eq!(
            tick.best_ask_qty,
            "40.66".parse::<Decimal>().expect("valid decimal literal")
        );
    }

    #[test]
    fn decodes_kline_docs_example() {
        let json = r#"{
            "e": "kline", "E": 1672515782136, "s": "BNBBTC",
            "k": {
                "t": 1672515780000, "T": 1672515839999, "s": "BNBBTC", "i": "1m",
                "f": 100, "L": 200, "o": "0.0010", "c": "0.0020", "h": "0.0025",
                "l": "0.0015", "v": "1000", "n": 100, "x": false, "q": "1.0000",
                "V": "500", "Q": "0.500", "B": "123456"
            }
        }"#;
        let event: KlineEvent = serde_json::from_str(json).expect("docs example must decode");
        assert_eq!(event.kline.interval, "1m");
        assert!(!event.kline.is_closed);
        assert_eq!(
            event.kline.taker_buy_base_volume,
            "500".parse::<Decimal>().expect("valid decimal literal")
        );
    }
}
