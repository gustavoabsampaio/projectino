//! Serde models for the Binance websocket payloads this pipeline consumes.
//!
//! Prices and quantities arrive as JSON *strings* and are decoded into
//! `Decimal` (never `f64`) via `rust_decimal::serde::str`.
//!
//! TODO: verify every `#[serde(rename)]` mapping (and any missing fields)
//! against Binance's current websocket stream docs before wiring the ingestor
//! to real payloads. The single-letter keys below follow the long-documented
//! shapes of the `aggTrade`, `bookTicker`, and `kline` streams, but they have
//! NOT been re-verified against the live API.

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
/// TODO: confirm whether the raw stream payload carries an event-type/time
/// envelope; historically it does not.
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
///
/// TODO: taker-buy volume fields ("V"/"Q") and the ignore field ("B") are
/// omitted for now — add them after verifying against current Binance docs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Kline {
    /// Open time (ms since epoch).
    #[serde(rename = "t")]
    pub open_time: i64,
    /// Close time (ms since epoch).
    #[serde(rename = "T")]
    pub close_time: i64,
    #[serde(rename = "s")]
    pub symbol: String,
    /// Interval, e.g. "1m".
    #[serde(rename = "i")]
    pub interval: String,
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
    #[serde(rename = "q", with = "rust_decimal::serde::str")]
    pub quote_volume: Decimal,
    #[serde(rename = "n")]
    pub trade_count: i64,
    /// True once the candle is closed/final.
    #[serde(rename = "x")]
    pub is_closed: bool,
}
