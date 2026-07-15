//! Decode an enveloped Kafka message into a typed reducer call, then invoke it
//! on the SpacetimeDB connection.
//!
//! The decode/conversion step ([`decode`]) is a pure function returning a
//! [`ReducerCall`], so it is unit-testable without a live connection; the thin
//! [`dispatch`] wrapper performs the actual (fire-and-forget) reducer call.
//!
//! `Decimal` prices/quantities are rendered to their exact decimal `String`
//! here — SpacetimeDB has no decimal column type, and the module stores them as
//! strings (see the spacetime-module).

use anyhow::{Context, Result, bail};
use common::events::{AggTrade, BookTicker, KlineEvent};
use serde::Deserialize;

use crate::module_bindings::{DbConnection, record_book_ticker, record_kline, record_trade};

/// A fully-decoded reducer invocation, with prices already converted to the
/// exact strings the module expects. Field order matches the reducer arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReducerCall {
    Trade {
        symbol: String,
        price: String,
        quantity: String,
        trade_time: i64,
        agg_trade_id: i64,
        is_buyer_maker: bool,
    },
    BookTicker {
        symbol: String,
        best_bid_price: String,
        best_bid_qty: String,
        best_ask_price: String,
        best_ask_qty: String,
        update_id: i64,
    },
    Kline {
        symbol: String,
        interval: String,
        open: String,
        high: String,
        low: String,
        close: String,
        volume: String,
        quote_volume: String,
        trade_count: i64,
        open_time: i64,
        close_time: i64,
        is_closed: bool,
    },
}

impl ReducerCall {
    /// Event-kind label for logging/metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            ReducerCall::Trade { .. } => "agg_trade",
            ReducerCall::BookTicker { .. } => "book_ticker",
            ReducerCall::Kline { .. } => "kline",
        }
    }
}

/// Envelope with the payload left unparsed until the type is known.
#[derive(Deserialize)]
struct RawEnvelope<'a> {
    event_type: String,
    #[serde(borrow)]
    payload: &'a serde_json::value::RawValue,
}

/// Decode an enveloped market event into a typed [`ReducerCall`]. Pure — no
/// connection, no side effects. Errors on undecodable input or unknown type.
pub fn decode(bytes: &[u8]) -> Result<ReducerCall> {
    let env: RawEnvelope = serde_json::from_slice(bytes).context("decoding envelope")?;
    let payload = env.payload.get();

    match env.event_type.as_str() {
        "agg_trade" => {
            let t: AggTrade = serde_json::from_str(payload).context("decoding aggTrade payload")?;
            Ok(ReducerCall::Trade {
                symbol: t.symbol,
                price: t.price.to_string(),
                quantity: t.quantity.to_string(),
                trade_time: t.trade_time,
                agg_trade_id: t.agg_trade_id,
                is_buyer_maker: t.is_buyer_maker,
            })
        }
        "book_ticker" => {
            let b: BookTicker =
                serde_json::from_str(payload).context("decoding bookTicker payload")?;
            Ok(ReducerCall::BookTicker {
                symbol: b.symbol,
                best_bid_price: b.best_bid_price.to_string(),
                best_bid_qty: b.best_bid_qty.to_string(),
                best_ask_price: b.best_ask_price.to_string(),
                best_ask_qty: b.best_ask_qty.to_string(),
                update_id: b.update_id,
            })
        }
        "kline" => {
            let event: KlineEvent =
                serde_json::from_str(payload).context("decoding kline payload")?;
            let k = event.kline;
            Ok(ReducerCall::Kline {
                symbol: k.symbol,
                interval: k.interval,
                open: k.open.to_string(),
                high: k.high.to_string(),
                low: k.low.to_string(),
                close: k.close.to_string(),
                volume: k.volume.to_string(),
                quote_volume: k.quote_volume.to_string(),
                trade_count: k.trade_count,
                open_time: k.open_time,
                close_time: k.close_time,
                is_closed: k.is_closed,
            })
        }
        other => bail!("unknown event_type `{other}`"),
    }
}

/// Decode `bytes` and enqueue the matching reducer call on `conn`. Returns the
/// event kind on success.
pub fn dispatch(conn: &DbConnection, bytes: &[u8]) -> Result<&'static str> {
    let call = decode(bytes)?;
    let kind = call.kind();
    match call {
        ReducerCall::Trade {
            symbol,
            price,
            quantity,
            trade_time,
            agg_trade_id,
            is_buyer_maker,
        } => conn
            .reducers
            .record_trade(
                symbol,
                price,
                quantity,
                trade_time,
                agg_trade_id,
                is_buyer_maker,
            )
            .map_err(|e| anyhow::anyhow!("record_trade call failed: {e}"))?,
        ReducerCall::BookTicker {
            symbol,
            best_bid_price,
            best_bid_qty,
            best_ask_price,
            best_ask_qty,
            update_id,
        } => conn
            .reducers
            .record_book_ticker(
                symbol,
                best_bid_price,
                best_bid_qty,
                best_ask_price,
                best_ask_qty,
                update_id,
            )
            .map_err(|e| anyhow::anyhow!("record_book_ticker call failed: {e}"))?,
        ReducerCall::Kline {
            symbol,
            interval,
            open,
            high,
            low,
            close,
            volume,
            quote_volume,
            trade_count,
            open_time,
            close_time,
            is_closed,
        } => conn
            .reducers
            .record_kline(
                symbol,
                interval,
                open,
                high,
                low,
                close,
                volume,
                quote_volume,
                trade_count,
                open_time,
                close_time,
                is_closed,
            )
            .map_err(|e| anyhow::anyhow!("record_kline call failed: {e}"))?,
    }
    Ok(kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Payloads are the exact Binance docs examples, wrapped in the pipeline
    // envelope the ingestor produces.

    #[test]
    fn decodes_agg_trade_and_stringifies_decimals() {
        let bytes = br#"{"event_type":"agg_trade","schema_version":1,"payload":{
            "e":"aggTrade","E":1672515782136,"s":"BTCUSDT","a":12345,"p":"0.001",
            "q":"100","f":100,"l":105,"T":1672515782136,"m":true,"M":true}}"#;
        let call = decode(bytes).expect("must decode");
        assert_eq!(
            call,
            ReducerCall::Trade {
                symbol: "BTCUSDT".to_string(),
                price: "0.001".to_string(),
                quantity: "100".to_string(),
                trade_time: 1672515782136,
                agg_trade_id: 12345,
                is_buyer_maker: true,
            }
        );
        assert_eq!(call.kind(), "agg_trade");
    }

    #[test]
    fn decodes_book_ticker() {
        let bytes = br#"{"event_type":"book_ticker","schema_version":1,"payload":{
            "u":400900217,"s":"ETHUSDT","b":"25.35190000","B":"31.21000000",
            "a":"25.36520000","A":"40.66000000"}}"#;
        let call = decode(bytes).expect("must decode");
        assert_eq!(
            call,
            ReducerCall::BookTicker {
                symbol: "ETHUSDT".to_string(),
                best_bid_price: "25.35190000".to_string(),
                best_bid_qty: "31.21000000".to_string(),
                best_ask_price: "25.36520000".to_string(),
                best_ask_qty: "40.66000000".to_string(),
                update_id: 400900217,
            }
        );
    }

    #[test]
    fn decodes_kline_from_nested_candle() {
        let bytes = br#"{"event_type":"kline","schema_version":1,"payload":{
            "e":"kline","E":1672515782136,"s":"BTCUSDT","k":{
            "t":1672515780000,"T":1672515839999,"s":"BTCUSDT","i":"1m","f":100,
            "L":200,"o":"0.0010","c":"0.0020","h":"0.0025","l":"0.0015","v":"1000",
            "n":100,"x":false,"q":"1.0000","V":"500","Q":"0.500","B":"123456"}}}"#;
        let call = decode(bytes).expect("must decode");
        assert_eq!(call.kind(), "kline");
        match call {
            ReducerCall::Kline {
                symbol,
                interval,
                close,
                is_closed,
                ..
            } => {
                assert_eq!(symbol, "BTCUSDT");
                assert_eq!(interval, "1m");
                assert_eq!(close, "0.0020");
                assert!(!is_closed);
            }
            other => panic!("expected Kline, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_event_type() {
        let bytes = br#"{"event_type":"depth","schema_version":1,"payload":{}}"#;
        assert!(decode(bytes).is_err());
    }

    #[test]
    fn rejects_malformed_json() {
        assert!(decode(b"{not json").is_err());
    }
}
