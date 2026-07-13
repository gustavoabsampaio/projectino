//! Decoding of Binance combined-stream frames into typed market events.
//!
//! Combined-stream frames are wrapped as `{"stream":"<name>","data":<payload>}`
//! (verified; see the `binance-api-reference` skill cache). Routing is by
//! stream-name suffix, since not every payload self-describes (`bookTicker`
//! carries no event-type field).

use common::envelope::Envelope;
use common::events::{AggTrade, BookTicker, KlineEvent};
use common::topics;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unrecognized stream `{0}`")]
    UnknownStream(String),
}

/// A decoded frame: either a market event to publish, or a control signal.
/// The event is boxed so the control variant doesn't pay `MarketEvent`'s
/// (Kline-sized) footprint (clippy::large_enum_variant).
#[derive(Debug)]
pub enum Decoded {
    Market(Box<MarketEvent>),
    /// `!serverShutdown`: the server is about to disconnect everyone;
    /// callers should reconnect promptly.
    ServerShutdown,
}

#[derive(Debug)]
pub enum MarketEvent {
    AggTrade(AggTrade),
    BookTicker(BookTicker),
    Kline(KlineEvent),
}

impl MarketEvent {
    /// Envelope `event_type` discriminator.
    pub fn event_type(&self) -> &'static str {
        match self {
            MarketEvent::AggTrade(_) => "agg_trade",
            MarketEvent::BookTicker(_) => "book_ticker",
            MarketEvent::Kline(_) => "kline",
        }
    }

    /// Destination topic (one topic per event type — see `common::topics`).
    pub fn topic(&self) -> &'static str {
        match self {
            MarketEvent::AggTrade(_) => topics::TRADES,
            MarketEvent::BookTicker(_) => topics::BOOK_TICKERS,
            MarketEvent::Kline(_) => topics::KLINES,
        }
    }

    /// Partition key: the uppercase exchange symbol, guaranteeing per-symbol
    /// ordering within a partition.
    pub fn key(&self) -> &str {
        match self {
            MarketEvent::AggTrade(t) => &t.symbol,
            MarketEvent::BookTicker(t) => &t.symbol,
            MarketEvent::Kline(k) => &k.symbol,
        }
    }

    /// Serialize as the enveloped wire format produced to Kafka.
    pub fn to_envelope_json(&self) -> serde_json::Result<Vec<u8>> {
        match self {
            MarketEvent::AggTrade(p) => serde_json::to_vec(&Envelope::new(self.event_type(), p)),
            MarketEvent::BookTicker(p) => serde_json::to_vec(&Envelope::new(self.event_type(), p)),
            MarketEvent::Kline(p) => serde_json::to_vec(&Envelope::new(self.event_type(), p)),
        }
    }
}

/// One combined-stream frame. `data` stays unparsed until routing decides the
/// payload type.
#[derive(Deserialize)]
struct CombinedFrame<'a> {
    stream: &'a str,
    #[serde(borrow)]
    data: &'a serde_json::value::RawValue,
}

pub fn decode_combined_frame(text: &str) -> Result<Decoded, DecodeError> {
    let frame: CombinedFrame<'_> = serde_json::from_str(text)?;

    if frame.stream == "!serverShutdown" {
        return Ok(Decoded::ServerShutdown);
    }

    let payload = frame.data.get();
    let suffix = frame
        .stream
        .split_once('@')
        .map(|(_, suffix)| suffix)
        .ok_or_else(|| DecodeError::UnknownStream(frame.stream.to_string()))?;

    let event = match suffix {
        "aggTrade" => MarketEvent::AggTrade(serde_json::from_str(payload)?),
        "bookTicker" => MarketEvent::BookTicker(serde_json::from_str(payload)?),
        s if s.starts_with("kline_") => MarketEvent::Kline(serde_json::from_str(payload)?),
        _ => return Err(DecodeError::UnknownStream(frame.stream.to_string())),
    };
    Ok(Decoded::Market(Box::new(event)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // `data` payloads are the exact example payloads from the Binance docs.

    const AGG_TRADE_FRAME: &str = r#"{"stream":"bnbbtc@aggTrade","data":{
        "e":"aggTrade","E":1672515782136,"s":"BNBBTC","a":12345,"p":"0.001",
        "q":"100","f":100,"l":105,"T":1672515782136,"m":true,"M":true}}"#;

    const BOOK_TICKER_FRAME: &str = r#"{"stream":"bnbusdt@bookTicker","data":{
        "u":400900217,"s":"BNBUSDT","b":"25.35190000","B":"31.21000000",
        "a":"25.36520000","A":"40.66000000"}}"#;

    const KLINE_FRAME: &str = r#"{"stream":"bnbbtc@kline_1m","data":{
        "e":"kline","E":1672515782136,"s":"BNBBTC","k":{
        "t":1672515780000,"T":1672515839999,"s":"BNBBTC","i":"1m","f":100,
        "L":200,"o":"0.0010","c":"0.0020","h":"0.0025","l":"0.0015","v":"1000",
        "n":100,"x":false,"q":"1.0000","V":"500","Q":"0.500","B":"123456"}}}"#;

    fn expect_market(text: &str) -> MarketEvent {
        match decode_combined_frame(text).expect("frame must decode") {
            Decoded::Market(event) => *event,
            other => panic!("expected market event, got {other:?}"),
        }
    }

    #[test]
    fn routes_agg_trade() {
        let event = expect_market(AGG_TRADE_FRAME);
        assert_eq!(event.topic(), "market.trades");
        assert_eq!(event.key(), "BNBBTC");
        assert_eq!(event.event_type(), "agg_trade");
    }

    #[test]
    fn routes_book_ticker() {
        let event = expect_market(BOOK_TICKER_FRAME);
        assert_eq!(event.topic(), "market.book-tickers");
        assert_eq!(event.key(), "BNBUSDT");
    }

    #[test]
    fn routes_kline_any_interval() {
        let event = expect_market(KLINE_FRAME);
        assert_eq!(event.topic(), "market.klines");
        assert_eq!(event.event_type(), "kline");
    }

    #[test]
    fn recognizes_server_shutdown() {
        let frame =
            r#"{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1770123456789}}"#;
        assert!(matches!(
            decode_combined_frame(frame).expect("must decode"),
            Decoded::ServerShutdown
        ));
    }

    #[test]
    fn rejects_unknown_stream() {
        let frame = r#"{"stream":"bnbbtc@depth","data":{}}"#;
        assert!(matches!(
            decode_combined_frame(frame),
            Err(DecodeError::UnknownStream(_))
        ));
    }

    #[test]
    fn envelope_wire_format_carries_type_and_version() {
        let event = expect_market(AGG_TRADE_FRAME);
        let bytes = event.to_envelope_json().expect("must serialize");
        let value: serde_json::Value =
            serde_json::from_slice(&bytes).expect("wire format is valid JSON");
        assert_eq!(value["event_type"], "agg_trade");
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["payload"]["s"], "BNBBTC");
    }
}
