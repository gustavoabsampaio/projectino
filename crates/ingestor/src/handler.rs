//! Frame-handling path shared by the live ingestor and the replay harness.
//!
//! Both feed raw combined-stream text frames through [`handle_frame`], so the
//! decode/route/envelope logic under test is byte-for-byte identical whether
//! the input came from a live websocket or a recorded fixture file (see the
//! `replay-testing-harness` skill). The only thing that differs is the
//! [`Sink`]: live production goes to Kafka, replay goes nowhere.

use std::fmt;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::{debug, warn};

use crate::decode::{Decoded, MarketEvent, decode_combined_frame};

/// What the caller should do after a handled frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameAction {
    /// Keep reading.
    Continue,
    /// `!serverShutdown` seen — the live loop should reconnect promptly.
    ServerShutdown,
}

/// Running tallies over a stream or replay, rendered as a one-line summary.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Stats {
    pub agg_trades: u64,
    pub book_tickers: u64,
    pub klines: u64,
    pub decode_errors: u64,
    pub server_shutdowns: u64,
}

impl Stats {
    /// Total successfully decoded market events (excludes errors/control).
    pub fn events(&self) -> u64 {
        self.agg_trades + self.book_tickers + self.klines
    }

    fn record(&mut self, event: &MarketEvent) {
        match event {
            MarketEvent::AggTrade(_) => self.agg_trades += 1,
            MarketEvent::BookTicker(_) => self.book_tickers += 1,
            MarketEvent::Kline(_) => self.klines += 1,
        }
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} events (agg_trade={}, book_ticker={}, kline={}); {} decode errors; {} server-shutdowns",
            self.events(),
            self.agg_trades,
            self.book_tickers,
            self.klines,
            self.decode_errors,
            self.server_shutdowns,
        )
    }
}

/// Destination for decoded events. `Kafka` is the live path; `Null` validates
/// the decode path with no external dependency (replay + integration tests).
pub enum Sink<'a> {
    Kafka(&'a FutureProducer),
    Null,
}

impl Sink<'_> {
    async fn accept(&self, event: &MarketEvent) -> Result<()> {
        match self {
            Sink::Kafka(producer) => publish(producer, event).await,
            Sink::Null => Ok(()),
        }
    }
}

/// Decode one raw text frame and route any market event to `sink`, updating
/// `stats`. A single undecodable frame is logged and counted, never fatal —
/// the same policy live and in replay, so one bad message can't take the
/// ingestor down. Propagates only genuine sink failures (e.g. a Kafka error).
pub async fn handle_frame(text: &str, sink: &Sink<'_>, stats: &mut Stats) -> Result<FrameAction> {
    match decode_combined_frame(text) {
        Ok(Decoded::Market(event)) => {
            sink.accept(&event).await?;
            stats.record(&event);
            debug!(topic = event.topic(), key = event.key(), "frame handled");
            Ok(FrameAction::Continue)
        }
        Ok(Decoded::ServerShutdown) => {
            stats.server_shutdowns += 1;
            Ok(FrameAction::ServerShutdown)
        }
        Err(error) => {
            stats.decode_errors += 1;
            warn!(
                error = %error,
                frame = %truncate(text, 256),
                "dropping undecodable frame"
            );
            Ok(FrameAction::Continue)
        }
    }
}

async fn publish(producer: &FutureProducer, event: &MarketEvent) -> Result<()> {
    let payload = event
        .to_envelope_json()
        .context("serializing event envelope")?;
    let record = FutureRecord::to(event.topic())
        .key(event.key())
        .payload(&payload);

    // Awaiting the delivery future applies natural backpressure: the read loop
    // can't outrun the broker. Revisit (pipelined sends) if throughput demands.
    let delivery = producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(error, _msg)| anyhow!(error))
        .with_context(|| format!("producing to {}", event.topic()))?;
    debug!(
        topic = event.topic(),
        key = event.key(),
        partition = delivery.partition,
        offset = delivery.offset,
        "event published"
    );
    Ok(())
}

/// Char-boundary-safe truncation for logging payload snippets.
fn truncate(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    const AGG_TRADE_FRAME: &str = r#"{"stream":"bnbbtc@aggTrade","data":{
        "e":"aggTrade","E":1672515782136,"s":"BNBBTC","a":12345,"p":"0.001",
        "q":"100","f":100,"l":105,"T":1672515782136,"m":true,"M":true}}"#;

    #[test]
    fn truncate_respects_char_boundaries() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello", 3), "hel");
        // '€' is 3 bytes; cutting mid-char must back up, not panic.
        assert_eq!(truncate("€€", 4), "€");
    }

    #[tokio::test]
    async fn counts_market_event_via_null_sink() {
        let mut stats = Stats::default();
        let action = handle_frame(AGG_TRADE_FRAME, &Sink::Null, &mut stats)
            .await
            .expect("null sink never fails");
        assert_eq!(action, FrameAction::Continue);
        assert_eq!(stats.agg_trades, 1);
        assert_eq!(stats.events(), 1);
        assert_eq!(stats.decode_errors, 0);
    }

    #[tokio::test]
    async fn undecodable_frame_is_counted_not_fatal() {
        let mut stats = Stats::default();
        let action = handle_frame("{not valid json", &Sink::Null, &mut stats)
            .await
            .expect("bad frames are dropped, not errors");
        assert_eq!(action, FrameAction::Continue);
        assert_eq!(stats.decode_errors, 1);
        assert_eq!(stats.events(), 0);
    }

    #[tokio::test]
    async fn server_shutdown_signals_reconnect() {
        let mut stats = Stats::default();
        let frame =
            r#"{"stream":"!serverShutdown","data":{"e":"serverShutdown","E":1770123456789}}"#;
        let action = handle_frame(frame, &Sink::Null, &mut stats)
            .await
            .expect("control frame decodes");
        assert_eq!(action, FrameAction::ServerShutdown);
        assert_eq!(stats.server_shutdowns, 1);
    }

    #[test]
    fn stats_display_is_readable() {
        let stats = Stats {
            agg_trades: 3,
            book_tickers: 2,
            klines: 1,
            decode_errors: 0,
            server_shutdowns: 0,
        };
        assert_eq!(
            stats.to_string(),
            "6 events (agg_trade=3, book_ticker=2, kline=1); 0 decode errors; 0 server-shutdowns"
        );
    }
}
