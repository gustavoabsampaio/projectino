//! Kafka/Redpanda topic names — the single code-level source of truth.
//!
//! Naming contract (see the `kafka-schema-conventions` skill): one topic per
//! event type, `<domain>.<event-type>`, lowercase, dot-separated. Messages
//! are keyed by uppercase symbol (e.g. `"BTCUSDT"`) so per-symbol ordering is
//! guaranteed within a partition. Update the skill file and this module
//! together when adding an event type.

pub const TRADES: &str = "market.trades";
pub const BOOK_TICKERS: &str = "market.book-tickers";
pub const KLINES: &str = "market.klines";

/// Dead-letter topic corresponding to a raw topic.
pub fn dlq(topic: &str) -> String {
    format!("{topic}.dlq")
}

#[cfg(test)]
mod tests {
    #[test]
    fn dlq_appends_suffix() {
        assert_eq!(super::dlq(super::TRADES), "market.trades.dlq");
    }
}
