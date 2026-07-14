//! projectino live-state module.
//!
//! Holds the *current* market state that browser clients subscribe to — one
//! row per symbol (per symbol+interval for klines), upserted on every event.
//! These are live-state tables, not history: they never grow unbounded (that
//! is the cold path's job). History lives in the Parquet lake.
//!
//! API verified 2026-07-14 against SpacetimeDB docs (spacetimedb 2.6):
//! - tables are private by default; `public` exposes them to client
//!   subscriptions (all tables here are public).
//! - upsert = `ctx.db.<table>().<pk>().find(key)` then `update(row)`, else
//!   `insert(row)`.
//! - no native decimal column type, so prices/quantities are stored as the
//!   exact Binance decimal *strings* (lossless); the hot-consumer converts
//!   `rust_decimal::Decimal` -> `String` at this boundary. Revisit with a
//!   scaled integer only if in-DB numeric comparison/aggregation is needed.
//! - timestamps are milliseconds since the Unix epoch (`i64`), as Binance
//!   sends them.

use spacetimedb::{ReducerContext, Table, reducer, table};

/// Latest aggregate trade per symbol.
#[table(accessor = live_trade, public)]
pub struct LiveTrade {
    #[primary_key]
    pub symbol: String,
    pub price: String,
    pub quantity: String,
    pub trade_time: i64,
    pub agg_trade_id: i64,
    pub is_buyer_maker: bool,
}

/// Best bid/ask per symbol.
#[table(accessor = live_book_ticker, public)]
pub struct LiveBookTicker {
    #[primary_key]
    pub symbol: String,
    pub best_bid_price: String,
    pub best_bid_qty: String,
    pub best_ask_price: String,
    pub best_ask_qty: String,
    pub update_id: i64,
}

/// Current candle per (symbol, interval).
///
/// SpacetimeDB primary keys are single-column, so the composite identity is a
/// derived `id` = `"<symbol>:<interval>"`; `symbol` is indexed so clients can
/// subscribe per symbol. Note: `id`/`bar_interval` are named to avoid the
/// reserved SQL words `key`/`interval`, which break `spacetime sql` queries
/// and client subscriptions.
#[table(accessor = live_kline, public)]
pub struct LiveKline {
    #[primary_key]
    pub id: String,
    #[index(btree)]
    pub symbol: String,
    pub bar_interval: String,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub quote_volume: String,
    pub trade_count: i64,
    pub open_time: i64,
    pub close_time: i64,
    pub is_closed: bool,
}

/// Composite key for `LiveKline`.
fn kline_key(symbol: &str, interval: &str) -> String {
    format!("{symbol}:{interval}")
}

#[reducer(init)]
pub fn init(_ctx: &ReducerContext) {
    log::info!("projectino live-state module initialized");
}

/// Upsert the latest trade for a symbol.
#[reducer]
pub fn record_trade(
    ctx: &ReducerContext,
    symbol: String,
    price: String,
    quantity: String,
    trade_time: i64,
    agg_trade_id: i64,
    is_buyer_maker: bool,
) {
    let row = LiveTrade {
        symbol: symbol.clone(),
        price,
        quantity,
        trade_time,
        agg_trade_id,
        is_buyer_maker,
    };
    if ctx.db.live_trade().symbol().find(&symbol).is_some() {
        ctx.db.live_trade().symbol().update(row);
    } else {
        ctx.db.live_trade().insert(row);
    }
}

/// Upsert the best bid/ask for a symbol.
#[reducer]
pub fn record_book_ticker(
    ctx: &ReducerContext,
    symbol: String,
    best_bid_price: String,
    best_bid_qty: String,
    best_ask_price: String,
    best_ask_qty: String,
    update_id: i64,
) {
    let row = LiveBookTicker {
        symbol: symbol.clone(),
        best_bid_price,
        best_bid_qty,
        best_ask_price,
        best_ask_qty,
        update_id,
    };
    if ctx.db.live_book_ticker().symbol().find(&symbol).is_some() {
        ctx.db.live_book_ticker().symbol().update(row);
    } else {
        ctx.db.live_book_ticker().insert(row);
    }
}

/// Upsert the current candle for a (symbol, interval).
#[reducer]
#[allow(clippy::too_many_arguments)]
pub fn record_kline(
    ctx: &ReducerContext,
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
) {
    let id = kline_key(&symbol, &interval);
    let row = LiveKline {
        id: id.clone(),
        symbol,
        bar_interval: interval,
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
    };
    if ctx.db.live_kline().id().find(&id).is_some() {
        ctx.db.live_kline().id().update(row);
    } else {
        ctx.db.live_kline().insert(row);
    }
}

#[cfg(test)]
mod tests {
    use super::kline_key;

    #[test]
    fn kline_key_is_symbol_and_interval() {
        assert_eq!(kline_key("BTCUSDT", "1m"), "BTCUSDT:1m");
        assert_ne!(kline_key("BTCUSDT", "1m"), kline_key("BTCUSDT", "5m"));
    }
}
