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

/// A rolling window of 1s candles per symbol.
///
/// The one interval where the cold path can't keep up: a `/klines` request
/// costs ~2s against the lake, so a 1s chart polling the api can never render
/// a response that isn't already stale. This table is the live half of that
/// chart — the client seeds deep history from the lake once, then follows this
/// subscription for updates.
///
/// Unlike the other tables here this one is *append*-shaped, so it is bounded
/// explicitly by [`trim_window`] rather than by upserting a single row. Only
/// 1s is stored: every coarser interval polls the lake perfectly well, and
/// keeping hours of them here would defeat the point of a bounded live table.
#[table(accessor = live_kline_second, public)]
pub struct LiveKlineSecond {
    /// `"<symbol>:<open_time>"` — pks are single-column, so the composite
    /// identity is derived (same trick as `LiveKline`).
    #[primary_key]
    pub id: String,
    #[index(btree)]
    pub symbol: String,
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

/// Candles kept per symbol — ~10 minutes at one per second. Deep history comes
/// from the lake, so this only has to cover the gap since the client loaded.
const WINDOW_1S: usize = 600;

/// Extra candles tolerated before a trim runs. Trimming scans the table, so
/// this amortizes that cost over `SLACK` inserts instead of paying it on every
/// one.
const WINDOW_1S_SLACK: usize = 120;

/// Composite key for `LiveKlineSecond`.
fn kline_second_key(symbol: &str, open_time: i64) -> String {
    format!("{symbol}:{open_time}")
}

/// The `open_time` below which rows should be dropped, or `None` while the
/// window still has room. Pure so it can be unit-tested on the host target.
fn trim_cutoff(times: &mut [i64], keep: usize, slack: usize) -> Option<i64> {
    if times.len() <= keep + slack {
        return None;
    }
    times.sort_unstable();
    Some(times[times.len() - keep])
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

/// Upsert one 1s candle, then bound the window.
///
/// A forming candle is re-sent every time it updates, so the same `open_time`
/// arrives repeatedly and updates in place; only a genuinely new candle can
/// grow the table, which is why the trim only runs on insert.
#[reducer]
#[allow(clippy::too_many_arguments)]
pub fn record_kline_second(
    ctx: &ReducerContext,
    symbol: String,
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
    let id = kline_second_key(&symbol, open_time);
    let row = LiveKlineSecond {
        id: id.clone(),
        symbol: symbol.clone(),
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
    if ctx.db.live_kline_second().id().find(&id).is_some() {
        ctx.db.live_kline_second().id().update(row);
    } else {
        ctx.db.live_kline_second().insert(row);
        trim_window(ctx, &symbol);
    }
}

/// Drop the oldest candles for `symbol` once the window overflows.
///
/// Scans the table, which is O(rows). That's acceptable only because the trim
/// runs once per `WINDOW_1S_SLACK` inserts (about every two minutes per symbol
/// at one candle a second) and the table is bounded by construction. Rows are
/// deleted by primary key — the only delete form verified for this SDK
/// version; a range delete over the `open_time` index would be better if it
/// exists.
fn trim_window(ctx: &ReducerContext, symbol: &str) {
    let mut times: Vec<i64> = ctx
        .db
        .live_kline_second()
        .iter()
        .filter(|row| row.symbol == symbol)
        .map(|row| row.open_time)
        .collect();

    let Some(cutoff) = trim_cutoff(&mut times, WINDOW_1S, WINDOW_1S_SLACK) else {
        return;
    };
    let dropped = times.iter().take_while(|t| **t < cutoff).count();
    for open_time in times.into_iter().take(dropped) {
        ctx.db
            .live_kline_second()
            .id()
            .delete(kline_second_key(symbol, open_time));
    }
    log::info!("trimmed {dropped} candles for {symbol} (window {WINDOW_1S})");
}

#[cfg(test)]
mod tests {
    use super::{kline_key, kline_second_key, trim_cutoff};

    #[test]
    fn kline_key_is_symbol_and_interval() {
        assert_eq!(kline_key("BTCUSDT", "1m"), "BTCUSDT:1m");
        assert_ne!(kline_key("BTCUSDT", "1m"), kline_key("BTCUSDT", "5m"));
    }

    #[test]
    fn kline_second_key_is_symbol_and_open_time() {
        assert_eq!(
            kline_second_key("BTCUSDT", 1_700_000_000_000),
            "BTCUSDT:1700000000000"
        );
        assert_ne!(
            kline_second_key("BTCUSDT", 1),
            kline_second_key("ETHUSDT", 1)
        );
    }

    #[test]
    fn no_cutoff_until_the_window_overflows() {
        // Exactly keep+slack is still within tolerance — trimming every insert
        // is what the slack exists to avoid.
        let mut times: Vec<i64> = (0..12).collect();
        assert_eq!(trim_cutoff(&mut times, 10, 2), None);
    }

    #[test]
    fn cutoff_keeps_exactly_the_newest_window() {
        let mut times: Vec<i64> = (0..13).collect();
        // 13 rows, keep 10 → drop the 3 oldest, so the cutoff is open_time 3.
        assert_eq!(trim_cutoff(&mut times, 10, 2), Some(3));
        assert_eq!(times.iter().filter(|t| **t >= 3).count(), 10);
    }

    #[test]
    fn cutoff_is_computed_on_time_order_not_arrival_order() {
        // Candles can arrive out of order across partitions; the window must
        // still be the newest N by open_time.
        let mut times = vec![50, 10, 90, 20, 80, 30, 70, 40, 60, 100, 5, 95, 15];
        assert_eq!(trim_cutoff(&mut times, 10, 2), Some(20));
    }
}
