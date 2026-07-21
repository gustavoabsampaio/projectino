// Candles from two sources, reconciled.
//
// Every interval except 1s comes purely from the Parquet lake over the api. 1s
// is a hybrid: a `/klines` request costs ~2s against the lake, so polling it at
// 1s can never render a response that isn't already stale. Instead the chart
// seeds deep history from the lake *once*, then follows the module's rolling
// 1s window over the SpacetimeDB subscription — push, not poll.
//
// That leaves a seam where the two overlap, which `mergeCandles` closes.

/**
 * The fields the chart actually renders. Both the lake's `KlineRow` and the
 * module's live rows narrow to this, which is what lets one chart component
 * take either source without inventing the columns it doesn't have.
 */
export interface Candle {
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
  trade_count: number;
  open_time: number;
}

/**
 * One row of the module's rolling 1s window.
 *
 * The generated SpacetimeDB bindings are camelCase, and i64 columns arrive as
 * `bigint` — both differ from the lake's row shape, which is why this needs a
 * conversion rather than a cast.
 */
export interface LiveCandleRow {
  symbol: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
  tradeCount: bigint;
  openTime: bigint;
}

/** Narrow a live module row to the chart's shape. */
export function fromLiveRow(row: LiveCandleRow): Candle {
  return {
    open: row.open,
    high: row.high,
    low: row.low,
    close: row.close,
    volume: row.volume,
    trade_count: Number(row.tradeCount),
    // ms since epoch is ~1.8e12 — comfortably inside Number's safe range.
    open_time: Number(row.openTime),
  };
}

/**
 * Merge lake history with the live window, oldest-first.
 *
 * The two overlap: the lake holds every 1s candle the cold path has flushed,
 * and the live window holds the last ~10 minutes. Where both have the same
 * `open_time` they describe the same candle, and the live copy is at least as
 * fresh — it comes straight off the websocket rather than via a Parquet flush —
 * so it wins.
 */
export function mergeCandles(history: Candle[], live: Candle[]): Candle[] {
  const byOpenTime = new Map<number, Candle>();
  for (const candle of history) byOpenTime.set(candle.open_time, candle);
  for (const candle of live) byOpenTime.set(candle.open_time, candle);
  return [...byOpenTime.values()].sort((a, b) => a.open_time - b.open_time);
}
