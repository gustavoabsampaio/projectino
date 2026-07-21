// Binance kline intervals and their durations.

/**
 * Every interval Binance documents (verified against binance-spot-api-docs):
 * there is no `10m`, no `3h`, and a day is `1d`, not `24h`. Kept complete so
 * `INTERVAL_MS` stays correct for any interval that gets re-enabled.
 */
export const ALL_INTERVALS = [
  '1s',
  '1m',
  '3m',
  '5m',
  '15m',
  '30m',
  '1h',
  '2h',
  '4h',
  '6h',
  '8h',
  '12h',
  '1d',
  '3d',
  '1w',
  '1M',
] as const;

/**
 * The intervals the chart offers.
 *
 * **Must match `DEFAULT_INTERVALS` in `crates/ingestor/src/config.rs`.** The
 * ingestor only subscribes to those, so offering one it doesn't stream is worse
 * than omitting it: the chart would render whatever stale history the lake
 * happens to hold and then never update, with nothing on screen saying why.
 *
 * Nothing enforces this at build time — one list is Rust, the other TypeScript —
 * so changing either means changing both.
 */
export const INTERVALS = [
  '1s',
  '1m',
  '5m',
  '15m',
  '30m',
  '1h',
  '6h',
  '12h',
  '1d',
  '1w',
  '1M',
] as const;

export type Interval = (typeof INTERVALS)[number];

const SECOND = 1_000;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

/** Nominal duration of one candle, in milliseconds. Covers `ALL_INTERVALS`. */
export const INTERVAL_MS: Record<string, number> = {
  '1s': SECOND,
  '1m': MINUTE,
  '3m': 3 * MINUTE,
  '5m': 5 * MINUTE,
  '15m': 15 * MINUTE,
  '30m': 30 * MINUTE,
  '1h': HOUR,
  '2h': 2 * HOUR,
  '4h': 4 * HOUR,
  '6h': 6 * HOUR,
  '8h': 8 * HOUR,
  '12h': 12 * HOUR,
  '1d': DAY,
  '3d': 3 * DAY,
  '1w': 7 * DAY,
  '1M': 30 * DAY,
};

/**
 * `setInterval` stores its delay in a 32-bit int: anything above ~24.8 days
 * overflows and fires immediately, in a loop. `1M` (30 days) crosses that, so
 * cap the polling period — a chart that refreshes at most daily is plenty for
 * the long intervals.
 */
const MAX_TIMER_MS = DAY;

/** How often to re-poll the api for a given interval (1s → 1s, 1h → 1h, …). */
export function refreshPeriodMs(interval: string): number {
  return Math.min(INTERVAL_MS[interval] ?? MINUTE, MAX_TIMER_MS);
}

/** Human label for the refresh cadence, e.g. "every 1h". */
export function refreshLabel(interval: string): string {
  const period = refreshPeriodMs(interval);
  if (period >= DAY) return 'every 24h';
  if (period >= HOUR) return `every ${Math.round(period / HOUR)}h`;
  if (period >= MINUTE) return `every ${Math.round(period / MINUTE)}m`;
  return `every ${Math.round(period / SECOND)}s`;
}
