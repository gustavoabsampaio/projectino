// Client for the historical-query API (Axum + DataFusion over the Parquet lake).
//
// Live state comes from SpacetimeDB subscriptions (see spacetime.ts); this
// module is the *cold* half — history read back out of the lake over REST.
//
// Prices/quantities are exact decimal strings (the lake has no decimal column
// type), so convert with Number() only for rendering, never for accounting.

const API_URL: string = import.meta.env.VITE_API_URL ?? 'http://localhost:8081';

/** One row of `market.klines` as stored in the lake. */
export interface KlineRow {
  symbol: string;
  interval: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
  quote_volume: string;
  trade_count: number;
  open_time: number;
  close_time: number;
  is_closed: boolean;
  kafka_partition: number;
  kafka_offset: number;
}

/** One row of `market.trades` as stored in the lake. */
export interface TradeRow {
  symbol: string;
  price: string;
  quantity: string;
  trade_time: number;
  agg_trade_id: number;
  is_buyer_maker: boolean;
  kafka_partition: number;
  kafka_offset: number;
}

type Params = Record<string, string | number | undefined>;

/** Extra request options; `signal` lets callers abort a superseded fetch. */
export interface RequestOptions {
  limit?: number;
  signal?: AbortSignal;
}

async function getRows<T>(path: string, params: Params, signal?: AbortSignal): Promise<T[]> {
  const url = new URL(path, API_URL);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) url.searchParams.set(key, String(value));
  }
  const res = await fetch(url, { signal });
  if (!res.ok) {
    throw new Error(`GET ${path} failed: ${res.status} ${res.statusText}`);
  }
  return (await res.json()) as T[];
}

/**
 * The lake is append-only, so a 1m candle appears once per update (~every 2s),
 * many rows sharing one `open_time`. Keep the newest row per candle (highest
 * Kafka offset) and return them oldest-first for charting.
 */
export function dedupeCandles(rows: KlineRow[]): KlineRow[] {
  const latest = new Map<number, KlineRow>();
  for (const row of rows) {
    const seen = latest.get(row.open_time);
    if (!seen || row.kafka_offset > seen.kafka_offset) latest.set(row.open_time, row);
  }
  return [...latest.values()].sort((a, b) => a.open_time - b.open_time);
}

/** Fetch history and collapse it into one row per candle, oldest-first. */
export async function fetchCandles(
  symbol: string,
  interval = '1m',
  { limit = 1000, signal }: RequestOptions = {},
): Promise<KlineRow[]> {
  const rows = await getRows<KlineRow>('/klines', { symbol, interval, limit }, signal);
  return dedupeCandles(rows);
}

export async function fetchTrades(
  symbol: string,
  { limit = 20, signal }: RequestOptions = {},
): Promise<TradeRow[]> {
  return getRows<TradeRow>('/trades', { symbol, limit }, signal);
}
