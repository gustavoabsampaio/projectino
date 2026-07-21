import { useEffect, useMemo, useState } from 'react';
import { useSpacetimeDB, useTable } from 'spacetimedb/react';
import { tables } from './module_bindings';
import CandleChart from './components/CandleChart';
import { fetchCandles, type KlineRow } from './lib/api';
import { fromLiveRow, mergeCandles } from './lib/candles';
import { INTERVALS, refreshLabel, refreshPeriodMs } from './lib/intervals';

const SYMBOLS = ['BTCUSDT', 'ETHUSDT'];

/**
 * Hard deadline on a single history request. Generous next to the observed ~2s
 * response, but bounded — see the poll loop in `History` for why an unbounded
 * request would freeze the chart permanently.
 */
const REQUEST_TIMEOUT_MS = 20_000;

/**
 * The one interval served live from SpacetimeDB rather than by polling the
 * lake. Must match the interval the hot-consumer mirrors into the module's
 * rolling window (`INTERVAL_1S` in `crates/hot-consumer/src/translate.rs`).
 */
const LIVE_INTERVAL = '1s';

/** Sort a copy of the rows by symbol for stable rendering. */
function bySymbol<T extends { symbol: string }>(rows: readonly T[]): T[] {
  return [...rows].sort((a, b) => a.symbol.localeCompare(b.symbol));
}

export default function App() {
  return (
    <main className="app">
      <header>
        <h1>projectino — crypto market pipeline</h1>
        <LiveStatus />
      </header>
      <Live />
      <History />
    </main>
  );
}

function LiveStatus() {
  const conn = useSpacetimeDB();
  const status = conn.connectionError
    ? `error: ${conn.connectionError.message}`
    : conn.isActive
      ? 'connected'
      : 'connecting…';
  return (
    <p>
      SpacetimeDB: <span className={conn.isActive ? 'ok' : 'wait'}>{status}</span>
      {' · '}live state from the hot path, history from the Parquet lake
    </p>
  );
}

/** Live state: pushed from SpacetimeDB subscriptions (hot path). */
function Live() {
  const [trades] = useTable(tables.live_trade);
  const [tickers] = useTable(tables.live_book_ticker);

  return (
    <>
      <section>
        <h2>
          Book tickers <span className="muted">— live</span>
        </h2>
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Bid</th>
              <th>Ask</th>
              <th>Spread</th>
            </tr>
          </thead>
          <tbody>
            {bySymbol(tickers).map((t) => (
              <tr key={t.symbol}>
                <td>{t.symbol}</td>
                <td className="num">{t.bestBidPrice}</td>
                <td className="num">{t.bestAskPrice}</td>
                <td className="num">
                  {(Number(t.bestAskPrice) - Number(t.bestBidPrice)).toFixed(2)}
                </td>
              </tr>
            ))}
            {tickers.length === 0 && <EmptyRow cols={4} />}
          </tbody>
        </table>
      </section>

      <section>
        <h2>
          Latest trades <span className="muted">— live</span>
        </h2>
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Price</th>
              <th>Quantity</th>
              <th>Side</th>
            </tr>
          </thead>
          <tbody>
            {bySymbol(trades).map((t) => (
              <tr key={t.symbol}>
                <td>{t.symbol}</td>
                <td className="num">{t.price}</td>
                <td className="num">{t.quantity}</td>
                <td>{t.isBuyerMaker ? 'sell' : 'buy'}</td>
              </tr>
            ))}
            {trades.length === 0 && <EmptyRow cols={4} />}
          </tbody>
        </table>
      </section>
    </>
  );
}

/**
 * History from the Axum + DataFusion api over the Parquet lake — except at 1s,
 * which is a hybrid: seeded from the lake once, then followed live over the
 * SpacetimeDB subscription. See `lib/candles.ts` for why.
 */
function History() {
  const [symbol, setSymbol] = useState('BTCUSDT');
  const [barInterval, setBarInterval] = useState('1m');
  const [seeded, setSeeded] = useState<KlineRow[]>([]);
  const [state, setState] = useState<'loading' | 'ready' | 'error'>('loading');
  const [error, setError] = useState('');

  // At 1s the chart follows the module's rolling window instead of polling.
  // The subscription is always open — the table is bounded to ~10 minutes per
  // symbol, so it costs little to hold — and simply goes unused at coarser
  // intervals, which the lake serves perfectly well on its own.
  const isLive = barInterval === LIVE_INTERVAL;
  const [liveRows] = useTable(tables.live_kline_second);

  const candles = useMemo(() => {
    if (!isLive) return seeded;
    const live = liveRows.filter((row) => row.symbol === symbol).map(fromLiveRow);
    return mergeCandles(seeded, live);
  }, [isLive, seeded, liveRows, symbol]);

  // At 1s the freshness that matters is the newest candle's own timestamp, not
  // when the seed request landed — that would sit there going stale while the
  // chart was in fact updating every second.
  const liveUpdatedAt = useMemo(
    () => (isLive && candles.length > 0 ? new Date(candles[candles.length - 1].open_time) : null),
    [isLive, candles],
  );

  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);
  // Bumping this re-runs the effect below — that's the manual refresh.
  const [reloadToken, setReloadToken] = useState(0);

  // Load on selection change, then re-poll at the candle's own cadence:
  // 1s → every 1s, 1h → every 1h, capped at 24h.
  //
  // The poll re-arms itself *after* each request settles rather than firing on
  // a fixed interval. That's deliberate: at 1s the api is slower than the poll
  // period (~2s per response), and a fixed interval would start a new request
  // before the previous one landed, so every response was superseded by a newer
  // one and the chart never rendered. Self-scheduling makes overlap impossible
  // by construction, so a slow api degrades to slower refreshes rather than to
  // none — at the cost of the period being "time since last completion", so the
  // real cadence at 1s is ~1s + response time.
  //
  // `REQUEST_TIMEOUT_MS` is what keeps that from wedging: a request that never
  // settles would otherwise never re-arm the timer and the chart would freeze
  // permanently, with no recovery even once the api came back. The deadline
  // guarantees the `finally` always runs.
  //
  // `cancelled` + `abort` cover the other direction: when the selection changes,
  // a request already in flight must not overwrite the new interval's data.
  useEffect(() => {
    let cancelled = false;
    let firstLoad = true;
    let timer: number | undefined;
    let inFlight: AbortController | null = null;

    const run = async () => {
      const controller = new AbortController();
      inFlight = controller;
      let timedOut = false;
      const deadline = window.setTimeout(() => {
        timedOut = true;
        controller.abort();
      }, REQUEST_TIMEOUT_MS);

      if (firstLoad) setState('loading');
      try {
        const rows = await fetchCandles(symbol, barInterval, { signal: controller.signal });
        if (cancelled) return;
        setSeeded(rows);
        setState('ready');
        setUpdatedAt(new Date());
      } catch (err) {
        // An abort from the cleanup below is expected — not an error. A timeout
        // is a real failure and should surface as one.
        if (cancelled) return;
        setError(
          timedOut
            ? `No response from the api within ${REQUEST_TIMEOUT_MS / 1000}s — is it running?`
            : err instanceof Error
              ? err.message
              : String(err),
        );
        setState('error');
      } finally {
        window.clearTimeout(deadline);
        firstLoad = false;
        inFlight = null;
        // Re-arm even after an error, so the chart recovers on its own once the
        // api is healthy again — but not at 1s, where this request is a
        // one-shot seed and the subscription takes over from here. Re-arming
        // there is exactly the polling this interval was moved off.
        if (!cancelled && !isLive) {
          timer = window.setTimeout(() => void run(), refreshPeriodMs(barInterval));
        }
      }
    };

    void run();
    return () => {
      cancelled = true;
      inFlight?.abort();
      window.clearTimeout(timer);
    };
  }, [symbol, barInterval, reloadToken, isLive]);

  return (
    <section>
      <h2>
        Candles <span className="muted">— history, from the Parquet lake</span>
      </h2>
      <div className="controls">
        {SYMBOLS.map((s) => (
          <button
            key={s}
            type="button"
            className={s === symbol ? 'active' : ''}
            onClick={() => setSymbol(s)}
          >
            {s}
          </button>
        ))}
        <span className="muted">|</span>
        {INTERVALS.map((i) => (
          <button
            key={i}
            type="button"
            className={i === barInterval ? 'active' : ''}
            onClick={() => setBarInterval(i)}
          >
            {i}
          </button>
        ))}
        <button type="button" onClick={() => setReloadToken((t) => t + 1)}>
          refresh
        </button>
        <span className="muted">
          {isLive ? 'live · pushed from spacetimedb' : `auto ${refreshLabel(barInterval)}`}
          {isLive
            ? liveUpdatedAt && ` · candle ${liveUpdatedAt.toLocaleTimeString()}`
            : updatedAt && ` · updated ${updatedAt.toLocaleTimeString()}`}
          {state === 'loading' && ' · loading…'}
        </span>
      </div>
      {state === 'error' ? (
        <p className="empty">
          could not reach the api ({error}) — is <code>cargo run -p api</code> running?
        </p>
      ) : (
        <CandleChart candles={candles} interval={barInterval} />
      )}
    </section>
  );
}

function EmptyRow({ cols }: { cols: number }) {
  return (
    <tr>
      <td colSpan={cols} className="empty">
        no rows yet — is the ingestor + hot-consumer running?
      </td>
    </tr>
  );
}
