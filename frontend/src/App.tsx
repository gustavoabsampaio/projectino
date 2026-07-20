import { useCallback, useEffect, useRef, useState } from 'react';
import { useSpacetimeDB, useTable } from 'spacetimedb/react';
import { tables } from './module_bindings';
import CandleChart from './components/CandleChart';
import { fetchCandles, type KlineRow } from './lib/api';
import { INTERVALS, refreshLabel, refreshPeriodMs } from './lib/intervals';

const SYMBOLS = ['BTCUSDT', 'ETHUSDT'];

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

/** History: fetched from the Axum + DataFusion api over the Parquet lake. */
function History() {
  const [symbol, setSymbol] = useState('BTCUSDT');
  const [barInterval, setBarInterval] = useState('1m');
  const [candles, setCandles] = useState<KlineRow[]>([]);
  const [state, setState] = useState<'loading' | 'ready' | 'error'>('loading');
  const [error, setError] = useState('');

  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);
  // Skip the "loading" flash on background polls — only the first load and
  // explicit refreshes should blank the chart.
  const loadedOnce = useRef(false);

  const load = useCallback(async (sym: string, interval: string, quiet = false) => {
    if (!quiet || !loadedOnce.current) setState('loading');
    try {
      setCandles(await fetchCandles(sym, interval));
      setState('ready');
      setUpdatedAt(new Date());
      loadedOnce.current = true;
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setState('error');
    }
  }, []);

  // Reload on selection change, then poll at the candle's own cadence:
  // 1s → every 1s, 1h → every 1h, capped at 24h (setInterval's 32-bit limit).
  useEffect(() => {
    loadedOnce.current = false;
    void load(symbol, barInterval);
    const period = refreshPeriodMs(barInterval);
    const timer = window.setInterval(() => void load(symbol, barInterval, true), period);
    return () => window.clearInterval(timer);
  }, [symbol, barInterval, load]);

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
        <button type="button" onClick={() => void load(symbol, barInterval)}>
          refresh
        </button>
        <span className="muted">
          auto {refreshLabel(barInterval)}
          {updatedAt && ` · updated ${updatedAt.toLocaleTimeString()}`}
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
