import { useSpacetimeDB, useTable } from 'spacetimedb/react';
import { tables } from './module_bindings';

/** Sort a copy of the rows by symbol for stable rendering. */
function bySymbol<T extends { symbol: string }>(rows: readonly T[]): T[] {
  return [...rows].sort((a, b) => a.symbol.localeCompare(b.symbol));
}

export default function App() {
  const conn = useSpacetimeDB();
  // Each hook subscribes to a public live-state table and re-renders on change.
  const [trades] = useTable(tables.live_trade);
  const [tickers] = useTable(tables.live_book_ticker);
  const [klines] = useTable(tables.live_kline);

  const status = conn.connectionError
    ? `error: ${conn.connectionError.message}`
    : conn.isActive
      ? 'connected'
      : 'connecting…';

  return (
    <main className="app">
      <header>
        <h1>projectino — live market state</h1>
        <p>
          SpacetimeDB: <span className={conn.isActive ? 'ok' : 'wait'}>{status}</span>
          {' · '}driven live by the hot-consumer
        </p>
      </header>

      <section>
        <h2>Book tickers</h2>
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
        <h2>Latest trades</h2>
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

      <section>
        <h2>Current candles</h2>
        <table>
          <thead>
            <tr>
              <th>Symbol</th>
              <th>Interval</th>
              <th>Open</th>
              <th>High</th>
              <th>Low</th>
              <th>Close</th>
            </tr>
          </thead>
          <tbody>
            {bySymbol(klines).map((k) => (
              <tr key={k.id}>
                <td>{k.symbol}</td>
                <td>{k.barInterval}</td>
                <td className="num">{k.open}</td>
                <td className="num">{k.high}</td>
                <td className="num">{k.low}</td>
                <td className="num">{k.close}</td>
              </tr>
            ))}
            {klines.length === 0 && <EmptyRow cols={6} />}
          </tbody>
        </table>
      </section>
    </main>
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
