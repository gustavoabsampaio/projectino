// Minimal candlestick chart, hand-rolled in SVG.
//
// Deliberately dependency-free (no charting library) — the project keeps its
// dependency tree small, and OHLC candles are just scaled rects + wicks.

import type { KlineRow } from '../lib/api';

const WIDTH = 760;
const HEIGHT = 280;
const PAD = { left: 74, right: 10, top: 10, bottom: 24 };

function formatPrice(value: number): string {
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatTime(ms: number): string {
  return new Date(ms).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

export default function CandleChart({ candles }: { candles: KlineRow[] }) {
  if (candles.length === 0) {
    return (
      <p className="empty">
        No candles in the lake yet — run the ingestor and cold-consumer, then reload.
      </p>
    );
  }

  const lows = candles.map((c) => Number(c.low));
  const highs = candles.map((c) => Number(c.high));
  const min = Math.min(...lows);
  const max = Math.max(...highs);
  const range = max - min || 1;

  const innerW = WIDTH - PAD.left - PAD.right;
  const innerH = HEIGHT - PAD.top - PAD.bottom;
  const step = candles.length > 1 ? innerW / (candles.length - 1) : 0;
  const x = (i: number) => PAD.left + (candles.length > 1 ? i * step : innerW / 2);
  const y = (price: number) => PAD.top + innerH - ((price - min) / range) * innerH;
  const bodyWidth = Math.max(2, Math.min(14, (step || innerW) * 0.6));

  const firstTime = candles[0]?.open_time;
  const lastTime = candles[candles.length - 1]?.open_time;

  return (
    <svg
      className="chart"
      viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
      role="img"
      aria-label={`Candlestick chart, ${candles.length} candles`}
    >
      {[min, min + range / 2, max].map((price) => (
        <g key={price}>
          <line className="grid" x1={PAD.left} x2={WIDTH - PAD.right} y1={y(price)} y2={y(price)} />
          <text
            className="axis"
            x={PAD.left - 8}
            y={y(price)}
            textAnchor="end"
            dominantBaseline="middle"
          >
            {formatPrice(price)}
          </text>
        </g>
      ))}

      {candles.map((candle, i) => {
        const open = Number(candle.open);
        const close = Number(candle.close);
        const isUp = close >= open;
        const top = y(Math.max(open, close));
        const bottom = y(Math.min(open, close));
        return (
          <g key={candle.open_time} className={isUp ? 'up' : 'down'}>
            <line
              className="wick"
              x1={x(i)}
              x2={x(i)}
              y1={y(Number(candle.high))}
              y2={y(Number(candle.low))}
            />
            <rect
              className="body"
              x={x(i) - bodyWidth / 2}
              y={top}
              width={bodyWidth}
              height={Math.max(1, bottom - top)}
            />
          </g>
        );
      })}

      {firstTime !== undefined && (
        <text className="axis" x={PAD.left} y={HEIGHT - 6}>
          {formatTime(firstTime)}
        </text>
      )}
      {lastTime !== undefined && (
        <text className="axis" x={WIDTH - PAD.right} y={HEIGHT - 6} textAnchor="end">
          {formatTime(lastTime)}
        </text>
      )}
    </svg>
  );
}
