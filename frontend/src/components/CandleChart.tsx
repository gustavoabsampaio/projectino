// Interactive candlestick chart, hand-rolled in SVG.
//
// Deliberately dependency-free (no charting library) — the project keeps its
// dependency tree small, and OHLC candles are just scaled rects + wicks.
//
// Interaction: scroll to zoom, drag to pan, hover for a crosshair + OHLCV
// tooltip. The price scale auto-fits the *visible* window, so zooming into a
// quiet stretch expands it rather than squashing it against the full range.

import { useMemo, useRef, useState } from 'react';
import type { Candle } from '../lib/candles';

const WIDTH = 760;
const HEIGHT = 300;
const PAD = { left: 74, right: 12, top: 12, bottom: 26 };
const INNER_W = WIDTH - PAD.left - PAD.right;
const INNER_H = HEIGHT - PAD.top - PAD.bottom;

/** Candles shown before the user zooms. */
const DEFAULT_VISIBLE = 120;
const MIN_VISIBLE = 5;

interface View {
  start: number;
  count: number;
}

function formatPrice(value: number): string {
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

/**
 * Short axis label scaled to the interval: seconds/minutes show a time, hours
 * show date + hour, and day/week/month show a dated label with the year (a
 * weekly chart spans years, so "Oct 6" alone is ambiguous).
 */
function formatAxisTime(ms: number, interval: string): string {
  const d = new Date(ms);
  if (/[dwM]$/.test(interval)) {
    return d.toLocaleDateString([], { year: '2-digit', month: 'short', day: 'numeric' });
  }
  if (interval.endsWith('h')) {
    return d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit' });
  }
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function formatFullTime(ms: number): string {
  return new Date(ms).toLocaleString();
}

/** Evenly spaced "nice enough" gridline values across a range. */
function ticks(min: number, max: number, n = 4): number[] {
  if (!(max > min)) return [min];
  return Array.from({ length: n + 1 }, (_, i) => min + ((max - min) * i) / n);
}

export default function CandleChart({
  candles,
  interval,
}: {
  candles: Candle[];
  interval: string;
}) {
  const svgRef = useRef<SVGSVGElement>(null);
  // `null` = follow the live edge; set once the user zooms or pans.
  const [view, setView] = useState<View | null>(null);
  const [hover, setHover] = useState<number | null>(null);
  const dragRef = useRef<{ pointerX: number; start: number } | null>(null);

  const total = candles.length;

  // Resolve the visible window, clamped to the data we actually have.
  const { start, visible } = useMemo(() => {
    if (total === 0) return { start: 0, visible: [] as Candle[] };
    const count = Math.max(MIN_VISIBLE, Math.min(view?.count ?? DEFAULT_VISIBLE, total));
    const maxStart = Math.max(0, total - count);
    const s = Math.max(0, Math.min(view?.start ?? maxStart, maxStart));
    return { start: s, visible: candles.slice(s, s + count) };
  }, [candles, total, view]);

  const scale = useMemo(() => {
    if (visible.length === 0) return null;
    const lows = visible.map((c) => Number(c.low));
    const highs = visible.map((c) => Number(c.high));
    let min = Math.min(...lows);
    let max = Math.max(...highs);
    // A little headroom so candles don't touch the frame.
    const pad = (max - min || Math.abs(max) * 0.001 || 1) * 0.08;
    min -= pad;
    max += pad;
    const span = max - min || 1;
    return {
      min,
      max,
      x: (i: number) =>
        PAD.left + (visible.length > 1 ? (i * INNER_W) / (visible.length - 1) : INNER_W / 2),
      y: (price: number) => PAD.top + INNER_H - ((price - min) / span) * INNER_H,
    };
  }, [visible]);

  if (total === 0 || !scale) {
    return (
      <p className="empty">
        No candles for this interval yet — run the backfill (<code>make backfill</code>) or let the
        pipeline collect some.
      </p>
    );
  }

  const bodyWidth = Math.max(1.5, Math.min(16, (INNER_W / visible.length) * 0.65));

  /** Map a pointer event to a candle index within the visible window. */
  const indexAt = (clientX: number): number => {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect || rect.width === 0) return 0;
    const xInView = ((clientX - rect.left) / rect.width) * WIDTH;
    const ratio = (xInView - PAD.left) / INNER_W;
    return Math.max(0, Math.min(visible.length - 1, Math.round(ratio * (visible.length - 1))));
  };

  /** Where the pointer sits across the plot area, 0..1 (count-independent). */
  const ratioAt = (clientX: number): number => {
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect || rect.width === 0) return 0.5;
    const xInView = ((clientX - rect.left) / rect.width) * WIDTH;
    return Math.max(0, Math.min(1, (xInView - PAD.left) / INNER_W));
  };

  const onWheel = (e: React.WheelEvent<SVGSVGElement>) => {
    const factor = e.deltaY > 0 ? 1.25 : 0.8;
    const ratio = ratioAt(e.clientX);
    // Functional update: a burst of wheel events within one frame would
    // otherwise all read the same stale window and apply a single step.
    setView((prev) => {
      const prevCount = Math.max(MIN_VISIBLE, Math.min(prev?.count ?? DEFAULT_VISIBLE, total));
      const prevStart = Math.max(
        0,
        Math.min(prev?.start ?? Math.max(0, total - prevCount), Math.max(0, total - prevCount)),
      );
      const nextCount = Math.max(MIN_VISIBLE, Math.min(Math.round(prevCount * factor), total));
      // Keep the candle under the cursor roughly in place while zooming.
      const anchor = prevStart + ratio * (prevCount - 1);
      const nextStart = Math.round(anchor - ratio * (nextCount - 1));
      return {
        start: Math.max(0, Math.min(nextStart, Math.max(0, total - nextCount))),
        count: nextCount,
      };
    });
  };

  const onPointerDown = (e: React.PointerEvent<SVGSVGElement>) => {
    dragRef.current = { pointerX: e.clientX, start };
    e.currentTarget.setPointerCapture(e.pointerId);
  };

  const onPointerMove = (e: React.PointerEvent<SVGSVGElement>) => {
    setHover(indexAt(e.clientX));
    const drag = dragRef.current;
    if (!drag) return;
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect || rect.width === 0) return;
    // Convert pixel movement into a candle offset.
    const perCandle = (rect.width * (INNER_W / WIDTH)) / Math.max(1, visible.length - 1);
    const moved = Math.round((drag.pointerX - e.clientX) / Math.max(perCandle, 0.5));
    const nextStart = Math.max(0, Math.min(drag.start + moved, total - visible.length));
    setView({ start: nextStart, count: visible.length });
  };

  const endDrag = (e: React.PointerEvent<SVGSVGElement>) => {
    dragRef.current = null;
    if (e.currentTarget.hasPointerCapture(e.pointerId)) {
      e.currentTarget.releasePointerCapture(e.pointerId);
    }
  };

  const hovered = hover !== null ? visible[hover] : undefined;
  const followingLiveEdge = view === null || start + visible.length >= total;

  return (
    <>
      <svg
        ref={svgRef}
        className="chart"
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        aria-label={`Candlestick chart, ${visible.length} of ${total} candles`}
        onWheel={onWheel}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={endDrag}
        onPointerCancel={endDrag}
        onPointerLeave={(e) => {
          endDrag(e);
          setHover(null);
        }}
      >
        {ticks(scale.min, scale.max).map((price) => (
          <g key={price}>
            <line
              className="grid"
              x1={PAD.left}
              x2={WIDTH - PAD.right}
              y1={scale.y(price)}
              y2={scale.y(price)}
            />
            <text
              className="axis"
              x={PAD.left - 8}
              y={scale.y(price)}
              textAnchor="end"
              dominantBaseline="middle"
            >
              {formatPrice(price)}
            </text>
          </g>
        ))}

        {visible.map((candle, i) => {
          const open = Number(candle.open);
          const close = Number(candle.close);
          const top = scale.y(Math.max(open, close));
          const bottom = scale.y(Math.min(open, close));
          return (
            <g key={candle.open_time} className={close >= open ? 'up' : 'down'}>
              <line
                className="wick"
                x1={scale.x(i)}
                x2={scale.x(i)}
                y1={scale.y(Number(candle.high))}
                y2={scale.y(Number(candle.low))}
              />
              <rect
                className="body"
                x={scale.x(i) - bodyWidth / 2}
                y={top}
                width={bodyWidth}
                height={Math.max(1, bottom - top)}
              />
            </g>
          );
        })}

        {hover !== null && hovered && (
          <line
            className="crosshair"
            x1={scale.x(hover)}
            x2={scale.x(hover)}
            y1={PAD.top}
            y2={PAD.top + INNER_H}
          />
        )}

        <text className="axis" x={PAD.left} y={HEIGHT - 6}>
          {formatAxisTime(visible[0]?.open_time ?? 0, interval)}
        </text>
        <text className="axis" x={WIDTH - PAD.right} y={HEIGHT - 6} textAnchor="end">
          {formatAxisTime(visible[visible.length - 1]?.open_time ?? 0, interval)}
        </text>

        {hovered && <Tooltip candle={hovered} x={scale.x(hover ?? 0)} />}
      </svg>

      <p className="chart-hint muted">
        showing {visible.length} of {total} candles
        {followingLiveEdge ? ' (latest)' : ''} · scroll to zoom, drag to pan, hover for detail
        {view !== null && (
          <>
            {' · '}
            <button type="button" className="link" onClick={() => setView(null)}>
              reset view
            </button>
          </>
        )}
      </p>
    </>
  );
}

/** OHLCV readout pinned near the hovered candle. */
function Tooltip({ candle, x }: { candle: Candle; x: number }) {
  const lines = [
    formatFullTime(candle.open_time),
    `O ${candle.open}`,
    `H ${candle.high}`,
    `L ${candle.low}`,
    `C ${candle.close}`,
    `V ${candle.volume}`,
    `${candle.trade_count} trades`,
  ];
  const boxW = 190;
  const boxH = lines.length * 14 + 10;
  // Flip to the other side of the crosshair near the right edge.
  const boxX = x + boxW + 12 > WIDTH ? x - boxW - 10 : x + 10;
  return (
    <g className="tooltip" pointerEvents="none">
      <rect x={boxX} y={PAD.top} width={boxW} height={boxH} rx={4} />
      {lines.map((line, i) => (
        <text key={line} x={boxX + 8} y={PAD.top + 18 + i * 14}>
          {line}
        </text>
      ))}
    </g>
  );
}
