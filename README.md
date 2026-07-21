# projectino

Real-time crypto market data pipeline (portfolio project, built alongside an
AI agent).

**Status:** every stage is implemented and the pipeline runs end to end,
exchange to browser, on both paths.

The **ingestor** streams live Binance market data (aggTrades, bookTickers,
klines) over a combined websocket stream and produces enveloped JSON events to
per-event-type Redpanda topics, keyed by symbol, with jittered-backoff
reconnects and graceful shutdown. Sends are pipelined into librdkafka rather
than awaited per message — see the throughput note in the TODOs for why that
mattered.

The **hot path**: the `hot-consumer` reads those topics and calls SpacetimeDB
reducers to maintain live-state tables — latest trade, book ticker and current
candle per symbol, plus a bounded rolling window of `1s` candles — and the
**React frontend subscribes to them live** via the SpacetimeDB TS SDK, so
Binance ticks show up in the browser in real time.

The **cold path**: the `cold-consumer` batches events into Arrow and writes
partitioned Parquet to the MinIO lake (deterministic, idempotent filenames;
offsets committed only after upload), and the `api` crate serves historical
queries over that lake with **DataFusion** — registering the Parquet as tables
and answering typed REST endpoints (`/trades`, `/book_tickers`, `/klines`).

The frontend renders both, and the candlestick chart uses **each path where it
wins**: `1s` is seeded once from the lake and then followed live over the
SpacetimeDB subscription, while every coarser interval polls the api. See
"The live 1s chart" below.

## Architecture

```
Binance public websocket
        │
        ▼
   ingestor (Rust) ──produce──▶ Redpanda (Kafka protocol)
                                    │
              ┌─────────────────────┴──────────────────────┐
              ▼ hot path                                    ▼ cold path
   hot-consumer (Rust)                            cold-consumer (Rust)
        │ reducer calls                                │ batched Parquet
        ▼                                              ▼
   SpacetimeDB (live state)                       MinIO (S3-compatible lake)
        │ live subscription                            │ DataFusion batch reads
        ▼                                              ▼
   frontend (React/Vite, Bun) ◀──────REST────── api (Rust, Axum)
```

- **Hot path:** latest market state — plus a bounded rolling window of `1s`
  candles — streamed to the browser via the SpacetimeDB TypeScript SDK.
- **Cold path:** append-only Parquet history on MinIO, queried through the
  Axum API with DataFusion.
- **The chart uses both:** `1s` seeds from the cold path once and then follows
  the hot path; coarser intervals are cold path only.

## Docker strategy

**Only infrastructure runs in containers** (Redpanda + Console, MinIO,
SpacetimeDB). The Rust services and the frontend run **natively on the host**
(`cargo run`, `bun run dev`) against the mapped ports. Containerizing our own
services is a deliberate later deployment step — do not add them to
`docker-compose.yml`.

The JS toolchain is **Bun only** — no Node.js, npm, or nvm anywhere.

## Prerequisites

- Rust stable (`rustup`) — MSRV is **1.96** (`rust-version` in the workspace
  `Cargo.toml`); CI builds on stable only, so the MSRV is declared but not yet
  enforced by a toolchain leg. Plus the wasm target for the SpacetimeDB module:
  `rustup target add wasm32-unknown-unknown`
- `cmake`, a C compiler, **and a C++ compiler** (rdkafka builds librdkafka
  from source; CMake's toolchain detection needs a C++ compiler even though
  librdkafka itself is C) — on Ubuntu, `build-essential` covers both
- `libcurl` dev headers (librdkafka's build unconditionally includes
  `curl/curl.h` regardless of build flags) — on Ubuntu, `libcurl4-openssl-dev`
- Docker with the compose plugin
- [Bun](https://bun.sh) ≥ 1.3
- [SpacetimeDB CLI](https://spacetimedb.com/install) (`spacetime`) — for
  publishing the module and generating client bindings

## Getting started

```sh
# 1. infrastructure
cp .env.example .env
cp frontend/.env.example frontend/.env
docker compose up -d          # or: make infra-up

# 2. create the market topics (6 partitions, explicit retention)
make topics

# 3. rust services (each runs until Ctrl-C — separate terminals)
cargo run -p ingestor         # streams Binance → Redpanda until Ctrl-C
make backfill                 # one-shot: REST history for each configured interval → the lake
cargo run -p hot-consumer     # market.* topics → SpacetimeDB reducers (needs module published)
cargo run -p cold-consumer    # market.* topics → batched Parquet on MinIO
cargo run -p api              # DataFusion over the lake; REST on :8081

# 4. spacetime module (SpacetimeDB runs as a compose service; state persists
#    in a volume across `docker compose down` — use `down -v` to wipe)
spacetime publish --server http://localhost:3000 \
  --module-path crates/spacetime-module projectino    # or: make module-publish

# 5. frontend (Bun only) — live view of the SpacetimeDB tables
cd frontend
bun install                   # reproducible from the committed bun.lock
bun run dev                   # http://localhost:5173
```

## Verifying each piece

| Check | How |
|---|---|
| Redpanda | `docker compose exec redpanda rpk cluster health` |
| Live events flowing | `docker compose exec redpanda rpk topic consume market.trades --num 1` |
| Redpanda Console | http://localhost:8080 |
| MinIO + bucket | http://localhost:9001 (minioadmin/minioadmin), bucket `market-lake` |
| SpacetimeDB | `curl http://localhost:3000/v1/database/projectino` (200 after publish) |
| Live state (hot path) | run `hot-consumer`, then `spacetime sql --server http://localhost:3000 projectino "SELECT symbol, price FROM live_trade"` |
| Lake files (cold path) | run `cold-consumer`, then browse http://localhost:9001 → bucket `market-lake` (Parquet under `market.trades/partition=…/`) |
| Axum API (health) | `curl http://localhost:8081/health` → `{"status":"ok"}` |
| History query (cold path) | `curl "http://localhost:8081/trades?symbol=BTCUSDT&limit=5"` (also `/book_tickers`, `/klines?symbol=…&interval=1m`) |
| Live 1s window | `spacetime sql --server http://localhost:3000 projectino "SELECT COUNT(*) AS n FROM live_kline_second"` (needs ingestor + hot-consumer; aggregates require an alias) |
| Frontend | http://localhost:5173 — live tables (hot path) + candlestick chart (cold path). Needs the `api` running for history; `1s` additionally needs ingestor + hot-consumer, and updates once a second |

## Listing freshness (`API_LISTING_TTL_MS`)

DataFusion resolves a table's set of Parquet files when the table is
*registered*, so a long-running api would otherwise never see files the
cold-consumer wrote after startup. The api therefore re-registers a table
before querying it — but that re-listing is a full `LIST` over the lake prefix,
and it grows with the lake.

Doing that per request broke any client polling faster than the response time:
the 1s chart's every response was superseded by the next request before it
landed, the frontend's stale-response guard correctly discarded it, and the
chart never rendered. Slower intervals were unaffected.

Now the listing is reused for `API_LISTING_TTL_MS` (default 3000). That bounds
how stale a query's view of the lake can be, and caps re-listing at once per
TTL however fast clients poll. Set it to `0` to re-list on every request.
Concurrent requests for the same table share one refresh rather than each
starting their own.

**Measured 2026-07-21** (`/klines?interval=1s&limit=200`, idle machine, lake
larger than the original repro; only the api running):

| | per request |
|---|---|
| `API_LISTING_TTL_MS=0` (re-list every time) | ~2.0s |
| listing cached (TTL not yet expired) | ~1.1s |

So the listing costs ~0.9s and the Parquet scan ~1.1s. Two things follow:

- An earlier note in this README put the per-request cost at 5–7s at 1,611
  files. It measures ~2.0s now on a *larger* lake, so that figure was probably
  inflated by contention — it was taken with the ingestor and cold-consumer
  running. Treat it as withdrawn.
- **The TTL alone does not fix the 1s chart.** A cached response still takes
  ~1.1s, which is longer than the 1s poll period, so responses would still be
  superseded. The self-scheduling poll is what makes it render; the TTL keeps it
  from falling further behind. That got 1s to ~2–3s per refresh — honest
  behaviour under a slow api rather than a spinner forever, but still not 1s.
  Polling was the wrong shape for this interval, which is what the live path
  below replaces it with.

## The live 1s chart (hot path)

Every interval except `1s` is served by polling the lake through the api. `1s`
is a hybrid, because ~1.1s is the *floor* for a lake query and the chart wants a
new candle every second — polling can never win that race.

Instead:

1. **Seed** — one `/klines?interval=1s` request on load, for deep history.
2. **Follow** — a SpacetimeDB subscription to `live_kline_second`, a rolling
   window of 1s candles fed by the hot path (ingestor → Redpanda →
   hot-consumer → `record_kline_second`). Push, not poll.
3. **Merge** — the two overlap; same `open_time` means the same candle and the
   live copy wins (`frontend/src/lib/candles.ts`).

Measured 2026-07-21: **10 chart updates in 10s** (one per second), against
~2–3s when polling. Network shows exactly one `/klines` request per load and
none after — the polling is genuinely gone, not just faster.

`live_kline_second` is the only append-shaped table in the module, so unlike the
upsert live-state tables it needs explicit bounding: `WINDOW_1S` (600 candles,
~10 min per symbol) with a slack margin so the trim's table scan is amortized
over many inserts rather than paid on every one. Deep history stays in the lake;
this window only has to cover the gap since a client loaded. Verified by
temporarily shrinking the window and watching the row count oscillate under the
cap instead of growing.

Only 1s is mirrored this way, deliberately — coarser intervals poll the lake
perfectly well, and keeping hours of them in a live table would defeat the point
of bounding it.

## Kline intervals (`KLINE_INTERVALS`)

Binance documents 16 intervals. Streaming all of them opens 32 kline streams for
two symbols and mostly buys waste: every interval except `1s` re-sends its
forming candle every 2s regardless of how often it actually changes, so a `1M`
candle costs the same bandwidth as a `1m` one. The real cost isn't the ~5 msg/s
— it's that each update appends a row to the Parquet lake, and lake size is what
drives `/klines` scan latency.

Unset, `KLINE_INTERVALS` defaults to `DEFAULT_INTERVALS` in
`crates/ingestor/src/config.rs` — the documented set minus `3m`/`2h`/`4h`/`8h`/`3d`:

```
1s  1m  5m  15m  30m  1h  6h  12h  1d  1w  1M
```

All 16 remain valid; set `KLINE_INTERVALS` to stream any of them. There is no
`10m`, no `3h`, and a day is `1d` not `24h` — the config rejects anything outside
Binance's documented set at startup rather than opening a dead stream.

**Two lists must agree.** `INTERVALS` in `frontend/src/lib/intervals.ts` is what
the chart offers, and it has to match `DEFAULT_INTERVALS`. Nothing checks this at
build time (one is TypeScript, the other Rust). Offering an interval the ingestor
doesn't stream renders stale lake history that never updates, with nothing on
screen explaining why. If you change one, change the other.

## Testing the ingestor

Ingestor behavior (decode/route/envelope) is tested against **recorded fixtures**,
not live Binance — fast, deterministic, and hermetic (no network, no running
services). Fixtures are newline-delimited raw websocket frames replayed through
the exact same handling path the live client uses.

```sh
make test-ingestor      # cargo test -p ingestor + replay every fixture; logs to logs/
```

Replay a single fixture with readable logs, or record a new one:

```sh
cargo run -p ingestor --bin replay -- crates/ingestor/tests/fixtures/normal-trading/stream.ndjson

# record from live traffic, then trim into tests/fixtures/<scenario>/stream.ndjson
INGESTOR_DUMP_RAW=out.ndjson cargo run -p ingestor
```

The hermetic replay assertions in `crates/ingestor/tests/replay.rs` run as part
of `cargo test --workspace`, so CI gates every push on them.

## Ports

| Service | Host port |
|---|---|
| Redpanda Kafka API (external listener) | 19092 |
| Redpanda schema registry / pandaproxy / admin | 18081 / 18082 / 9644 |
| Redpanda Console UI | 8080 |
| MinIO S3 API / console UI | 9000 / 9001 |
| SpacetimeDB | 3000 |
| Axum API | 8081 |
| Vite dev server | 5173 |

## Repo layout

```
crates/
  common/            shared types (Binance event models, symbols, config)   [implemented]
  ingestor/          Binance websocket → Kafka producer (+ REST backfill)   [implemented]
  hot-consumer/      Kafka → SpacetimeDB reducer calls                      [implemented]
  cold-consumer/     Kafka → batched Parquet on MinIO                       [implemented]
  spacetime-module/  SpacetimeDB server module (wasm)                       [implemented]
  api/               Axum + DataFusion historical query API                 [implemented]
frontend/            React + TypeScript (Vite), managed with Bun            [implemented]
```

## CI

CI is scoped to what's implemented (`.github/workflows/ci.yml`):

- **rust** — `cargo fmt --check`, `cargo clippy -D warnings`, and
  `cargo test` across the workspace, plus a
  `wasm32-unknown-unknown` build of `spacetime-module` — `cargo test` only
  host-compiles it, so this verifies its real deployable artifact.
- **supply-chain** — `cargo deny check` (advisories + licenses + bans +
  sources). Deferred advisories (from `datafusion` via the `api`, and
  `object_store`'s S3 XML parsing) are listed with rationale in `deny.toml`.
  Those deferrals were taken when `api` was a skeleton; it is implemented now,
  so they are due for review — see the TODOs.
- **compose** — `docker compose config` validates `docker-compose.yml`
  (schema/interpolation/service refs) without pulling images or starting
  containers.
- **frontend** — `bun install --frozen-lockfile`, `bun run typecheck` (against
  the generated SDK bindings), and `bun audit`. Bun only, no Node/npm.

Deferred until the relevant part exists: `cargo-audit` and an MSRV toolchain
leg. See the workflow footer.

## Known TODOs

Some are marked with `TODO` in the code, most are design notes that only make
sense at this level. Not a backlog in priority order.

- Hot-consumer delivery: currently commit-after-enqueue with fire-and-forget
  reducer calls (safe for self-healing live-state upserts). A stricter
  commit-after-apply via the SDK `_then` callbacks, batched, is a follow-up.
- Frontend polish: the candlestick chart is hand-rolled SVG (no chart library)
  with zoom/pan, a crosshair tooltip, and visible-window auto-scaling. Possible
  next steps: a volume sub-plot and a trades-history view. Candle dedup happens
  client-side because the lake is append-only (one row per kline *update*) —
  an api-side "latest per open_time" aggregation would be cheaper.
- **Historical depth, pagination, and a well-defined chart window.** Three gaps
  that are really one problem, and only make sense fixed together:
  1. *Backfill* is one-shot and always fetches the most recent N candles. It has
     no `startTime`/`endTime` paging, so it cannot walk back beyond 1000 candles
     per interval — deep history simply never enters the lake.
  2. *`/klines`* has no pagination or time-range filters. It answers "the newest
     N" and nothing else, so a client cannot ask for an earlier window. Panning
     the chart past the loaded range shows empty space rather than fetching more.
  3. *The chart* therefore displays whatever one request happened to return. What
     is on screen depends on when the page loaded, how much history the lake had
     at that moment, and — at `1s` — how much the live window has accumulated
     since. Two clients can legitimately show different candle counts for the
     same symbol and interval, and neither is wrong.

  The fix is the whole chain: backfill pages backwards to a target depth,
  `/klines` accepts an explicit time range and pages, and the chart requests a
  window it names rather than "the latest N", fetching more as the user pans.
  Until that lands, treat the displayed range as best-effort rather than a
  guarantee — it is not a bug that two sessions disagree.
- API refinements: a `Decimal128` lake schema so price aggregations don't need a
  cast.
- The chart's interval list (`INTERVALS` in `frontend/src/lib/intervals.ts`) must
  be kept in sync by hand with `DEFAULT_INTERVALS` in
  `crates/ingestor/src/config.rs` — one is TypeScript, the other Rust, and
  nothing checks them against each other at build time. Offering an interval the
  ingestor does not stream renders stale lake history that never updates, with
  nothing on screen explaining why. A CI check comparing the two lists would
  close this properly.
- Lake listing is cached behind `API_LISTING_TTL_MS` (default 3s) rather than
  re-listed per request — see "Listing freshness" above. The remaining ~1.1s per
  query is the Parquet scan itself. `1s` no longer polls at all (see "The live
  1s chart"), but every other interval still pays that scan; the api-side
  "latest per open_time" aggregation in the frontend note above is the next
  lever for those.
- The live 1s window is seeded per *client*, from the lake, on every page load.
  A client that stays open longer than `WINDOW_1S` (~10 min) and then loses its
  websocket could have a gap between what the window still holds and what the
  lake has flushed. Not observed, and a reconnect re-seeds — but the seam is
  untested.
- Regenerate SDK bindings with `make module-generate` after module schema
  changes.
- **Due now:** the `deny.toml` advisory deferrals were taken while `api` was a
  skeleton and `cold-consumer` unbuilt. Both are implemented, so the rationale
  ("only pulled in by the not-yet-implemented `api` skeleton") no longer holds
  and each deferral needs re-justifying or removing.
- Undecodable Kafka messages are logged and skipped in both consumers; they
  should be routed to a `.dlq` topic instead of dropped (marked `TODO` in
  `crates/cold-consumer/src/lib.rs` and `crates/hot-consumer/src/lib.rs`).
- Performance metrics: the replay `Stats` and the ingestor currently track only
  correctness counters (events, decode errors) — add throughput (frames/s,
  events/s) and latency (decode time, Kafka produce time) instrumentation, plus
  consumer lag / reconnect counts as structured `tracing` fields that can later
  back a metrics exporter.
- Ingestor throughput scaling: step (1) is **done** — the read loop no longer
  awaits each produce inline; `publish` queues via `send_result` and librdkafka
  batches (measured 2026-07-21: ceiling was ~130–140 msg/s with lag growing to
  ~96s; afterwards 237 msg/s sustained with lag under 1s, and the ceiling was
  never reached). Step (2) remains if a single connection is outgrown: shard
  into multiple websocket connections partitioned by symbol group. Any
  parallelism must preserve per-symbol ordering (messages are keyed by symbol).
- Unexplained: with the old awaited-send ingestor, the Binance websocket dropped
  with `Connection reset without closing handshake` at almost exactly 10 minutes,
  twice (10m02s, 9m59s). No documented limit was being approached — streams,
  connection attempts, outbound message rate and REST weight all had 30×+
  headroom. After the pipelining fix one connection ran 12.6 min without a drop,
  consistent with having been dropped as a slow consumer, but that is one
  observation against two. If it recurs, test with a single low-volume stream:
  a drop with no backlog would point at the endpoint or the network path rather
  than at us.
