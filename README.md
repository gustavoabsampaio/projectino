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
- The lake stores prices/quantities as native `Decimal128(38, 8)` (done
  2026-07-24) so analytical queries aggregate them without a cast. Scale 8 is
  lossless for Binance payloads; the api casts the column back to a string at
  the JSON boundary so the wire format stays exact-decimal-as-string. **Breaking
  migration:** this changed the Parquet column type, so a lake holding older
  `Utf8` files will fail DataFusion's schema unification when queried alongside
  new files. Reset the lake bucket before running the new cold-consumer against
  old data — `make lake-reset` (empties `market-lake`), then re-run
  `make backfill` and the cold-consumer to repopulate.
  - The `Decimal128 → Utf8` cast in `batches_to_json` is **intentional, not a
    cleanup target.** The data is numeric in the lake and text on the wire, so
    the conversion has to live somewhere; placing it at the JSON boundary casts
    only the ≤`MAX_LIMIT` returned rows, versus casting every *scanned* row if
    the lake were `Utf8` and a future aggregation had to `CAST` back to decimal.
    It is also dwarfed by the ~1.1s Parquet scan. Leave it be.
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
- The `deny.toml` advisory deferrals were re-justified 2026-07-24 now that
  `api` and `cold-consumer` are implemented (they no longer rest on the old
  "not-yet-implemented skeleton" reasoning). None are removable yet: `paste`
  (RUSTSEC-2024-0436) is an unmaintained-only advisory with no patched release,
  and the two `quick-xml` DoS advisories (RUSTSEC-2026-0194/0195) are fixed in
  0.41 but we pin 0.39.4 transitively via `object_store` — and only ever parse
  our own local MinIO's XML, never attacker input. Revisit on the next
  `object_store` bump. See the comments in `deny.toml` for the full rationale.
- Dead-letter routing. **Done for both consumers (2026-07-24).** Undecodable
  messages (malformed JSON / unknown `event_type`) are permanent poison pills,
  routed to the topic's `.dlq` sibling — original bytes as payload, error reason
  + source coordinates as headers — instead of being dropped. `.dlq` topics are
  created by `make topics` (1-day retention).
  - *Hot-consumer* additionally distinguishes a reducer *enqueue* failure as
    potentially transient (SpacetimeDB briefly down): retried with capped
    backoff and never committed until it applies, so a persistent failure stops
    the consumer with the offset uncommitted rather than dropping valid data.
  - *Cold-consumer* has no transient class — a Parquet flush failure is already
    handled by not committing the batch, so those messages replay. Because it
    commits offsets in batches (`max+1` per partition), a poison pill is
    dead-lettered *at decode time*, awaiting delivery before a later flush can
    commit past it. A poison pill isolated in a partition with no later message
    may be re-dead-lettered on restart until a good message advances the commit;
    DLQ duplicates are acceptable (visibility, not reprocessing automation).
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

### From an architecture/code review (2026-07-24)

The items below came out of a read-through of the whole workspace, not from
running into them in production. Roughly ordered by impact; none are addressed
yet. Some sharpen a lever already named above rather than adding a new one.

- **Lake partitioning does not match the query patterns — this is the real
  driver of `/klines` and `/trades` scan latency, not a fixed cost.** The
  cold-consumer partitions the lake by *Kafka partition*
  (`{topic}/partition={kafka_partition}/…` in `crates/cold-consumer/src/lib.rs`),
  which is an ingestion artifact, not a query dimension. Every api query filters
  by `symbol` (and `interval` for klines) and sorts by time, but nothing in the
  file layout lets DataFusion prune to the relevant files — so
  `/trades?symbol=BTCUSDT` scans *every* symbol's trades, sorts the lot, then
  applies `LIMIT`. That is the ~1.1s scan, and it grows with total lake size
  without bound. Hive-style partitioning (`symbol=…/interval=…/date=…`) would let
  DataFusion prune to a handful of files per query, and would make the
  deep-history/time-range work above prunable rather than a full scan. This is a
  larger lever than the api-side "latest per open_time" aggregation noted
  earlier. **Breaking:** it changes the lake layout, so reset the bucket
  (`make lake-reset`) and repopulate, same as the `Decimal128` migration above.
- **No lake compaction and no lake retention.** Each flush writes one small
  Parquet file per (topic, partition) every `COLD_BATCH_MAX_ROWS`/`COLD_FLUSH_SECS`,
  so the lake accumulates thousands of tiny files. Small files inflate both the
  `LIST` (O(files) — the cost `API_LISTING_TTL_MS` exists to amortize) and the
  scan (per-file footer/open overhead), and nothing merges them. Separately, the
  lake has no lifecycle policy: the Kafka topics expire after 3 days but the lake
  grows forever, so LIST and scan cost climb without bound over time. A periodic
  compaction step (merge small files into larger ones) plus a date-partition and
  an object-store lifecycle rule would bound both.
- **Cold-consumer flush blocks the runtime and the consume loop.**
  `lake::to_parquet` is CPU-bound synchronous encoding run directly on the async
  task, and the per-partition `store.put(...)` uploads are awaited one at a time
  (`crates/cold-consumer/src/lib.rs`). While a flush runs, `consumer.recv()` is
  not polled, so ingestion stalls for the whole encode + upload. Wrapping the
  encode in `spawn_blocking` and uploading the partition files concurrently
  (`try_join_all`) would keep the reactor and the consume loop moving. Not
  visible at single-symbol dev volume; a throughput ceiling as symbols/rate grow.
- **Ingestor spawns a task per delivered message.** `publish`
  (`crates/ingestor/src/handler.rs`) does `tokio::spawn` for every successfully
  queued record purely to observe its delivery report and count failures — about
  one spawn per message at the sustained rate. A custom rdkafka `ProducerContext`
  with a `delivery` callback would tally failures with no per-message future.
  Functionally correct today (the spawns are bounded by the queue-full
  backpressure), but churny.
- **Hot-consumer commits every message and processes serially.** It calls
  `commit_message(..., Async)` per message (`crates/hot-consumer/src/lib.rs`);
  given the self-healing upsert tables, periodic commits (every N messages or on
  a timer) would cut OffsetCommit traffic with no correctness loss, since replay
  is already safe. The loop is also a single serial task across all three
  topics/partitions — fine at the measured rate, but the only path to more is the
  same websocket/consumer sharding noted for the ingestor.
- **`fresh_table` releases its lock before taking the table handle.** The
  per-table `Mutex` meant to collapse concurrent refreshes drops at the end of
  the refresh block, then `ctx.table(name)` runs outside it
  (`crates/api/src/lib.rs`), so a concurrent refresh can deregister/re-register
  on the shared `SessionContext` in the gap. Low-probability and likely benign,
  but the "one refresh, shared" invariant does not actually hold to the point of
  use.
- **`/klines` and `/trades` return mixed-meaning rows when `interval`/`symbol`
  are omitted.** With no `interval`, the klines handler returns the newest N
  closed candles *across all intervals interleaved* plus one forming candle
  across all intervals; `/trades` with no `symbol` interleaves symbols. The
  frontend always passes both, so it is latent — but the endpoints should reject
  the ambiguous case (400) rather than answer with nonsense.
- **The api echoes internal error strings to clients.**
  `AppError::into_response` puts `err.to_string()` in the JSON body
  (`crates/api/src/lib.rs`), disclosing DataFusion/object-store internals. Fine
  for a local portfolio API; a generic 500 body plus the existing server-side log
  line is the usual split before this is exposed anywhere.
- **Empty-payload messages are skipped without dead-lettering or advancing the
  offset.** Both consumers `warn` and continue on a `None` payload without
  committing past it, so one at a partition tail re-warns on every restart (like
  the acknowledged poison-pill-at-tail case, but not even parked in the DLQ).
  "Shouldn't happen from the ingestor," so low priority — noted for completeness
  alongside the DLQ behaviour above.
- **`Symbol` is a closed two-variant enum.** `common::symbol::Symbol` hardcodes
  `BtcUsdt`/`EthUsdt` with hand-written `FromStr`/`as_*` arms, and the frontend
  hardcodes the same list (`frontend/src/App.tsx`), so adding a symbol is a
  multi-site code change despite `SYMBOLS` being an env var. A validated newtype
  over the uppercase/lowercase string pair (or pulling the exchange symbol list)
  would make the configurability real.
