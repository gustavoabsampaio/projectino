# projectino

Real-time crypto market data pipeline (portfolio project, built alongside an
AI agent).

**Status:** the **ingestor is implemented** — it streams live Binance market
data (aggTrades, bookTickers, klines) over a combined websocket stream and
produces enveloped JSON events to per-event-type Redpanda topics, keyed by
symbol, with jittered-backoff reconnects and graceful shutdown.

The **hot path is complete, end to end**: the `hot-consumer` reads those topics
and calls SpacetimeDB reducers to maintain live-state tables (latest trade /
book ticker / candle per symbol), and the **React frontend subscribes to them
live** via the SpacetimeDB TS SDK — Binance ticks show up in the browser in real
time.

The **cold path is complete too**: the `cold-consumer` batches events into Arrow
and writes partitioned Parquet to the MinIO lake (deterministic, idempotent
filenames; offsets committed only after upload), and the `api` crate serves
historical queries over that lake with **DataFusion** — registering the Parquet
as tables and answering typed REST endpoints (`/trades`, `/book_tickers`,
`/klines`). All that remains is wiring history charts into the frontend.

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

- **Hot path:** latest market state, streamed to the browser via the
  SpacetimeDB TypeScript SDK.
- **Cold path:** append-only Parquet history on MinIO, queried through the
  Axum API with DataFusion.

## Docker strategy

**Only infrastructure runs in containers** (Redpanda + Console, MinIO,
SpacetimeDB). The Rust services and the frontend run **natively on the host**
(`cargo run`, `bun run dev`) against the mapped ports. Containerizing our own
services is a deliberate later deployment step — do not add them to
`docker-compose.yml`.

The JS toolchain is **Bun only** — no Node.js, npm, or nvm anywhere.

## Prerequisites

- Rust stable (`rustup`), plus the wasm target for the SpacetimeDB module:
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

# 3. rust services
cargo run -p ingestor         # streams Binance → Redpanda until Ctrl-C
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
| Frontend | http://localhost:5173 — prints SpacetimeDB status on page & console |

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
  ingestor/          Binance websocket → Kafka producer                     [implemented]
  hot-consumer/      Kafka → SpacetimeDB reducer calls                      [implemented]
  cold-consumer/     Kafka → batched Parquet on MinIO                       [implemented]
  spacetime-module/  SpacetimeDB server module (wasm)                       [skeleton]
  api/               Axum + DataFusion historical query API                [implemented]
frontend/            React + TypeScript (Vite), managed with Bun            [implemented]
```

## CI

CI is scoped to what's implemented (`.github/workflows/ci.yml`):

- **rust** — `cargo fmt --check`, `cargo clippy -D warnings`, and
  `cargo test` across the workspace (skeletons must still compile), plus a
  `wasm32-unknown-unknown` build of `spacetime-module` — `cargo test` only
  host-compiles it, so this verifies its real deployable artifact.
- **supply-chain** — `cargo deny check` (advisories + licenses + bans +
  sources). Deferred advisories (from `datafusion` in the `api` skeleton, and
  `object_store`'s S3 XML parsing) are listed with rationale in `deny.toml`.
- **compose** — `docker compose config` validates `docker-compose.yml`
  (schema/interpolation/service refs) without pulling images or starting
  containers.
- **frontend** — `bun install --frozen-lockfile`, `bun run typecheck` (against
  the generated SDK bindings), and `bun audit`. Bun only, no Node/npm.

Deferred until the relevant part exists: `cargo-audit` and an MSRV toolchain
leg. See the workflow footer.

## Known TODOs (marked in code)

- Hot-consumer delivery: currently commit-after-enqueue with fire-and-forget
  reducer calls (safe for self-healing live-state upserts). A stricter
  commit-after-apply via the SDK `_then` callbacks, batched, is a follow-up.
- Frontend history: wire charts fed by the `api` endpoints (`/trades`,
  `/klines`, …) alongside the live tables. The api will need CORS for the
  browser origin, and prices are strings (cast in queries as needed).
- API refinements: pagination / time-range filters; a `Decimal128` lake schema
  so price aggregations don't need a cast; lazy table (re)registration so a
  restart isn't needed when the first data lands.
- Regenerate SDK bindings with `make module-generate` after module schema
  changes.
- Revisit the deferred `deny.toml` advisories as `api`/`cold-consumer` are built.
- Performance metrics: the replay `Stats` and the ingestor currently track only
  correctness counters (events, decode errors) — add throughput (frames/s,
  events/s) and latency (decode time, Kafka produce time) instrumentation, plus
  consumer lag / reconnect counts as structured `tracing` fields that can later
  back a metrics exporter.
- Ingestor throughput scaling: the read loop awaits each Kafka produce inline
  (`publish().await` in `ingestor::lib`), which caps throughput at ~1/ack-latency
  and limits librdkafka batching — fine at current scale, a bottleneck as streams
  grow (bookTicker dominates). When the metrics above show the ceiling: (1)
  decouple produce from read via a *bounded* mpsc channel or `FuturesUnordered`
  (keep bounded for backpressure), then (2) shard into multiple websocket
  connections partitioned by symbol group. Any parallelism must preserve
  per-symbol ordering (messages are keyed by symbol).
