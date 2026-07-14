# projectino

Real-time crypto market data pipeline (portfolio project, built alongside an
AI agent).

**Status:** the **ingestor is implemented** — it streams live Binance market
data (aggTrades, bookTickers, klines) over a combined websocket stream and
produces enveloped JSON events to per-event-type Redpanda topics, keyed by
symbol, with jittered-backoff reconnects and graceful shutdown. The consumers,
API, and frontend are still connectivity skeletons.

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
cargo run -p hot-consumer     # skeleton: connects, logs, exits
cargo run -p cold-consumer    # skeleton: connects, logs, exits
cargo run -p api              # stays up, serves GET /health on :8081

# 4. spacetime module (SpacetimeDB runs as a compose service; state persists
#    in a volume across `docker compose down` — use `down -v` to wipe)
spacetime publish --server http://localhost:3000 \
  --module-path crates/spacetime-module projectino    # or: make module-publish

# 5. frontend (Bun only)
cd frontend
bun install                   # first run: commit the generated bun.lock
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
| Axum API | `curl http://localhost:8081/health` → `{"status":"ok"}` |
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
  hot-consumer/      Kafka → SpacetimeDB reducer calls                      [skeleton]
  cold-consumer/     Kafka → batched Parquet on MinIO                       [skeleton]
  spacetime-module/  SpacetimeDB server module (wasm)                       [skeleton]
  api/               Axum + DataFusion historical query API                [skeleton]
frontend/            React + TypeScript (Vite), managed with Bun            [skeleton]
```

## CI

CI is scoped to what's implemented (`.github/workflows/ci.yml`):

- **rust** — `cargo fmt --check`, `cargo clippy -D warnings`, and
  `cargo test` across the workspace (skeletons must still compile).
- **supply-chain** — `cargo deny check` (advisories + licenses + bans +
  sources). Deferred advisories, all from dependencies of the not-yet-built
  `api`/`cold-consumer` skeletons, are listed with rationale in `deny.toml`.

Deferred until the relevant part exists: the frontend job
(`bun install --frozen-lockfile` + `bun audit`, needs a committed `bun.lock`),
`cargo-audit`, and an MSRV toolchain leg. See the workflow footer.

## Known TODOs (marked in code)

- Generate SpacetimeDB client bindings (`make module-generate`) and replace
  the placeholder status checks with real connections (frontend service and
  hot-consumer).
- Cold-consumer Parquet writing, API lake queries, real module tables.
- Commit `bun.lock` (first `bun install`) and re-enable the frontend CI job.
- Revisit the deferred `deny.toml` advisories as `api`/`cold-consumer` are built.
