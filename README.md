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

# 4. spacetime module
spacetime publish --server http://localhost:3000 \
  --project-path crates/spacetime-module projectino   # or: make module-publish

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
  common/            shared types (Binance event models, symbols, config)
  ingestor/          Binance websocket → Kafka producer
  hot-consumer/      Kafka → SpacetimeDB reducer calls
  cold-consumer/     Kafka → batched Parquet on MinIO
  spacetime-module/  SpacetimeDB server module (wasm)
  api/               Axum + DataFusion historical query API
frontend/            React + TypeScript (Vite), managed with Bun
```

## Known TODOs (marked in code)

- Generate SpacetimeDB client bindings (`make module-generate`) and replace
  the placeholder status checks with real connections (frontend service and
  hot-consumer).
- Cold-consumer Parquet writing, API lake queries, real module tables.
- `bun.lock` must be committed after the first `bun install` (CI installs
  with `--frozen-lockfile`).
