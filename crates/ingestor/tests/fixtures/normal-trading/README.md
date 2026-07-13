# Fixture: normal-trading

Baseline "nothing broke" scenario — a short slice of real Binance traffic where
every frame is expected to decode cleanly.

- **File:** `stream.ndjson` — newline-delimited raw combined-stream frames
  (`{"stream":"<name>","data":{...}}`), exactly as received from the websocket.
- **Recorded:** 2026-07-13, via `INGESTOR_DUMP_RAW=… cargo run -p ingestor`
  against `wss://data-stream.binance.vision`, symbols `btcusdt,ethusdt`,
  kline interval `1m`. First 150 frames of the session.
- **Contents:** 23 aggTrade, 125 bookTicker, 2 kline frames.
- **Schema:** raw upstream Binance payloads (pre-envelope). Decoded by
  `common::events`; the pipeline envelope is `schema_version = 1`. If
  `common::events` field mappings change, re-record or migrate this fixture.

Replay it:

```sh
cargo run -p ingestor --bin replay -- crates/ingestor/tests/fixtures/normal-trading/stream.ndjson
```
