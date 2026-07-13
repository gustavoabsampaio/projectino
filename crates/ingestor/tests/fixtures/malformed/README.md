# Fixture: malformed

Exercises resilience: a mix of valid and broken frames proving the ingestor
**logs and skips** bad input rather than panicking or tearing down the stream.

- **File:** `stream.ndjson` — hand-authored (not recorded), 5 frames:
  1. valid aggTrade
  2. unknown stream (`@depth`) → `UnknownStream` error
  3. truncated/invalid JSON → parse error
  4. bookTicker with a non-decimal price → parse error
  5. valid kline
- **Expected replay result:** 2 events (1 aggTrade, 1 kline), **3 decode
  errors**, no panic.

Because decode errors are expected here, replay must allow them:

```sh
cargo run -p ingestor --bin replay -- \
  crates/ingestor/tests/fixtures/malformed/stream.ndjson --max-decode-errors 3
```
