#!/usr/bin/env bash
#
# Ingestor test runner: unit/integration tests + fixture replays, all hermetic
# (no network, no Redpanda/SpacetimeDB). Writes a readable, timestamped log to
# logs/ while also printing to the terminal.
#
# Usage: scripts/test-ingestor.sh
#
# Record a new fixture from live traffic instead (dev only):
#   INGESTOR_DUMP_RAW=out.ndjson cargo run -p ingestor
#   # then trim `out.ndjson` into crates/ingestor/tests/fixtures/<name>/stream.ndjson

set -euo pipefail

cd "$(dirname "$0")/.."

FIXTURES_DIR="crates/ingestor/tests/fixtures"
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/ingestor-test-$(date +%Y%m%d-%H%M%S).log"

# Everything below is both shown and appended to $LOG_FILE.
exec > >(tee -a "$LOG_FILE") 2>&1

# Colours only when attached to a terminal (keeps the log file clean).
if [[ -t 1 ]]; then
  BOLD=$'\e[1m'; GREEN=$'\e[32m'; RED=$'\e[31m'; BLUE=$'\e[34m'; DIM=$'\e[2m'; RESET=$'\e[0m'
else
  BOLD=""; GREEN=""; RED=""; BLUE=""; DIM=""; RESET=""
fi

section() { echo; echo "${BOLD}${BLUE}==> $*${RESET}"; }
pass()    { echo "${GREEN}PASS${RESET} $*"; }
fail()    { echo "${RED}FAIL${RESET} $*"; }

FAILURES=0

echo "${BOLD}Ingestor test run${RESET} ${DIM}$(date -Is)${RESET}"
echo "${DIM}log: $LOG_FILE${RESET}"

section "Unit + integration tests (cargo test -p ingestor)"
if cargo test -p ingestor --locked; then
  pass "cargo test"
else
  fail "cargo test"; FAILURES=$((FAILURES + 1))
fi

# Build the replay tool once so per-fixture runs are fast and quiet.
section "Building replay tool"
cargo build -p ingestor --bin replay --locked

REPLAY="./target/debug/replay"

# Replay each fixture. Fixtures whose name starts with "malformed" are expected
# to contain bad frames, so they get a non-zero decode-error budget.
run_replay() {
  local name="$1" max_errors="$2"
  section "Replay fixture: ${name} ${DIM}(max-decode-errors=${max_errors})${RESET}"
  if RUST_LOG="${RUST_LOG:-info}" "$REPLAY" \
      "$FIXTURES_DIR/$name/stream.ndjson" --max-decode-errors "$max_errors"; then
    pass "replay $name"
  else
    fail "replay $name"; FAILURES=$((FAILURES + 1))
  fi
}

for dir in "$FIXTURES_DIR"/*/; do
  name="$(basename "$dir")"
  [[ -f "$dir/stream.ndjson" ]] || continue
  case "$name" in
    malformed*) run_replay "$name" 3 ;;
    *)          run_replay "$name" 0 ;;
  esac
done

section "Summary"
if [[ "$FAILURES" -eq 0 ]]; then
  echo "${GREEN}${BOLD}All ingestor checks passed.${RESET}"
  echo "${DIM}full log: $LOG_FILE${RESET}"
  exit 0
else
  echo "${RED}${BOLD}${FAILURES} check(s) failed.${RESET}"
  echo "${DIM}full log: $LOG_FILE${RESET}"
  exit 1
fi
