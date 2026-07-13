//! Hermetic replay regression test: runs committed fixtures through the real
//! decode/handle path with no network or running services. Kept fast so it
//! gates every push via the existing `cargo test` job (see the
//! `replay-testing-harness` skill).

use std::path::PathBuf;

use ingestor::handler::{Sink, Stats, handle_frame};

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// Replay every non-empty line of a fixture through the null sink.
async fn replay(scenario: &str) -> Stats {
    let path = fixtures_dir().join(scenario).join("stream.ndjson");
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("reading fixture {}: {e}", path.display()));
    let mut stats = Stats::default();
    let sink = Sink::Null;
    for line in contents.lines().filter(|l| !l.trim().is_empty()) {
        handle_frame(line, &sink, &mut stats)
            .await
            .expect("null sink never errors");
    }
    stats
}

#[tokio::test]
async fn normal_trading_decodes_cleanly() {
    let stats = replay("normal-trading").await;
    // Baseline "nothing broke": every recorded frame decodes, and the fixture
    // actually exercised all three event types.
    assert_eq!(stats.decode_errors, 0, "unexpected decode errors: {stats}");
    assert!(stats.events() > 0, "fixture produced no events: {stats}");
    assert!(stats.agg_trades > 0, "no aggTrades in fixture: {stats}");
    assert!(stats.book_tickers > 0, "no bookTickers in fixture: {stats}");
    assert!(stats.klines > 0, "no klines in fixture: {stats}");
}

#[tokio::test]
async fn malformed_frames_are_skipped_not_fatal() {
    // The whole point: bad frames must be counted and skipped, and the valid
    // frames around them must still decode. Reaching this assertion at all
    // proves no panic tore down the replay.
    let stats = replay("malformed").await;
    assert_eq!(
        stats.decode_errors, 3,
        "expected exactly 3 bad frames: {stats}"
    );
    assert_eq!(stats.events(), 2, "valid frames should survive: {stats}");
    assert_eq!(stats.agg_trades, 1, "{stats}");
    assert_eq!(stats.klines, 1, "{stats}");
}
