//! Replay a recorded fixture through the ingestor's real decode/handle path.
//!
//! Hermetic: reads newline-delimited raw combined-stream frames from a fixture
//! file and feeds each through the same [`handle_frame`] the live client uses,
//! with a null sink (no Kafka, no network). Prints a readable per-run summary
//! and exits non-zero if decode errors exceed the allowed threshold — so it
//! doubles as a regression gate. See the `replay-testing-harness` skill.
//!
//! Usage:
//!   replay <fixture-path> [--max-decode-errors N]   (N defaults to 0)

use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use ingestor::handler::{FrameAction, Sink, Stats, handle_frame};
use tracing::{info, warn};

struct Args {
    fixture: String,
    max_decode_errors: u64,
}

fn parse_args() -> Result<Args> {
    let mut fixture = None;
    let mut max_decode_errors = 0u64;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--max-decode-errors" => {
                let value = args
                    .next()
                    .context("--max-decode-errors requires a value")?;
                max_decode_errors = value
                    .parse()
                    .with_context(|| format!("invalid --max-decode-errors `{value}`"))?;
            }
            "-h" | "--help" => {
                println!("usage: replay <fixture-path> [--max-decode-errors N]");
                std::process::exit(0);
            }
            other if fixture.is_none() => fixture = Some(other.to_string()),
            other => bail!("unexpected argument `{other}`"),
        }
    }
    Ok(Args {
        fixture: fixture.context("missing <fixture-path> argument")?,
        max_decode_errors,
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<ExitCode> {
    // Readable, human-friendly log format (compact, no target column).
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();

    let args = parse_args()?;
    let contents = std::fs::read_to_string(&args.fixture)
        .with_context(|| format!("reading fixture {}", args.fixture))?;

    info!(fixture = %args.fixture, "replay starting");
    let mut stats = Stats::default();
    let sink = Sink::Null;
    let mut lines = 0u64;
    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        lines += 1;
        if let FrameAction::ServerShutdown = handle_frame(line, &sink, &mut stats).await? {
            info!(line = index + 1, "serverShutdown frame in fixture");
        }
    }

    info!(lines, %stats, "replay complete");

    if stats.events() == 0 {
        warn!("fixture produced no market events — is it empty or the wrong format?");
    }
    if stats.decode_errors > args.max_decode_errors {
        bail!(
            "{} decode error(s) exceed the allowed maximum of {}",
            stats.decode_errors,
            args.max_decode_errors
        );
    }
    Ok(ExitCode::SUCCESS)
}
