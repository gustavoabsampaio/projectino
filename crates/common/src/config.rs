//! Env-var based configuration, shared by every binary.
//!
//! Deliberately hand-rolled on top of `std::env` + `dotenvy` instead of a
//! config crate — the needs here are trivial.

use anyhow::{Context, Result};

/// Load `.env` from the current directory (or any parent), if present.
/// Missing files are fine; real environment variables always win.
pub fn load_dotenv() {
    dotenvy::dotenv().ok();
}

/// Read a required environment variable, with a helpful error naming the key.
pub fn required(key: &str) -> Result<String> {
    std::env::var(key)
        .with_context(|| format!("missing required env var `{key}` (see .env.example)"))
}

/// Read an optional environment variable, falling back to `default`.
pub fn optional(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Initialize structured logging. Respects `RUST_LOG`, defaults to `info`.
pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}
