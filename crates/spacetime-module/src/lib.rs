//! Minimal SpacetimeDB module: one placeholder table and one placeholder
//! reducer, enough to `spacetime publish` and prove the toolchain works.
//!
//! Publish (server from docker-compose, listening on localhost:3000):
//!   spacetime publish --server http://localhost:3000 \
//!     --project-path crates/spacetime-module projectino
//!
//! TODO: replace with real live-state tables (trades, tickers, candles) and
//! reducers called by the hot-consumer.

use spacetimedb::{ReducerContext, Table, reducer, table};

#[table(accessor = heartbeat, public)]
pub struct Heartbeat {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub note: String,
}

#[reducer(init)]
pub fn init(_ctx: &ReducerContext) {
    log::info!("projectino module initialized");
}

/// Placeholder reducer: records a note in the heartbeat table.
#[reducer]
pub fn ping(ctx: &ReducerContext, note: String) {
    // id 0 is the auto_inc sentinel; the database assigns the real id.
    ctx.db.heartbeat().insert(Heartbeat { id: 0, note });
    log::info!("ping recorded");
}
