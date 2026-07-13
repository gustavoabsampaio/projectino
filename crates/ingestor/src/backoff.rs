//! Jittered exponential backoff for reconnects.
//!
//! "Equal jitter": each delay is `ceiling/2 + rand(0..=ceiling/2)` where the
//! ceiling doubles per attempt up to a cap. The random half prevents
//! synchronized thundering-herd reconnects; the fixed half guarantees we never
//! spin-reconnect (Binance caps connection attempts at 300 per 5 minutes/IP).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct Backoff {
    base: Duration,
    cap: Duration,
    attempt: u32,
}

impl Backoff {
    pub fn new(base: Duration, cap: Duration) -> Self {
        Self {
            base,
            cap,
            attempt: 0,
        }
    }

    /// The next delay to sleep before reconnecting.
    pub fn next_delay(&mut self) -> Duration {
        let ceiling = self
            .base
            .saturating_mul(2u32.saturating_pow(self.attempt))
            .min(self.cap);
        self.attempt = self.attempt.saturating_add(1);
        let half = ceiling / 2;
        half + half.mul_f64(random_fraction())
    }

    /// Call after a connection proved healthy (lived long enough) so the next
    /// failure starts from the base delay again.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

/// Cheap non-cryptographic jitter source; hand-rolled from the clock instead
/// of pulling in `rand` for a single call site (supply-chain minimalism).
fn random_fraction() -> f64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    f64::from(nanos) / 1_000_000_000_f64
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: Duration = Duration::from_secs(1);
    const CAP: Duration = Duration::from_secs(60);

    #[test]
    fn delays_stay_within_equal_jitter_bounds() {
        let mut backoff = Backoff::new(BASE, CAP);
        // Deterministic ceilings: 1s, 2s, 4s, ... capped at 60s.
        for attempt in 0..10 {
            let ceiling = BASE.saturating_mul(2u32.pow(attempt)).min(CAP);
            let delay = backoff.next_delay();
            assert!(
                delay >= ceiling / 2,
                "attempt {attempt}: {delay:?} below half-ceiling"
            );
            assert!(
                delay <= ceiling,
                "attempt {attempt}: {delay:?} above ceiling"
            );
        }
    }

    #[test]
    fn caps_at_configured_maximum() {
        let mut backoff = Backoff::new(BASE, CAP);
        for _ in 0..64 {
            // Would overflow without saturation; must never exceed the cap.
            assert!(backoff.next_delay() <= CAP);
        }
    }

    #[test]
    fn reset_returns_to_base() {
        let mut backoff = Backoff::new(BASE, CAP);
        for _ in 0..6 {
            backoff.next_delay();
        }
        backoff.reset();
        assert!(backoff.next_delay() <= BASE);
    }
}
