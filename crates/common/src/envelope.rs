//! Message envelope wrapping every payload produced to Kafka.
//!
//! Consumers branch on `event_type`/`schema_version` explicitly instead of
//! relying on topic identity alone — the standard pattern for topics whose
//! logical shape may evolve (see the `kafka-schema-conventions` skill).

use serde::{Deserialize, Serialize};

/// Current version of the on-the-wire message schemas. Bump when a `common`
/// event struct changes shape; consumers use this to branch on decode.
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub event_type: String,
    pub schema_version: u32,
    pub payload: T,
}

impl<T> Envelope<T> {
    pub fn new(event_type: impl Into<String>, payload: T) -> Self {
        Self {
            event_type: event_type.into(),
            schema_version: SCHEMA_VERSION,
            payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips_and_carries_version() {
        let env = Envelope::new("agg_trade", vec![1u32, 2, 3]);
        let json = serde_json::to_string(&env).expect("envelope must serialize");
        assert!(json.contains(r#""event_type":"agg_trade""#));
        assert!(json.contains(r#""schema_version":1"#));
        let back: Envelope<Vec<u32>> = serde_json::from_str(&json).expect("must round-trip");
        assert_eq!(back, env);
    }
}
