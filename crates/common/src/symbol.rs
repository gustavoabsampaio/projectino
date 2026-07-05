//! Trading symbols tracked by the pipeline.

use serde::{Deserialize, Serialize};

/// The symbols this pipeline knows about.
///
/// TODO: extend as needed; keep in sync with the `SYMBOLS` env var.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Symbol {
    BtcUsdt,
    EthUsdt,
}

#[derive(Debug, thiserror::Error)]
#[error("unknown symbol: {0}")]
pub struct UnknownSymbol(String);

impl Symbol {
    /// Lowercase form used in Binance stream names (e.g. `btcusdt@aggTrade`).
    pub fn as_stream_symbol(self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "btcusdt",
            Symbol::EthUsdt => "ethusdt",
        }
    }

    /// Uppercase form used in Binance payloads (e.g. `"s": "BTCUSDT"`).
    pub fn as_exchange_symbol(self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "BTCUSDT",
            Symbol::EthUsdt => "ETHUSDT",
        }
    }
}

impl std::str::FromStr for Symbol {
    type Err = UnknownSymbol;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "btcusdt" => Ok(Symbol::BtcUsdt),
            "ethusdt" => Ok(Symbol::EthUsdt),
            other => Err(UnknownSymbol(other.to_string())),
        }
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_exchange_symbol())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_case_insensitively() {
        assert_eq!("BTCUSDT".parse::<Symbol>().unwrap(), Symbol::BtcUsdt);
        assert_eq!("ethusdt".parse::<Symbol>().unwrap(), Symbol::EthUsdt);
        assert!("dogeusdt".parse::<Symbol>().is_err());
    }
}
