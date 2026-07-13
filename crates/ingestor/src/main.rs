//! Thin entrypoint: install the TLS crypto provider, load config, run.

use anyhow::Result;
use ingestor::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    // Must run before any TLS connection (tokio-tungstenite's rustls feature
    // pulls in rustls without selecting a crypto backend on its own).
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("installing the rustls ring crypto provider should only be attempted once");

    common::config::load_dotenv();
    common::config::init_tracing();

    let cfg = Config::from_env()?;
    ingestor::run(cfg).await
}
