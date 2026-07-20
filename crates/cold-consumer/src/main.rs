//! Thin entrypoint: load config, run the cold-path consumer.

use anyhow::Result;
use cold_consumer::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    common::config::load_dotenv();
    common::config::init_tracing();

    let cfg = Config::from_env()?;
    cold_consumer::run(cfg).await
}
