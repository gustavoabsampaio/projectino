//! Thin entrypoint: load config, run the hot-path consumer.

use anyhow::Result;
use hot_consumer::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    common::config::load_dotenv();
    common::config::init_tracing();

    let cfg = Config::from_env()?;
    hot_consumer::run(cfg).await
}
