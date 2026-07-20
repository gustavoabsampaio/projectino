//! Thin entrypoint: load config, run the historical-query API.

use anyhow::Result;
use api::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    common::config::load_dotenv();
    common::config::init_tracing();

    let cfg = Config::from_env()?;
    api::run(cfg).await
}
