use std::time::Duration;

use anyhow::{Context, ensure};
use clap::Parser;
use tokio_postgres::NoTls;

#[derive(Parser)]
#[command(about = "PeerDB catalog migrations (built without the SQL server)")]
struct Args {
    #[clap(long, default_value = "localhost", env = "PEERDB_CATALOG_HOST")]
    catalog_host: String,
    #[clap(long, default_value_t = 5432, env = "PEERDB_CATALOG_PORT")]
    catalog_port: u16,
    #[clap(long, default_value = "postgres", env = "PEERDB_CATALOG_USER")]
    catalog_user: String,
    #[clap(long, default_value = "postgres", env = "PEERDB_CATALOG_PASSWORD")]
    catalog_password: String,
    #[clap(long, default_value = "postgres", env = "PEERDB_CATALOG_DATABASE")]
    catalog_database: String,
    #[clap(long, env = "PEERDB_MIGRATIONS_ONLY")]
    migrations_only: bool,
    #[clap(long, env = "PEERDB_MIGRATIONS_TARGET")]
    migrations_target: Option<i32>,
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let args = Args::parse();
    ensure!(
        args.migrations_only,
        "This build requires --migrations-only"
    );
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut config = tokio_postgres::Config::new();
    config
        .host(&args.catalog_host)
        .port(args.catalog_port)
        .user(&args.catalog_user)
        .password(&args.catalog_password)
        .dbname(&args.catalog_database)
        .connect_timeout(Duration::from_secs(10));

    for attempt in 1..=3 {
        match config.connect(NoTls).await {
            Ok((mut client, connection)) => {
                tokio::spawn(async move {
                    if let Err(err) = connection.await {
                        tracing::error!("Catalog connection failed: {err}");
                    }
                });
                return catalog::migrations::run(&mut client, args.migrations_target).await;
            }
            Err(err) if attempt < 3 => {
                tracing::warn!("Failed to connect to catalog; retrying in 30 seconds: {err}");
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            Err(err) => return Err(err).context("Failed to connect to catalog"),
        }
    }
    unreachable!()
}
