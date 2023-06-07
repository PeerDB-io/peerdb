use anyhow::Context;
use postgres::{types, Client};
use prost::Message;
use pt::peers::{peer::Config, Peer};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

pub struct Catalog {
    pg: Box<Client>,
}

fn run_migrations(client: &mut Client) -> anyhow::Result<()> {
    let migration_report = embedded::migrations::runner()
        .run(client)
        .context("Failed to run migrations")?;
    for migration in migration_report.applied_migrations() {
        tracing::info!(
            "Migration Applied -  Name: {}, Version: {}",
            migration.name(),
            migration.version()
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct CatalogConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
}

fn get_connection_string(catalog_config: &CatalogConfig) -> String {
    let mut connection_string = String::new();
    connection_string.push_str("host=");
    connection_string.push_str(&catalog_config.host);
    connection_string.push_str(" port=");
    connection_string.push_str(&catalog_config.port.to_string());
    connection_string.push_str(" user=");
    connection_string.push_str(&catalog_config.user);
    connection_string.push_str(" password=");
    connection_string.push_str(&catalog_config.password);
    connection_string.push_str(" dbname=");
    connection_string.push_str(&catalog_config.database);
    connection_string
}

impl Catalog {
    pub fn new(catalog_config: &CatalogConfig) -> anyhow::Result<Self> {
        let connection_string = get_connection_string(catalog_config);
        let mut client = Client::connect(&connection_string, postgres::NoTls)
            .context("Failed to connect to catalog database")?;
        run_migrations(&mut client)?;
        Ok(Self {
            pg: Box::new(client),
        })
    }

    pub fn create_peer(&mut self, peer: Peer) -> anyhow::Result<i64> {
        let config_blob = {
            let config = peer.config.context("invalid peer config")?;
            let mut buf = Vec::new();

            match config {
                Config::SnowflakeConfig(snowflake_config) => {
                    let config_len = snowflake_config.encoded_len();
                    buf.reserve(config_len);
                    snowflake_config.encode(&mut buf)?;
                }
                Config::BigqueryConfig(bigquery_config) => {
                    let config_len = bigquery_config.encoded_len();
                    buf.reserve(config_len);
                    bigquery_config.encode(&mut buf)?;
                }
                Config::MongoConfig(mongo_config) => {
                    let config_len = mongo_config.encoded_len();
                    buf.reserve(config_len);
                    mongo_config.encode(&mut buf)?;
                }
                Config::PostgresConfig(postgres_config) => {
                    let config_len = postgres_config.encoded_len();
                    buf.reserve(config_len);
                    postgres_config.encode(&mut buf)?;
                }
            };

            buf
        };

        let stmt = self.pg.prepare_typed(
            "INSERT INTO peers (name, type, options) VALUES ($1, $2, $3)",
            &[types::Type::TEXT, types::Type::INT4, types::Type::BYTEA],
        )?;

        self.pg
            .execute(&stmt, &[&peer.name, &peer.r#type, &config_blob])?;

        self.get_peer_id(peer.name)
    }

    pub fn get_peer_id(&mut self, peer_name: String) -> anyhow::Result<i64> {
        let stmt = self
            .pg
            .prepare_typed("SELECT id FROM peers WHERE name = $1", &[types::Type::TEXT])?;

        let id: i32 = self
            .pg
            .query_opt(&stmt, &[&peer_name])?
            .map(|row| row.get(0))
            .context("Failed to get peer id")?;

        Ok(id as i64)
    }
}
