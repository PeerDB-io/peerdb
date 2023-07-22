use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Context};
use peer_cursor::QueryExecutor;
use peer_postgres::PostgresQueryExecutor;
use postgres_connection::{connect_postgres, get_pg_connection_string};
use prost::Message;
use pt::{
    flow_model::{FlowJob, QRepFlowJob},
    peerdb_peers::PostgresConfig,
    peerdb_peers::{peer::Config, DbType, Peer},
};
use tokio_postgres::{types, Client};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

pub struct Catalog {
    pg: Box<Client>,
    executor: Arc<Box<dyn QueryExecutor>>,
}

async fn run_migrations(client: &mut Client) -> anyhow::Result<()> {
    let migration_report = embedded::migrations::runner()
        .run_async(client)
        .await
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

#[derive(Debug, Clone)]
pub struct WorkflowDetails {
    pub workflow_id: String,
    pub source_peer: pt::peerdb_peers::Peer,
    pub destination_peer: pt::peerdb_peers::Peer,
}

impl CatalogConfig {
    pub fn new(host: String, port: u16, user: String, password: String, database: String) -> Self {
        Self {
            host,
            port,
            user,
            password,
            database,
        }
    }

    // convert catalog config to PostgresConfig
    pub fn to_postgres_config(&self) -> pt::peerdb_peers::PostgresConfig {
        PostgresConfig {
            host: self.host.clone(),
            port: self.port as u32,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
        }
    }

    pub fn to_pg_connection_string(&self) -> String {
        get_pg_connection_string(&self.to_postgres_config())
    }
}

impl Catalog {
    pub async fn new(catalog_config: &CatalogConfig) -> anyhow::Result<Self> {
        let pt_config = catalog_config.to_postgres_config();
        let client = connect_postgres(&pt_config).await?;
        let executor = PostgresQueryExecutor::new(None, &pt_config).await?;
        let boxed_trait = Box::new(executor) as Box<dyn QueryExecutor>;

        Ok(Self {
            pg: Box::new(client),
            executor: Arc::new(boxed_trait),
        })
    }

    pub async fn run_migrations(&mut self) -> anyhow::Result<()> {
        run_migrations(&mut self.pg).await
    }

    pub fn get_executor(&self) -> Arc<Box<dyn QueryExecutor>> {
        self.executor.clone()
    }

    pub async fn create_peer(&self, peer: &Peer) -> anyhow::Result<i64> {
        let config_blob = {
            let config = peer.config.clone().context("invalid peer config")?;
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
                Config::EventhubConfig(eventhub_config) => {
                    let config_len = eventhub_config.encoded_len();
                    buf.reserve(config_len);
                    eventhub_config.encode(&mut buf)?;
                }
                Config::S3Config(s3_config) => {
                    let config_len = s3_config.encoded_len();
                    buf.reserve(config_len);
                    s3_config.encode(&mut buf)?;
                }
                Config::SqlserverConfig(sqlserver_config) => {
                    let config_len = sqlserver_config.encoded_len();
                    buf.reserve(config_len);
                    sqlserver_config.encode(&mut buf)?;
                }
            };

            buf
        };

        let stmt = self
            .pg
            .prepare_typed(
                "INSERT INTO peers (name, type, options) VALUES ($1, $2, $3)",
                &[types::Type::TEXT, types::Type::INT4, types::Type::BYTEA],
            )
            .await?;

        self.pg
            .execute(&stmt, &[&peer.name, &peer.r#type, &config_blob])
            .await?;

        self.get_peer_id(&peer.name).await
    }

    async fn get_peer_id(&self, peer_name: &str) -> anyhow::Result<i64> {
        let id = self.get_peer_id_i32(peer_name).await?;
        Ok(id as i64)
    }

    // get peer id as i32
    pub async fn get_peer_id_i32(&self, peer_name: &str) -> anyhow::Result<i32> {
        let stmt = self
            .pg
            .prepare_typed("SELECT id FROM peers WHERE name = $1", &[types::Type::TEXT])
            .await?;

        self.pg
            .query_opt(&stmt, &[&peer_name])
            .await?
            .map(|row| row.get(0))
            .context("Failed to get peer id")
    }

    // get the database type for a given peer id
    pub async fn get_peer_type_for_id(&self, peer_id: i32) -> anyhow::Result<DbType> {
        let stmt = self
            .pg
            .prepare_typed("SELECT type FROM peers WHERE id = $1", &[types::Type::INT4])
            .await?;

        self.pg
            .query_opt(&stmt, &[&peer_id])
            .await?
            .map(|row| row.get(0))
            .map(|r#type| DbType::from_i32(r#type).unwrap()) // if row was inserted properly, this should never fail
            .context("Failed to get peer type")
    }

    pub async fn get_peers(&self) -> anyhow::Result<HashMap<String, Peer>> {
        let stmt = self
            .pg
            .prepare_typed("SELECT id, name, type, options FROM peers", &[])
            .await?;

        let rows = self.pg.query(&stmt, &[]).await?;

        let mut peers = HashMap::new();

        for row in rows {
            let name: String = row.get(1);
            let peer_type: i32 = row.get(2);
            let options: Vec<u8> = row.get(3);
            let db_type = DbType::from_i32(peer_type);
            let config = self.get_config(db_type, &name, options).await?;

            let peer = Peer {
                name: name.clone().to_lowercase(),
                r#type: peer_type,
                config,
            };
            peers.insert(name, peer);
        }

        Ok(peers)
    }

    pub async fn get_peer(&self, peer_name: &str) -> anyhow::Result<Peer> {
        let stmt = self
            .pg
            .prepare_typed(
                "SELECT id, name, type, options FROM peers WHERE name = $1",
                &[],
            )
            .await?;

        let rows = self.pg.query(&stmt, &[&peer_name]).await?;

        if let Some(row) = rows.first() {
            let name: String = row.get(1);
            let peer_type: i32 = row.get(2);
            let options: Vec<u8> = row.get(3);
            let db_type = DbType::from_i32(peer_type);
            let config = self.get_config(db_type, &name, options).await?;

            let peer = Peer {
                name: name.clone().to_lowercase(),
                r#type: peer_type,
                config,
            };

            Ok(peer)
        } else {
            Err(anyhow::anyhow!("No peer with name {} found", peer_name))
        }
    }

    pub async fn get_config(
        &self,
        db_type: Option<DbType>,
        name: &str,
        options: Vec<u8>,
    ) -> anyhow::Result<Option<Config>> {
        match db_type {
            Some(DbType::Snowflake) => {
                let err = format!("unable to decode {} options for peer {}", "snowflake", name);
                let snowflake_config =
                    pt::peerdb_peers::SnowflakeConfig::decode(options.as_slice()).context(err)?;
                Ok(Some(Config::SnowflakeConfig(snowflake_config)))
            }
            Some(DbType::Bigquery) => {
                let err = format!("unable to decode {} options for peer {}", "bigquery", name);
                let bigquery_config =
                    pt::peerdb_peers::BigqueryConfig::decode(options.as_slice()).context(err)?;
                Ok(Some(Config::BigqueryConfig(bigquery_config)))
            }
            Some(DbType::Mongo) => {
                let err = format!("unable to decode {} options for peer {}", "mongo", name);
                let mongo_config =
                    pt::peerdb_peers::MongoConfig::decode(options.as_slice()).context(err)?;
                Ok(Some(Config::MongoConfig(mongo_config)))
            }
            Some(DbType::Eventhub) => {
                let err = format!("unable to decode {} options for peer {}", "eventhub", name);
                let eventhub_config =
                    pt::peerdb_peers::EventHubConfig::decode(options.as_slice()).context(err)?;
                Ok(Some(Config::EventhubConfig(eventhub_config)))
            }
            Some(DbType::Postgres) => {
                let err = format!("unable to decode {} options for peer {}", "postgres", name);
                let postgres_config =
                    pt::peerdb_peers::PostgresConfig::decode(options.as_slice()).context(err)?;
                Ok(Some(Config::PostgresConfig(postgres_config)))
            }
            Some(DbType::S3) => {
                let err = format!("unable to decode {} options for peer {}", "s3", name);
                let s3_config =
                    pt::peerdb_peers::S3Config::decode(options.as_slice()).context(err)?;
                Ok(Some(Config::S3Config(s3_config)))
            }
            Some(DbType::Sqlserver) => {
                let err = format!("unable to decode {} options for peer {}", "sqlserver", name);
                let sqlserver_config =
                    pt::peerdb_peers::SqlServerConfig::decode(options.as_slice()).context(err)?;
                Ok(Some(Config::SqlserverConfig(sqlserver_config)))
            }
            None => Ok(None),
        }
    }

    async fn normalize_schema_for_table_identifier(
        &self,
        table_identifier: &str,
        peer_id: i32,
    ) -> anyhow::Result<String> {
        let peer_dbtype = self.get_peer_type_for_id(peer_id).await?;

        let mut table_identifier_parts = table_identifier.split('.').collect::<Vec<&str>>();
        if table_identifier_parts.len() == 1 && (peer_dbtype != DbType::Bigquery) {
            table_identifier_parts.insert(0, "public");
        }

        Ok(table_identifier_parts.join("."))
    }

    pub async fn create_flow_job_entry(&self, job: &FlowJob) -> anyhow::Result<()> {
        let source_peer_id = self
            .get_peer_id_i32(&job.source_peer)
            .await
            .context("unable to get source peer id")?;
        let destination_peer_id = self
            .get_peer_id_i32(&job.target_peer)
            .await
            .context("unable to get destination peer id")?;

        let stmt = self
            .pg
            .prepare_typed(
                "INSERT INTO flows (name, source_peer, destination_peer, description,
                     source_table_identifier, destination_table_identifier) VALUES ($1, $2, $3, $4, $5, $6)",
                &[types::Type::TEXT, types::Type::INT4, types::Type::INT4, types::Type::TEXT,
                 types::Type::TEXT, types::Type::TEXT],
            )
            .await?;

        for table_mapping in &job.table_mappings {
            let _rows = self
                .pg
                .execute(
                    &stmt,
                    &[
                        &job.name,
                        &source_peer_id,
                        &destination_peer_id,
                        &job.description,
                        &self
                            .normalize_schema_for_table_identifier(
                                &table_mapping.source_table_identifier,
                                source_peer_id,
                            )
                            .await?,
                        &self
                            .normalize_schema_for_table_identifier(
                                &table_mapping.target_table_identifier,
                                destination_peer_id,
                            )
                            .await?,
                    ],
                )
                .await?;
        }

        Ok(())
    }

    pub async fn get_qrep_flow_job_by_name(
        &self,
        job_name: &str,
    ) -> anyhow::Result<Option<QRepFlowJob>> {
        let stmt = self
            .pg
            .prepare_typed("SELECT f.*, sp.name as source_peer_name, dp.name as destination_peer_name FROM flows as f
                            INNER JOIN peers as sp ON f.source_peer = sp.id
                            INNER JOIN peers as dp ON f.destination_peer = dp.id
                            WHERE f.name = $1", &[types::Type::TEXT])
            .await?;

        let job = self.pg.query_opt(&stmt, &[&job_name]).await?.map(|row| {
            QRepFlowJob {
                name: row.get("name"),
                source_peer: row.get("source_peer_name"),
                target_peer: row.get("destination_peer_name"),
                description: row.get("description"),
                query_string: row.get("query_string"),
                flow_options: serde_json::from_value(row.get("flow_metadata"))
                    .context("unable to deserialize flow options")
                    .unwrap_or_default(),
                // we set the disabled flag to false by default
                disabled: false,
            }
        });

        Ok(job)
    }

    pub async fn create_qrep_flow_job_entry(&self, job: &QRepFlowJob) -> anyhow::Result<()> {
        let source_peer_id = self
            .get_peer_id_i32(&job.source_peer)
            .await
            .context("unable to get source peer id")?;
        let destination_peer_id = self
            .get_peer_id_i32(&job.target_peer)
            .await
            .context("unable to get destination peer id")?;

        let stmt = self
            .pg
            .prepare_typed(
                "INSERT INTO flows (name, source_peer, destination_peer, description,
                     destination_table_identifier, query_string, flow_metadata) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                &[types::Type::TEXT, types::Type::INT4, types::Type::INT4, types::Type::TEXT,
                 types::Type::TEXT, types::Type::TEXT, types::Type::JSONB],
            )
            .await?;

        let _rows = self
            .pg
            .execute(
                &stmt,
                &[
                    &job.name,
                    &source_peer_id,
                    &destination_peer_id,
                    &job.description,
                    &job.flow_options
                        .get("destination_table_name")
                        .unwrap()
                        .as_str()
                        .unwrap(),
                    &job.query_string,
                    &serde_json::to_value(job.flow_options.clone())
                        .context("unable to serialize flow options")?,
                ],
            )
            .await?;

        Ok(())
    }

    pub async fn update_workflow_id_for_flow_job(
        &self,
        flow_job_name: &str,
        workflow_id: &str,
    ) -> anyhow::Result<()> {
        let rows = self
            .pg
            .execute(
                "UPDATE FLOWS SET WORKFLOW_ID = $1 WHERE NAME = $2",
                &[&workflow_id, &flow_job_name],
            )
            .await?;
        if rows == 0 {
            return Err(anyhow!("unable to find metadata for flow"));
        }
        Ok(())
    }

    pub async fn get_workflow_details_for_flow_job(
        &self,
        flow_job_name: &str,
    ) -> anyhow::Result<Option<WorkflowDetails>> {
        let rows = self
            .pg
            .query(
                "SELECT workflow_id, source_peer, destination_peer FROM flows WHERE NAME = $1",
                &[&flow_job_name],
            )
            .await?;

        // currently multiple rows for a flow job exist in catalog, but all mapped to same workflow id
        // CHANGE LOGIC IF THIS ASSUMPTION CHANGES
        if rows.is_empty() {
            tracing::info!("no workflow id found for flow job {}", flow_job_name);
            return Ok(None);
        }

        let first_row = rows.get(0).unwrap();
        let workflow_id: String = first_row.get(0);
        let source_peer_id: i32 = first_row.get(1);
        let destination_peer_id: i32 = first_row.get(2);

        let source_peer = self
            .get_peer(&source_peer_id.to_string())
            .await
            .context("unable to get source peer")?;
        let destination_peer = self
            .get_peer(&destination_peer_id.to_string())
            .await
            .context("unable to get destination peer")?;

        Ok(Some(WorkflowDetails {
            workflow_id,
            source_peer,
            destination_peer,
        }))
    }

    pub async fn delete_flow_job_entry(&self, flow_job_name: &str) -> anyhow::Result<()> {
        let rows = self
            .pg
            .execute("DELETE FROM FLOWS WHERE NAME = $1", &[&flow_job_name])
            .await?;
        if rows == 0 {
            return Err(anyhow!("unable to delete flow job metadata"));
        }
        Ok(())
    }
}
