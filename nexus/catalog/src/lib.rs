use std::collections::HashMap;

use anyhow::{anyhow, Context};
use peer_cursor::{QueryExecutor, QueryOutput, Schema};
use peer_postgres::{self, ast};
use pgwire::error::PgWireResult;
use postgres_connection::{connect_postgres, get_pg_connection_string};
use prost::Message;
use pt::{
    flow_model::{FlowJob, QRepFlowJob},
    peerdb_peers::PostgresConfig,
    peerdb_peers::{peer::Config, DbType, Peer},
};
use serde_json::Value;
use sqlparser::ast::Statement;
use tokio_postgres::{types, Client};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

pub struct Catalog {
    pg: Client,
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

#[derive(Debug, Copy, Clone)]
pub struct CatalogConfig<'a> {
    pub host: &'a str,
    pub port: u16,
    pub user: &'a str,
    pub password: &'a str,
    pub database: &'a str,
}

#[derive(Debug, Clone)]
pub struct WorkflowDetails {
    pub workflow_id: String,
    pub source_peer: pt::peerdb_peers::Peer,
    pub destination_peer: pt::peerdb_peers::Peer,
}

impl<'a> CatalogConfig<'a> {
    // convert catalog config to PostgresConfig
    pub fn to_postgres_config(&self) -> pt::peerdb_peers::PostgresConfig {
        PostgresConfig {
            host: self.host.to_string(),
            port: self.port as u32,
            user: self.user.to_string(),
            password: self.password.to_string(),
            database: self.database.to_string(),
            transaction_snapshot: "".to_string(),
            metadata_schema: Some("".to_string()),
            ssh_config: None,
        }
    }

    pub fn to_pg_connection_string(&self) -> String {
        get_pg_connection_string(&self.to_postgres_config())
    }
}

impl Catalog {
    pub async fn new(pt_config: pt::peerdb_peers::PostgresConfig) -> anyhow::Result<Self> {
        let client = connect_postgres(&pt_config).await?;
        Ok(Self { pg: client })
    }

    pub async fn run_migrations(&mut self) -> anyhow::Result<()> {
        run_migrations(&mut self.pg).await
    }

    pub async fn create_peer(&self, peer: &Peer) -> anyhow::Result<i64> {
        let config_blob = {
            let config = peer.config.clone().context("invalid peer config")?;
            match config {
                Config::SnowflakeConfig(snowflake_config) => snowflake_config.encode_to_vec(),
                Config::BigqueryConfig(bigquery_config) => bigquery_config.encode_to_vec(),
                Config::MongoConfig(mongo_config) => mongo_config.encode_to_vec(),
                Config::PostgresConfig(postgres_config) => postgres_config.encode_to_vec(),
                Config::EventhubConfig(eventhub_config) => eventhub_config.encode_to_vec(),
                Config::S3Config(s3_config) => s3_config.encode_to_vec(),
                Config::SqlserverConfig(sqlserver_config) => sqlserver_config.encode_to_vec(),
                Config::EventhubGroupConfig(eventhub_group_config) => {
                    eventhub_group_config.encode_to_vec()
                }
                Config::ClickhouseConfig(clickhouse_config) => clickhouse_config.encode_to_vec(),
            }
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
            .prepare_typed(
                "SELECT id FROM public.peers WHERE name = $1",
                &[types::Type::TEXT],
            )
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
            .prepare_typed(
                "SELECT type FROM public.peers WHERE id = $1",
                &[types::Type::INT4],
            )
            .await?;

        self.pg
            .query_opt(&stmt, &[&peer_id])
            .await?
            .map(|row| row.get::<usize, i32>(0))
            .and_then(|r#type| DbType::try_from(r#type).ok()) // if row was inserted properly, this should never fail
            .context("Failed to get peer type")
    }

    pub async fn get_peers(&self) -> anyhow::Result<HashMap<String, Peer>> {
        let stmt = self
            .pg
            .prepare_typed("SELECT id, name, type, options FROM public.peers", &[])
            .await?;

        let rows = self.pg.query(&stmt, &[]).await?;

        let mut peers = HashMap::with_capacity(rows.len());

        for row in rows {
            let name: &str = row.get(1);
            let peer_type: i32 = row.get(2);
            let options: &[u8] = row.get(3);
            let db_type = DbType::try_from(peer_type).ok();
            let config = self.get_config(db_type, name, options).await?;

            let peer = Peer {
                name: name.to_lowercase(),
                r#type: peer_type,
                config,
            };
            peers.insert(name.to_string(), peer);
        }

        Ok(peers)
    }

    pub async fn get_peer(&self, peer_name: &str) -> anyhow::Result<Peer> {
        let stmt = self
            .pg
            .prepare_typed(
                "SELECT id, name, type, options FROM public.peers WHERE name = $1",
                &[],
            )
            .await?;

        let rows = self.pg.query(&stmt, &[&peer_name]).await?;

        if let Some(row) = rows.first() {
            let name: &str = row.get(1);
            let peer_type: i32 = row.get(2);
            let options: &[u8] = row.get(3);
            let db_type = DbType::try_from(peer_type).ok();
            let config = self.get_config(db_type, name, options).await?;

            let peer = Peer {
                name: name.to_lowercase(),
                r#type: peer_type,
                config,
            };

            Ok(peer)
        } else {
            Err(anyhow::anyhow!("No peer with name {} found", peer_name))
        }
    }

    pub async fn get_peer_by_id(&self, peer_id: i32) -> anyhow::Result<Peer> {
        let stmt = self
            .pg
            .prepare_typed(
                "SELECT name, type, options FROM public.peers WHERE id = $1",
                &[],
            )
            .await?;

        let rows = self.pg.query(&stmt, &[&peer_id]).await?;

        if let Some(row) = rows.first() {
            let name: &str = row.get(0);
            let peer_type: i32 = row.get(1);
            let options: &[u8] = row.get(2);
            let db_type = DbType::try_from(peer_type).ok();
            let config = self.get_config(db_type, name, options).await?;

            let peer = Peer {
                name: name.to_lowercase(),
                r#type: peer_type,
                config,
            };

            Ok(peer)
        } else {
            Err(anyhow::anyhow!("No peer with id {} found", peer_id))
        }
    }

    pub async fn get_config(
        &self,
        db_type: Option<DbType>,
        name: &str,
        options: &[u8],
    ) -> anyhow::Result<Option<Config>> {
        Ok(if let Some(db_type) = db_type {
            let err = || {
                format!(
                    "unable to decode {} options for peer {}",
                    db_type.as_str_name(),
                    name
                )
            };
            Some(match db_type {
                DbType::Snowflake => {
                    let snowflake_config =
                        pt::peerdb_peers::SnowflakeConfig::decode(options).with_context(err)?;
                    Config::SnowflakeConfig(snowflake_config)
                }
                DbType::Bigquery => {
                    let bigquery_config =
                        pt::peerdb_peers::BigqueryConfig::decode(options).with_context(err)?;
                    Config::BigqueryConfig(bigquery_config)
                }
                DbType::Mongo => {
                    let mongo_config =
                        pt::peerdb_peers::MongoConfig::decode(options).with_context(err)?;
                    Config::MongoConfig(mongo_config)
                }
                DbType::Eventhub => {
                    let eventhub_config =
                        pt::peerdb_peers::EventHubConfig::decode(options).with_context(err)?;
                    Config::EventhubConfig(eventhub_config)
                }
                DbType::Postgres => {
                    let postgres_config =
                        pt::peerdb_peers::PostgresConfig::decode(options).with_context(err)?;
                    Config::PostgresConfig(postgres_config)
                }
                DbType::S3 => {
                    let s3_config =
                        pt::peerdb_peers::S3Config::decode(options).with_context(err)?;
                    Config::S3Config(s3_config)
                }
                DbType::Sqlserver => {
                    let sqlserver_config =
                        pt::peerdb_peers::SqlServerConfig::decode(options).with_context(err)?;
                    Config::SqlserverConfig(sqlserver_config)
                }
                DbType::EventhubGroup => {
                    let eventhub_group_config =
                        pt::peerdb_peers::EventHubGroupConfig::decode(options).with_context(err)?;
                    Config::EventhubGroupConfig(eventhub_group_config)
                }
                DbType::Clickhouse => {
                    let clickhouse_config =
                        pt::peerdb_peers::ClickhouseConfig::decode(options).with_context(err)?;
                    Config::ClickhouseConfig(clickhouse_config)
                }
            })
        } else {
            None
        })
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

    pub async fn create_cdc_flow_job_entry(&self, job: &FlowJob) -> anyhow::Result<()> {
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
                                &table_mapping.destination_table_identifier,
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
            .prepare_typed("SELECT f.*, sp.name as source_peer_name, dp.name as destination_peer_name FROM public.flows as f
                            INNER JOIN public.peers as sp ON f.source_peer = sp.id
                            INNER JOIN public.peers as dp ON f.destination_peer = dp.id
                            WHERE f.name = $1 AND f.query_string IS NOT NULL", &[types::Type::TEXT])
            .await?;

        let job = self.pg.query_opt(&stmt, &[&job_name]).await?.map(|row| {
            let flow_opts: HashMap<String, Value> = row
                .get::<&str, Option<Value>>("flow_metadata")
                .and_then(|flow_opts| serde_json::from_value(flow_opts).ok())
                .unwrap_or_default();

            QRepFlowJob {
                name: row.get("name"),
                source_peer: row.get("source_peer_name"),
                target_peer: row.get("destination_peer_name"),
                description: row.get("description"),
                query_string: row.get("query_string"),
                flow_options: flow_opts,
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

        if job.flow_options.get("destination_table_name").is_none() {
            return Err(anyhow!("destination_table_name not found in flow options"));
        }

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
                "SELECT workflow_id, source_peer, destination_peer FROM public.flows WHERE NAME = $1",
                &[&flow_job_name],
            )
            .await?;

        // currently multiple rows for a flow job exist in catalog, but all mapped to same workflow id
        // CHANGE LOGIC IF THIS ASSUMPTION CHANGES
        if rows.is_empty() {
            tracing::info!("no workflow id found for flow job {}", flow_job_name);
            return Ok(None);
        }

        let first_row = rows.first().unwrap();
        let workflow_id: Option<String> = first_row.get(0);
        let Some(workflow_id) = workflow_id else {
            return Err(anyhow!(
                "workflow id not found for existing flow job {}",
                flow_job_name
            ));
        };
        let source_peer_id: i32 = first_row.get(1);
        let destination_peer_id: i32 = first_row.get(2);

        let source_peer = self
            .get_peer_by_id(source_peer_id)
            .await
            .context("unable to get source peer")?;
        let destination_peer = self
            .get_peer_by_id(destination_peer_id)
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
            .execute(
                "DELETE FROM public.flows WHERE name = $1",
                &[&flow_job_name],
            )
            .await?;
        if rows == 0 {
            return Err(anyhow!("unable to delete flow job metadata"));
        }
        Ok(())
    }

    pub async fn check_peer_entry(&self, peer_name: &str) -> anyhow::Result<i64> {
        let peer_check = self
            .pg
            .query_one(
                "SELECT COUNT(*) FROM public.peers WHERE name = $1",
                &[&peer_name],
            )
            .await?;
        let peer_count: i64 = peer_check.get(0);
        Ok(peer_count)
    }

    pub async fn get_qrep_config_proto(
        &self,
        flow_job_name: &str,
    ) -> anyhow::Result<Option<pt::peerdb_flow::QRepConfig>> {
        let row = self
            .pg
            .query_opt(
                "SELECT config_proto FROM public.flows WHERE name = $1 AND query_string IS NOT NULL",
                &[&flow_job_name],
            )
            .await?;

        Ok(match row {
            Some(row) => Some(pt::peerdb_flow::QRepConfig::decode::<&[u8]>(
                row.get("config_proto"),
            )?),
            None => None,
        })
    }
}

#[async_trait::async_trait]
impl QueryExecutor for Catalog {
    #[tracing::instrument(skip(self, stmt), fields(stmt = %stmt))]
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        peer_postgres::pg_execute(&self.pg, ast::PostgresAst { peername: None }, stmt).await
    }

    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<Schema>> {
        peer_postgres::pg_describe(&self.pg, stmt).await
    }
}
