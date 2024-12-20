use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_kms::{primitives::Blob, Client as KmsClient};
use base64::prelude::*;
use chacha20poly1305::{aead::Aead, KeyInit, XChaCha20Poly1305, XNonce};
use peer_cursor::{QueryExecutor, QueryOutput, Schema};
use peer_postgres::{self, ast};
use pgwire::error::PgWireResult;
use postgres_connection::{connect_postgres, get_pg_connection_string};
use pt::{
    flow_model::QRepFlowJob,
    peerdb_peers::PostgresConfig,
    peerdb_peers::{peer::Config, DbType, Peer},
    prost::Message,
};
use serde_json::{self, Value};
use sqlparser::ast::Statement;
use tokio_postgres::{types, Client};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

pub struct Catalog {
    pg: Client,
    kms_key_id: Option<Arc<String>>,
}

pub async fn kms_decrypt(encrypted_payload: &str, kms_key_id: &str) -> anyhow::Result<String> {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::defaults(BehaviorVersion::v2024_03_28())
        .region(region_provider)
        .load()
        .await;
    let client = KmsClient::new(&config);

    let decoded = BASE64_STANDARD
        .decode(encrypted_payload)
        .expect("Input does not contain valid base64 characters.");

    let resp = client
        .decrypt()
        .key_id(kms_key_id)
        .ciphertext_blob(Blob::new(decoded))
        .send()
        .await?;

    let inner = resp.plaintext.unwrap();
    let bytes = inner.as_ref();

    Ok(String::from_utf8(bytes.to_vec()).expect("Could not convert decrypted data to UTF-8"))
}

async fn run_migrations(client: &mut Client) -> anyhow::Result<()> {
    let migration_report = embedded::migrations::runner()
        .run_async(client)
        .await
        .context("Failed to run migrations")?;
    for migration in migration_report.applied_migrations() {
        tracing::info!(
            "Migration Applied - Name: {}, Version: {}",
            migration.name(),
            migration.version()
        );
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct CatalogConfig<'a> {
    pub host: &'a str,
    pub port: u16,
    pub user: &'a str,
    pub password: String,
    pub database: &'a str,
}

impl CatalogConfig<'_> {
    // convert catalog config to PostgresConfig
    pub fn to_postgres_config(&self) -> pt::peerdb_peers::PostgresConfig {
        PostgresConfig {
            host: self.host.to_string(),
            port: self.port as u32,
            user: self.user.to_string(),
            password: self.password.to_string(),
            database: self.database.to_string(),
            metadata_schema: Some("".to_string()),
            ssh_config: None,
        }
    }

    pub fn to_pg_connection_string(&self) -> String {
        get_pg_connection_string(&self.to_postgres_config())
    }
}

impl Catalog {
    pub async fn new(
        pt_config: pt::peerdb_peers::PostgresConfig,
        kms_key_id: &Option<Arc<String>>,
    ) -> anyhow::Result<Self> {
        let (pg, _) = connect_postgres(&pt_config).await?;
        Ok(Self {
            pg,
            kms_key_id: kms_key_id.clone(),
        })
    }

    pub async fn run_migrations(&mut self) -> anyhow::Result<()> {
        run_migrations(&mut self.pg).await
    }

    async fn env_enc_key(&self, enc_key_id: &str) -> anyhow::Result<Vec<u8>> {
        let mut enc_keys = env::var("PEERDB_ENC_KEYS")?;

        if let Some(kms_key_id) = &self.kms_key_id {
            enc_keys = kms_decrypt(&enc_keys, kms_key_id.as_ref()).await?;
        }

        // TODO use serde derive
        let Value::Array(keys) = serde_json::from_str(&enc_keys)? else {
            return Err(anyhow!("PEERDB_ENC_KEYS not a json array"));
        };
        for item in keys {
            let Value::Object(map) = item else {
                return Err(anyhow!("PEERDB_ENC_KEYS not a json array of json objects"));
            };
            let Some(Value::String(id)) = map.get("id") else {
                return Err(anyhow!("PEERDB_ENC_KEYS id not a json string"));
            };
            if id == enc_key_id {
                let Some(Value::String(value)) = map.get("value") else {
                    return Err(anyhow!("PEERDB_ENC_KEYS value not a json string"));
                };
                let key = BASE64_STANDARD.decode(value)?;
                if key.len() != 32 {
                    return Err(anyhow!("PEERDB_ENC_KEYS must be 32 bytes"));
                }
                return Ok(key);
            }
        }
        Err(anyhow!("failed to find default encryption key"))
    }

    pub async fn decrypt(&self, payload: &[u8], enc_key_id: &str) -> anyhow::Result<Vec<u8>> {
        if enc_key_id.is_empty() {
            return Ok(payload.to_vec());
        }

        const NONCE_SIZE: usize = 24;
        let key = self.env_enc_key(enc_key_id).await?;
        if payload.len() < NONCE_SIZE {
            return Err(anyhow!("ciphertext too short"));
        }

        let nonce = XNonce::from_slice(&payload[..NONCE_SIZE]);
        let ciphertext = &payload[NONCE_SIZE..];

        let cipher = XChaCha20Poly1305::new_from_slice(&key)
            .map_err(|e| anyhow!("Failed to create ChaCha20Poly1305 cipher: {}", e))?;

        cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| anyhow!("Decryption failed: {}", e))
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
            .prepare_typed(
                "SELECT name, type, options, enc_key_id FROM public.peers",
                &[],
            )
            .await?;

        let rows = self.pg.query(&stmt, &[]).await?;

        let mut peers = HashMap::with_capacity(rows.len());

        for row in rows {
            let name: &str = row.get(0);
            let peer_type: i32 = row.get(1);
            let options: &[u8] = row.get(2);
            let enc_key_id: &str = row.get(3);
            let db_type = DbType::try_from(peer_type).ok();
            let config = self.get_config(db_type, name, options, enc_key_id).await?;

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
                "SELECT name, type, options, enc_key_id FROM public.peers WHERE name = $1",
                &[],
            )
            .await?;

        let rows = self.pg.query(&stmt, &[&peer_name]).await?;

        if let Some(row) = rows.first() {
            let name: &str = row.get(0);
            let peer_type: i32 = row.get(1);
            let options: &[u8] = row.get(2);
            let enc_key_id: &str = row.get(3);
            let db_type = DbType::try_from(peer_type).ok();
            let config = self.get_config(db_type, name, options, enc_key_id).await?;

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

    pub async fn get_peer_name_by_id(&self, peer_id: i32) -> anyhow::Result<String> {
        let stmt = self
            .pg
            .prepare_typed("SELECT name FROM public.peers WHERE id = $1", &[])
            .await?;

        let row = self.pg.query_opt(&stmt, &[&peer_id]).await?;
        if let Some(row) = row {
            let name: String = row.get(0);
            Ok(name)
        } else {
            Err(anyhow::anyhow!("No peer with id {} found", peer_id))
        }
    }

    pub async fn get_peer_by_id(&self, peer_id: i32) -> anyhow::Result<Peer> {
        let stmt = self
            .pg
            .prepare_typed(
                "SELECT name, type, options, enc_key_id FROM public.peers WHERE id = $1",
                &[],
            )
            .await?;

        let row = self.pg.query_opt(&stmt, &[&peer_id]).await?;
        if let Some(row) = row {
            let name: &str = row.get(0);
            let peer_type: i32 = row.get(1);
            let options: &[u8] = row.get(2);
            let enc_key_id: &str = row.get(3);
            let db_type = DbType::try_from(peer_type).ok();
            let config = self.get_config(db_type, name, options, enc_key_id).await?;

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
        enc_key_id: &str,
    ) -> anyhow::Result<Option<Config>> {
        let options = self.decrypt(options, enc_key_id).await?;
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
                    let snowflake_config = pt::peerdb_peers::SnowflakeConfig::decode(&options[..])
                        .with_context(err)?;
                    Config::SnowflakeConfig(snowflake_config)
                }
                DbType::Bigquery => {
                    let bigquery_config =
                        pt::peerdb_peers::BigqueryConfig::decode(&options[..]).with_context(err)?;
                    Config::BigqueryConfig(bigquery_config)
                }
                DbType::Mongo => {
                    let mongo_config =
                        pt::peerdb_peers::MongoConfig::decode(&options[..]).with_context(err)?;
                    Config::MongoConfig(mongo_config)
                }
                DbType::Postgres => {
                    let postgres_config =
                        pt::peerdb_peers::PostgresConfig::decode(&options[..]).with_context(err)?;
                    Config::PostgresConfig(postgres_config)
                }
                DbType::S3 => {
                    let s3_config =
                        pt::peerdb_peers::S3Config::decode(&options[..]).with_context(err)?;
                    Config::S3Config(s3_config)
                }
                DbType::Sqlserver => {
                    let sqlserver_config = pt::peerdb_peers::SqlServerConfig::decode(&options[..])
                        .with_context(err)?;
                    Config::SqlserverConfig(sqlserver_config)
                }
                DbType::Eventhubs => {
                    let eventhub_group_config =
                        pt::peerdb_peers::EventHubGroupConfig::decode(&options[..])
                            .with_context(err)?;
                    Config::EventhubGroupConfig(eventhub_group_config)
                }
                DbType::Clickhouse => {
                    let clickhouse_config =
                        pt::peerdb_peers::ClickhouseConfig::decode(&options[..])
                            .with_context(err)?;
                    Config::ClickhouseConfig(clickhouse_config)
                }
                DbType::Kafka => {
                    let kafka_config =
                        pt::peerdb_peers::KafkaConfig::decode(&options[..]).with_context(err)?;
                    Config::KafkaConfig(kafka_config)
                }
                DbType::Pubsub => {
                    let pubsub_config =
                        pt::peerdb_peers::PubSubConfig::decode(&options[..]).with_context(err)?;
                    Config::PubsubConfig(pubsub_config)
                }
                DbType::Elasticsearch => {
                    let elasticsearch_config =
                        pt::peerdb_peers::ElasticsearchConfig::decode(&options[..])
                            .with_context(err)?;
                    Config::ElasticsearchConfig(elasticsearch_config)
                }
                DbType::Mysql => {
                    let mysql_config =
                        pt::peerdb_peers::MySqlConfig::decode(&options[..]).with_context(err)?;
                    Config::MysqlConfig(mysql_config)
                }
            })
        } else {
            None
        })
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

        let Some(destination_table_name) = job.flow_options.get("destination_table_name") else {
            return Err(anyhow!("destination_table_name not found in flow options"));
        };

        let _rows = self
            .pg
            .execute(
                &stmt,
                &[
                    &job.name,
                    &source_peer_id,
                    &destination_peer_id,
                    &job.description,
                    &destination_table_name.as_str().unwrap(),
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

    pub async fn flow_name_exists(&self, flow_job_name: &str) -> anyhow::Result<bool> {
        let row = self
            .pg
            .query_one(
                "SELECT EXISTS(SELECT * FROM flows WHERE name = $1)",
                &[&flow_job_name],
            )
            .await?;

        let exists: bool = row.get(0);
        Ok(exists)
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
            Some(row) => Some(pt::peerdb_flow::QRepConfig::decode(
                row.get::<&str, &[u8]>("config_proto"),
            )?),
            None => None,
        })
    }
}

#[async_trait::async_trait]
impl QueryExecutor for Catalog {
    async fn execute_raw(&self, query: &str) -> PgWireResult<QueryOutput> {
        peer_postgres::pg_execute_raw(&self.pg, query).await
    }

    #[tracing::instrument(skip(self, stmt), fields(stmt = %stmt))]
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        peer_postgres::pg_execute(&self.pg, ast::PostgresAst { peername: None }, stmt).await
    }

    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<Schema>> {
        peer_postgres::pg_describe(&self.pg, stmt).await
    }
}
