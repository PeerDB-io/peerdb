// multipass statement analyzer.

use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
    vec,
};

use anyhow::Context;
use flow_rs::{FlowJob, FlowJobTableMapping, QRepFlowJob};
use pt::peers::{
    peer::Config, BigqueryConfig, DbType, KafkaConfig, MongoConfig, Peer, PostgresConfig,
    SnowflakeConfig,
};
use serde_json::Number;
use sqlparser::ast::{visit_relations, visit_statements, FetchDirection, SqlOption, Statement};
use sqlparser::ast::{
    CreateMirror::{Select, CDC},
    Value,
};

pub trait StatementAnalyzer {
    type Output;

    fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output>;
}

/// PeerExistanceAnalyzer is a statement analyzer that checks if the given
/// statement touches a peer that exists in the system. If there isn't a peer
/// this points to a catalog query.
pub struct PeerExistanceAnalyzer<'a> {
    peers: &'a HashMap<String, Peer>,
}

impl<'a> PeerExistanceAnalyzer<'a> {
    pub fn new(peers: &'a HashMap<String, Peer>) -> Self {
        Self { peers }
    }
}

#[derive(Debug, Clone)]
pub enum QueryAssocation {
    Peer(Box<Peer>),
    Catalog,
}

impl<'a> StatementAnalyzer for PeerExistanceAnalyzer<'a> {
    type Output = QueryAssocation;

    fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output> {
        let mut peers_touched: HashSet<String> = HashSet::new();

        // This is necessary as visit relations was not visiting drop table's object names,
        // causing DROP commands for Postgres peer being interpreted as
        // catalog queries.
        visit_statements(statement, |stmt| {
            if let &Statement::Drop { names, .. } = &stmt {
                for name in names {
                    let peer_name = &name.0[0].value.to_lowercase();
                    if self.peers.contains_key(peer_name) {
                        peers_touched.insert(peer_name.into());
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
        visit_relations(statement, |relation| {
            let peer_name = &relation.0[0].value.to_lowercase();
            if self.peers.contains_key(peer_name) {
                peers_touched.insert(peer_name.into());
            }
            ControlFlow::<()>::Continue(())
        });

        // we only support single or no peer queries for now
        if peers_touched.len() > 1 {
            anyhow::bail!("queries touching multiple peers are not supported")
        } else if let Some(peer_name) = peers_touched.iter().next() {
            let peer = self.peers.get(peer_name).unwrap();
            Ok(QueryAssocation::Peer(Box::new(peer.clone())))
        } else {
            Ok(QueryAssocation::Catalog)
        }
    }
}

/// PeerDDLAnalyzer is a statement analyzer that checks if the given
/// statement is a PeerDB DDL statement. If it is, it returns the type of
/// DDL statement.
#[derive(Default)]
pub struct PeerDDLAnalyzer;

#[derive(Debug, Clone)]
pub enum PeerDDL {
    CreatePeer {
        peer: Box<pt::peers::Peer>,
        if_not_exists: bool,
    },
    CreateMirrorForCDC {
        flow_job: FlowJob,
    },
    CreateMirrorForSelect {
        qrep_flow_job: QRepFlowJob,
    },
    DropMirror {
        if_exists: bool,
        flow_job_name: String,
    },
}

impl PeerDDLAnalyzer {
    fn parse_string_for_options(
        raw_options: &HashMap<&str, &Value>,
        processed_options: &mut HashMap<String, serde_json::Value>,
        key: &str,
        is_required: bool,
        accepted_values: Option<&[&str]>,
    ) -> anyhow::Result<()> {
        if raw_options.get(key).is_none() {
            if is_required {
                anyhow::bail!("{} is required", key);
            } else {
                Ok(())
            }
        } else {
            let raw_value = *raw_options.get(key).unwrap();
            match raw_value {
                sqlparser::ast::Value::SingleQuotedString(str) => {
                    if accepted_values.is_some() {
                        let accepted_values = accepted_values.unwrap();
                        if !accepted_values.contains(&str.as_str()) {
                            anyhow::bail!("{} must be one of {:?}", key, accepted_values);
                        }
                    }
                    processed_options
                        .insert(key.to_string(), serde_json::Value::String(str.clone()));
                    Ok(())
                }
                _ => {
                    anyhow::bail!("invalid value for {}", key);
                }
            }
        }
    }

    fn parse_number_for_options(
        raw_options: &HashMap<&str, &Value>,
        processed_options: &mut HashMap<String, serde_json::Value>,
        key: &str,
        min_value: u32,
        default_value: u32,
    ) -> anyhow::Result<()> {
        if raw_options.get(key).is_none() {
            processed_options.insert(
                key.to_string(),
                serde_json::Value::Number(Number::from(default_value)),
            );
            Ok(())
        } else {
            let raw_value = *raw_options.get(key).unwrap();
            match raw_value {
                sqlparser::ast::Value::Number(str, _) => {
                    let value = str.parse::<u32>()?;
                    if value < min_value {
                        anyhow::bail!("{} must be greater than {}", key, min_value - 1);
                    }
                    processed_options.insert(
                        key.to_string(),
                        serde_json::Value::Number(Number::from(value)),
                    );
                    Ok(())
                }
                _ => {
                    anyhow::bail!("invalid value for {}", key);
                }
            }
        }
    }
}

impl StatementAnalyzer for PeerDDLAnalyzer {
    type Output = Option<PeerDDL>;

    fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output> {
        match statement {
            Statement::CreatePeer {
                if_not_exists,
                peer_name,
                peer_type,
                with_options,
            } => {
                let db_type = DbType::from(peer_type.clone());
                let config = parse_db_options(db_type, with_options.clone())?;
                let peer = Peer {
                    name: peer_name.to_string().to_lowercase(),
                    r#type: db_type as i32,
                    config,
                };

                Ok(Some(PeerDDL::CreatePeer {
                    peer: Box::new(peer),
                    if_not_exists: *if_not_exists,
                }))
            }
            Statement::CreateMirror { create_mirror } => {
                match create_mirror {
                    CDC(cdc) => {
                        let mut flow_job_table_mappings = vec![];
                        for table_mapping in &cdc.table_mappings {
                            flow_job_table_mappings.push(FlowJobTableMapping {
                                source_table_identifier: table_mapping
                                    .source_table_identifier
                                    .to_string()
                                    .to_lowercase(),
                                target_table_identifier: table_mapping
                                    .target_table_identifier
                                    .to_string()
                                    .to_lowercase(),
                            });
                        }

                        let flow_job = FlowJob {
                            name: cdc.mirror_name.to_string().to_lowercase(),
                            source_peer: cdc.source_peer.to_string().to_lowercase(),
                            target_peer: cdc.target_peer.to_string().to_lowercase(),
                            table_mappings: flow_job_table_mappings,
                            description: "".to_string(), // TODO: add description
                        };

                        Ok(Some(PeerDDL::CreateMirrorForCDC { flow_job }))
                    }
                    Select(select) => {
                        let mut raw_options = HashMap::new();
                        for option in &select.with_options {
                            raw_options.insert(&option.name.value as &str, &option.value);
                        }

                        let mut processed_options = HashMap::new();

                        // processing options that are REQUIRED and take a string value.
                        for key in [
                            "destination_table_name",
                            "watermark_column",
                            "watermark_table_name",
                        ] {
                            PeerDDLAnalyzer::parse_string_for_options(
                                &raw_options,
                                &mut processed_options,
                                key,
                                true,
                                None,
                            )?;
                        }
                        PeerDDLAnalyzer::parse_string_for_options(
                            &raw_options,
                            &mut processed_options,
                            "mode",
                            true,
                            Some(&["append", "upsert"]),
                        )?;
                        // processing options that are OPTIONAL and take a string value.
                        PeerDDLAnalyzer::parse_string_for_options(
                            &raw_options,
                            &mut processed_options,
                            "unique_key_columns",
                            false,
                            None,
                        )?;
                        PeerDDLAnalyzer::parse_string_for_options(
                            &raw_options,
                            &mut processed_options,
                            "sync_data_format",
                            false,
                            Some(&["default", "avro"]),
                        )?;
                        // processing options that are OPTIONAL and take a number value which a minimum and default value.
                        PeerDDLAnalyzer::parse_number_for_options(
                            &raw_options,
                            &mut processed_options,
                            "parallelism",
                            1,
                            2,
                        )?;
                        PeerDDLAnalyzer::parse_number_for_options(
                            &raw_options,
                            &mut processed_options,
                            "refresh_interval",
                            10,
                            10,
                        )?;
                        PeerDDLAnalyzer::parse_number_for_options(
                            &raw_options,
                            &mut processed_options,
                            "batch_size_int",
                            1,
                            10000,
                        )?;
                        PeerDDLAnalyzer::parse_number_for_options(
                            &raw_options,
                            &mut processed_options,
                            "batch_duration_timestamp",
                            1,
                            60,
                        )?;

                        if !processed_options.contains_key("sync_data_format") {
                            processed_options.insert(
                                "sync_data_format".to_string(),
                                serde_json::Value::String("default".to_string())
                            );
                        }

                        // unique_key_columns should only be specified if mode is upsert
                        if processed_options.contains_key("unique_key_columns")
                            ^ (processed_options.get("mode").unwrap() == "upsert")
                        {
                            if processed_options.get("mode").unwrap() == "upsert" {
                                anyhow::bail!(
                                    "unique_key_columns should be specified if mode is upsert"
                                );
                            } else {
                                anyhow::bail!(
                                    "mode should be upsert if unique_key_columns is specified"
                                );
                            }
                        }

                        if processed_options.contains_key("unique_key_columns") {
                            processed_options.insert(
                                "unique_key_columns".to_string(),
                                serde_json::Value::Array(
                                    processed_options
                                        .get("unique_key_columns")
                                        .unwrap()
                                        .as_str()
                                        .unwrap()
                                        .split(',')
                                        .map(|s| serde_json::Value::String(s.to_string()))
                                        .collect(),
                                ),
                            );
                        }

                        let qrep_flow_job = QRepFlowJob {
                            name: select.mirror_name.to_string().to_lowercase(),
                            source_peer: select.source_peer.to_string().to_lowercase(),
                            target_peer: select.target_peer.to_string().to_lowercase(),
                            query_string: select.query_string.to_string(),
                            flow_options: processed_options,
                            description: "".to_string(), // TODO: add description
                        };

                        Ok(Some(PeerDDL::CreateMirrorForSelect { qrep_flow_job }))
                    }
                }
            }
            Statement::DropMirror {
                if_exists,
                mirror_name,
            } => Ok(Some(PeerDDL::DropMirror {
                if_exists: *if_exists,
                flow_job_name: mirror_name.to_string().to_lowercase(),
            })),
            _ => Ok(None),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CursorEvent {
    Fetch(String, usize),
    CloseAll,
    Close(String),
}

/// PeerCursorAnalyzer is a statement analyzer that checks if the given
/// statement is a PeerDB cursor statement. If it is, it returns the type of
/// cursor statement.
///
/// Cursor statements are statements that are used to manage cursors. They are
/// used to fetch data from a peer and close cursors.
///
/// Note that this doesn't include DECLARE statements as they are not used to
/// manage cursors, but rather to declare / create them.
#[derive(Default)]
pub struct PeerCursorAnalyzer;

impl StatementAnalyzer for PeerCursorAnalyzer {
    type Output = Option<CursorEvent>;

    fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output> {
        match statement {
            Statement::Fetch {
                name, direction, ..
            } => {
                let count = match direction {
                    FetchDirection::Count {
                        limit: sqlparser::ast::Value::Number(n, _),
                    }
                    | FetchDirection::Forward {
                        limit: Some(sqlparser::ast::Value::Number(n, _)),
                    } => n.parse::<usize>(),
                    _ => {
                        return Err(anyhow::anyhow!(
                            "invalid fetch direction for cursor: {:?}",
                            direction
                        ))
                    }
                };
                Ok(Some(CursorEvent::Fetch(name.value.clone(), count?)))
            }
            Statement::Close { cursor } => match cursor {
                sqlparser::ast::CloseCursor::All => Ok(Some(CursorEvent::CloseAll)),
                sqlparser::ast::CloseCursor::Specific { name } => {
                    Ok(Some(CursorEvent::Close(name.to_string())))
                }
            },
            _ => Ok(None),
        }
    }
}

fn parse_db_options(
    db_type: DbType,
    with_options: Vec<SqlOption>,
) -> anyhow::Result<Option<Config>> {
    let mut opts: HashMap<String, String> = HashMap::new();
    for opt in with_options {
        let key = opt.name.value;
        let val = match opt.value {
            sqlparser::ast::Value::SingleQuotedString(str) => str,
            _ => panic!("invalid option type for peer"),
        };
        opts.insert(key, val);
    }

    let config = match db_type {
        DbType::Bigquery => {
            let pem_str = opts
                .remove("private_key")
                .ok_or_else(|| anyhow::anyhow!("missing private_key option for bigquery"))?;
            pem::parse(pem_str.as_bytes())
                .map_err(|err| anyhow::anyhow!("unable to parse private_key: {:?}", err))?;
            let bq_config = BigqueryConfig {
                auth_type: opts
                    .remove("type")
                    .ok_or_else(|| anyhow::anyhow!("missing type option for bigquery"))?,
                project_id: opts
                    .remove("project_id")
                    .ok_or_else(|| anyhow::anyhow!("missing project_id in peer options"))?,
                private_key_id: opts
                    .remove("private_key_id")
                    .ok_or_else(|| anyhow::anyhow!("missing private_key_id option for bigquery"))?,
                private_key: pem_str,
                client_email: opts
                    .remove("client_email")
                    .ok_or_else(|| anyhow::anyhow!("missing client_email option for bigquery"))?,
                client_id: opts
                    .remove("client_id")
                    .ok_or_else(|| anyhow::anyhow!("missing client_id option for bigquery"))?,
                auth_uri: opts
                    .remove("auth_uri")
                    .ok_or_else(|| anyhow::anyhow!("missing auth_uri option for bigquery"))?,
                token_uri: opts
                    .remove("token_uri")
                    .ok_or_else(|| anyhow::anyhow!("missing token_uri option for bigquery"))?,
                auth_provider_x509_cert_url: opts
                    .remove("auth_provider_x509_cert_url")
                    .ok_or_else(|| {
                        anyhow::anyhow!("missing auth_provider_x509_cert_url option for bigquery")
                    })?,
                client_x509_cert_url: opts.remove("client_x509_cert_url").ok_or_else(|| {
                    anyhow::anyhow!("missing client_x509_cert_url option for bigquery")
                })?,
                dataset_id: opts
                    .remove("dataset_id")
                    .ok_or_else(|| anyhow::anyhow!("missing dataset_id in peer options"))?,
            };
            let config = Config::BigqueryConfig(bq_config);
            Some(config)
        }
        DbType::Snowflake => {
            let snowflake_config = SnowflakeConfig {
                account_id: opts
                    .get("account_id")
                    .context("no account_id specified")?
                    .to_string(),
                username: opts
                    .get("username")
                    .context("no username specified")?
                    .to_string(),
                private_key: opts
                    .get("private_key")
                    .context("no private_key specified")?
                    .to_string(),
                database: opts
                    .get("database")
                    .context("no database specified")?
                    .to_string(),
                warehouse: opts
                    .get("warehouse")
                    .context("no warehouse specified")?
                    .to_string(),
                role: opts.get("role").context("no role specified")?.to_string(),
                query_timeout: opts
                    .get("query_timeout")
                    .context("no query_timeout specified")?
                    .parse::<u64>()
                    .context("unable to parse query_timeout")?,
            };
            let config = Config::SnowflakeConfig(snowflake_config);
            Some(config)
        }
        DbType::Mongo => {
            let mongo_config = MongoConfig {
                username: opts
                    .get("username")
                    .context("no username specified")?
                    .to_string(),
                password: opts
                    .get("password")
                    .context("no password specified")?
                    .to_string(),
                clusterurl: opts
                    .get("clusterurl")
                    .context("no clusterurl specified")?
                    .to_string(),
                database: opts
                    .get("database")
                    .context("no default database specified")?
                    .to_string(),
                clusterport: opts
                    .get("clusterport")
                    .context("no cluster port specified")?
                    .parse::<i32>()
                    .context("unable to parse port as valid int")?,
            };
            let config = Config::MongoConfig(mongo_config);
            Some(config)
        }
        DbType::Postgres => {
            let postgres_config = PostgresConfig {
                host: opts.get("host").context("no host specified")?.to_string(),
                port: opts
                    .get("port")
                    .context("no port specified")?
                    .parse::<u32>()
                    .context("unable to parse port as valid int")?,
                user: opts
                    .get("user")
                    .context("no username specified")?
                    .to_string(),
                password: opts
                    .get("password")
                    .context("no password specified")?
                    .to_string(),
                database: opts
                    .get("database")
                    .context("no default database specified")?
                    .to_string(),
            };
            let config = Config::PostgresConfig(postgres_config);
            Some(config)
        }
        DbType::Kafka => {
            let kafka_config = KafkaConfig {
                servers: opts
                    .get("servers")
                    .context("no kafka server hosts specified")?
                    .to_string(),
                security_protocol: opts
                    .get("security_protocol")
                    .context("no security protocol specified")?
                    .to_string(),
                ssl_certificate: opts
                    .get("ssl_certificate")
                    .context("no SSL certificate specified")?
                    .to_string(),
                username: opts
                    .get("username")
                    .context("no sasl username given")?
                    .to_string(),
                password: opts
                    .get("password")
                    .context("no sasl password given")?
                    .to_string(),
            };
            let config = Config::KafkaConfig(kafka_config);
            Some(config)
        }
    };

    Ok(config)
}
