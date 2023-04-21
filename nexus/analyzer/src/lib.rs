// multipass statement analyzer.

use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
};

use anyhow::Context;
use catalog::Catalog;
use pt::peers::{
    peer::Config, BigqueryConfig, DbType, MongoConfig, Peer, PostgresConfig, SnowflakeConfig,
};
use sqlparser::ast::{visit_relations, SqlOption, Statement};

#[async_trait::async_trait]
pub trait StatementAnalyzer {
    type Output;

    async fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output>;
}

/// PeerExistanceAnalyzer is a statement analyzer that checks if the given
/// statement touches a peer that exists in the system. If there isn't a peer
/// this points to a catalog query.
pub struct PeerExistanceAnalyzer<'a> {
    catalog: &'a mut Catalog,
}

impl<'a> PeerExistanceAnalyzer<'a> {
    pub fn new(catalog: &'a mut Catalog) -> Self {
        Self { catalog }
    }
}

pub enum QueryAssocation {
    Peer(Box<Peer>),
    Catalog,
}

#[async_trait::async_trait]
impl<'a> StatementAnalyzer for PeerExistanceAnalyzer<'a> {
    type Output = QueryAssocation;

    async fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output> {
        let peer_map = self.catalog.get_peers().await?;
        let mut peers_touched: HashSet<String> = HashSet::new();

        visit_relations(statement, |relation| {
            let peer_name = &relation.0[0].value;
            if peer_map.contains_key(peer_name) {
                peers_touched.insert(peer_name.into());
            }
            ControlFlow::<()>::Continue(())
        });

        // we only support single or no peer queries for now
        if peers_touched.len() > 1 {
            anyhow::bail!("queries touching multiple peers are not supported")
        } else if let Some(peer_name) = peers_touched.iter().next() {
            let peer = peer_map.get(peer_name).unwrap();
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

pub enum PeerDDL {
    CreatePeer {
        peer: Box<pt::peers::Peer>,
        if_not_exists: bool,
    },
}

#[async_trait::async_trait]
impl StatementAnalyzer for PeerDDLAnalyzer {
    type Output = Option<PeerDDL>;

    async fn analyze(&self, statement: &Statement) -> anyhow::Result<Self::Output> {
        if let Statement::CreatePeer {
            if_not_exists,
            peer_name,
            peer_type,
            with_options,
        } = statement
        {
            let db_type = DbType::from(peer_type.clone());
            let config = parse_db_options(db_type, with_options.clone())?;
            let peer = Peer {
                name: peer_name.to_string(),
                r#type: db_type as i32,
                config,
            };

            Ok(Some(PeerDDL::CreatePeer {
                peer: Box::new(peer),
                if_not_exists: *if_not_exists,
            }))
        } else {
            Ok(None)
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
    };

    Ok(config)
}
