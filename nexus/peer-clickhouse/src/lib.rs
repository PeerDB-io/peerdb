use anyhow::{anyhow, Context};
use memchr::memchr;
use peer_cursor::{CursorManager, CursorModification, QueryExecutor, QueryOutput, Record, Records, Schema};
use pgwire::api::results::{FieldInfo, FieldFormat};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use pt::peerdb_peers::ClickhouseConfig;
use reqwest::header;
use sqlparser::ast::{CloseCursor, Declare, FetchDirection, Query, Statement};

mod ast;
mod value;

use value::ClickHouseType;

use std::sync::Arc;

pub struct ClickHouseQueryExecutor {
    endpoint_url: String,
    reqwest_client: reqwest::Client,
    cursor_manager: CursorManager,
}

impl ClickHouseQueryExecutor {
    pub async fn new(config: &ClickhouseConfig) -> anyhow::Result<Self> {
        let mut default_headers = header::HeaderMap::new();
        default_headers.insert(
            "X-ClickHouse-User",
            header::HeaderValue::from_str(&config.user)?,
        );
        let mut password_header = header::HeaderValue::from_str(&config.password)?;
        password_header.set_sensitive(true);
        default_headers.insert(
            "X-ClickHouse-Key",
            password_header,
        );
        default_headers.insert(
            "X-ClickHouse-Format",
            header::HeaderValue::from_static("TabSeparatedWithNamesAndTypes"),
        );

        let reqwest_client = reqwest::ClientBuilder::new()
            .gzip(true)
            .default_headers(default_headers)
            .build()?;

        Ok(Self {
            endpoint_url: format!(
                "{}://{}:{}?wait_end_of_query=1",
                if config.disable_tls { "http" } else { "https" },
                config.host,
                if config.disable_tls { 8123 } else { 8433 },
            ),
            reqwest_client,
            cursor_manager: Default::default(),
        })
    }

    async fn process_query(&self, query_str: &str) -> anyhow::Result<Records> {
        let query_status_res = self
            .reqwest_client
            .post(&self.endpoint_url)
            .body(String::from(query_str))
            .send()
            .await
            .map_err(|e| {
                anyhow::anyhow!("failed in making request for QueryStatus. error: {:?}", e)
            })?;

        let result = query_status_res.bytes().await?;
        let mut columns: Vec<(ClickHouseType, &[u8])> = Vec::new();
        if let Some(nl_names) = memchr(b'\n', &result) {
            if let Some(nl_types) = memchr(b'\n', &result[nl_names+1..]) {
                for (name, typ) in result[..nl_names].split(|&ch| ch == b'\t').zip(result[nl_names+1..nl_types].split(|&ch| ch == b'\t')) {
                    columns.push((ClickHouseType::from_bytes(typ)?, name));
                }
                let schema = Arc::new(columns.iter().map(|(typ, name)| FieldInfo::new(String::from_utf8_lossy(name).into_owned(), None, None, typ.pgtype(), FieldFormat::Text)).collect::<Vec<_>>());
                let mut lines = &result[nl_types+1..];
                let mut records = Vec::new();
                while let Some(idx) = memchr(b'\n', lines) {
                    let line = &lines[..idx];
                    lines = &lines[idx+1..];
                    let mut record = Vec::new();
                    for (idx, val) in line.split(|&ch| ch == b'\t').enumerate() {
                        record.push(columns[idx].0.parse(val)?);
                    }
                    records.push(Record { values: record, schema: schema.clone() });
                }

                return Ok(Records { records, schema })
            }
        }

        Err(anyhow!("failed to parse query schema"))
    }

    pub async fn query(&self, query: &Query) -> PgWireResult<Records> {
        let mut query = query.clone();

        let _ = ast::ClickHouseAst.rewrite(&mut query);

        let query_str: String = query.to_string();

        let result_set = self
            .process_query(&query_str)
            .await
            .map_err(|err| PgWireError::ApiError(err.into()))?;
        Ok(result_set)
    }
}

#[async_trait::async_trait]
impl QueryExecutor for ClickHouseQueryExecutor {
    async fn execute_raw(&self, query: &str) -> PgWireResult<QueryOutput> {
        let result_set = self
            .process_query(query)
            .await
            .map_err(|err| PgWireError::ApiError(err.into()))?;

        Ok(QueryOutput::Records(result_set))
    }

    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        match stmt {
            Statement::Query(query) => {
                let mut new_query = query.clone();

                ast::ClickHouseAst
                    .rewrite(&mut new_query)
                    .context("unable to rewrite query")
                    .map_err(|err| PgWireError::ApiError(err.into()))?;

                let result_set = self.query(&query.clone()).await?;

                Ok(QueryOutput::Records(result_set))
            }
            Statement::Declare { stmts } => {
                if stmts.len() != 1 {
                    Err(PgWireError::ApiError(
                        "peerdb only supports singular declare statements".into(),
                    ))
                } else if let Declare {
                    ref names,
                    for_query: Some(ref query),
                    ..
                } = stmts[0]
                {
                    let name = &names[0];
                    let query_stmt = Statement::Query(query.clone());
                    self.cursor_manager
                        .create_cursor(&name.value, &query_stmt, self)
                        .await?;

                    Ok(QueryOutput::Cursor(CursorModification::Created(
                        name.value.clone(),
                    )))
                } else {
                    Err(PgWireError::ApiError(
                        "peerdb only supports declare for query statements".into(),
                    ))
                }
            }
            Statement::Fetch {
                name, direction, ..
            } => {
                // Attempt to extract the count from the direction
                let count = match direction {
                    FetchDirection::ForwardAll | FetchDirection::All => usize::MAX,
                    FetchDirection::Next | FetchDirection::Forward { limit: None } => 1,
                    FetchDirection::Count {
                        limit: sqlparser::ast::Value::Number(n, _),
                    }
                    | FetchDirection::Forward {
                        limit: Some(sqlparser::ast::Value::Number(n, _)),
                    } => n
                        .parse::<usize>()
                        .map_err(|err| PgWireError::ApiError(err.into()))?,
                    _ => {
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "fdw_error".to_owned(),
                            "only FORWARD count and COUNT count are supported in FETCH".to_owned(),
                        ))))
                    }
                };

                // Fetch rows from the cursor manager
                let records = self.cursor_manager.fetch(&name.value, count).await?;

                // Return the fetched records as the query output
                Ok(QueryOutput::Records(records))
            }
            Statement::Close { cursor } => {
                let closed_cursors = match cursor {
                    CloseCursor::All => self.cursor_manager.close_all_cursors().await?,
                    CloseCursor::Specific { name } => {
                        self.cursor_manager.close(&name.value).await?;
                        vec![name.value.clone()]
                    }
                };
                Ok(QueryOutput::Cursor(CursorModification::Closed(
                    closed_cursors,
                )))
            }
            _ => {
                let error = format!(
                    "only SELECT statements are supported in clickhouse. got: {}",
                    stmt
                );
                PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "fdw_error".to_owned(),
                    error,
                ))))
            }
        }
    }

    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<Schema>> {
        match stmt {
            Statement::Query(query) => {
                let mut new_query = query.clone();
                ast::ClickHouseAst
                    .rewrite(&mut new_query)
                    .context("unable to rewrite query")
                    .map_err(|err| PgWireError::ApiError(err.into()))?;

                let result_set = self.query(&new_query).await?;
                Ok(Some(result_set.schema))
            }
            Statement::Declare { stmts } => {
                if stmts.len() != 1 {
                    Err(PgWireError::ApiError(
                        "peerdb only supports singular declare statements".into(),
                    ))
                } else if let Declare {
                    for_query: Some(ref query),
                    ..
                } = stmts[0]
                {
                    let query_stmt = Statement::Query(query.clone());
                    self.describe(&query_stmt).await
                } else {
                    Err(PgWireError::ApiError(
                        "peerdb only supports declare for query statements".into(),
                    ))
                }
            }
            _ => PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                "only SELECT statements are supported in clickhouse".to_owned(),
            )))),
        }
    }
}
