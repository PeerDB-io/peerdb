mod ast;

use std::fmt::Write;

use peer_cursor::{
    CursorManager, CursorModification, QueryExecutor, QueryOutput, Schema,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pt::peerdb_peers::ClickhouseConfig;
use sqlparser::ast::{CloseCursor, Declare, Expr, FetchDirection, Statement, Value};

pub struct ClickHouseQueryExecutor {
    peer_name: String,
    client: clickhouse::Client,
    cursor_manager: CursorManager,
}

impl ClickHouseQueryExecutor {
    pub async fn new(peer_name: String, config: &ClickhouseConfig) -> anyhow::Result<Self> {
        let protocol = if config.disable_tls {
            "http"
        } else {
            "https"
        };
        let client = clickhouse::Client::default()
            .with_url(format!("{}://{}:{}", protocol, config.host, config.port))
            .with_user(&config.user)
            .with_password(&config.password)
            .with_database(&config.database);

        Ok(Self {
            peer_name,
            client,
            cursor_manager: Default::default(),
        })
    }
}

#[async_trait::async_trait]
impl QueryExecutor for ClickHouseQueryExecutor {
    // #[tracing::instrument(skip(self, stmt), fields(stmt = %stmt))]
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        // only support SELECT statements
        match stmt {
            Statement::Explain {
                analyze,
                format,
                statement,
                ..
            } => {
                if let Statement::Query(ref query) = **statement {
                    let mut query = query.clone();
                    ast::rewrite_query(&self.peer_name, &mut query);
                    let mut querystr = String::from("EXPLAIN ");
                    if *analyze {
                        querystr.push_str("ANALYZE ");
                    }
                    if let Some(format) = format {
                        write!(querystr, "FORMAT={} ", format).ok();
                    }
                    write!(querystr, "{}", query).ok();
                    tracing::info!("clickhouse rewritten query: {}", query);

                    let cursor = self.client.query(&querystr).fetch().map_err(|err| err.into());
                    //Ok(QueryOutput::Stream(Box::pin(cursor)))
                    todo!()
                } else {
                    let error = format!(
                        "only EXPLAIN SELECT statements are supported in clickhouse. got: {}",
                        statement
                    );
                    Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "fdw_error".to_owned(),
                        error,
                    ))))
                }
            }
            Statement::Query(query) => {
                let mut query = query.clone();
                ast::rewrite_query(&self.peer_name, &mut query);
                let query = query.to_string();
                tracing::info!("clickhouse rewritten query: {}", query);

                //let cursor = self.query(query).await?;
                //Ok(QueryOutput::Stream(Box::pin(cursor)))
                todo!()
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
                    let mut query = query.clone();
                    ast::rewrite_query(&self.peer_name, &mut query);
                    let query_stmt = Statement::Query(query);
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
                tracing::info!("fetching cursor for clickhouse: {}", name.value);

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

                tracing::info!("fetching {} rows", count);

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
                Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "fdw_error".to_owned(),
                    error,
                ))))
            }
        }
    }

    // describe the output of the query
    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<Schema>> {
        // print the statement
        tracing::info!("[clickhouse] describe: {}", stmt);
        // only support SELECT statements
        match stmt {
            Statement::Query(query) => {
                let mut query = query.clone();
                ast::rewrite_query(&self.peer_name, &mut query);
                query.limit = Some(Expr::Value(Value::Number(String::from("0"), false)));
               // Ok(Some(self.query_schema(query.to_string()).await?))
                    todo!()
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
