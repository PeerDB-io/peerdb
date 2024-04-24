mod ast;
mod client;
mod stream;

use peer_cursor::{
    CursorManager, CursorModification, QueryExecutor, QueryOutput, RecordStream, Schema,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pt::peerdb_peers::MySqlConfig;
use sqlparser::ast::{CloseCursor, FetchDirection, Statement};
use stream::MyRecordStream;

pub struct MySqlQueryExecutor {
    peer_name: String,
    client: client::MyClient,
    cursor_manager: CursorManager,
}

impl MySqlQueryExecutor {
    pub async fn new(peer_name: String, config: &MySqlConfig) -> anyhow::Result<Self> {
        // TODO TLS
        let mut opts = mysql_async::OptsBuilder::default().prefer_socket(Some(false)); // prefer_socket breaks connecting to StarRocks
        if !config.user.is_empty() {
            opts = opts.user(Some(config.user.clone()))
        }
        if !config.password.is_empty() {
            opts = opts.pass(Some(config.password.clone()))
        }
        if !config.database.is_empty() {
            opts = opts.db_name(Some(config.database.clone()))
        }
        opts = opts
            .compression(mysql_async::Compression::new(config.compression))
            .ip_or_hostname(config.host.clone())
            .tcp_port(config.port as u16);
        let client = client::MyClient::new(opts.into()).await?;

        Ok(Self {
            peer_name,
            client,
            cursor_manager: Default::default(),
        })
    }

    async fn query(&self, query: String) -> PgWireResult<MyRecordStream> {
        MyRecordStream::query(self.client.clone(), query).await
    }

    async fn query_schema(&self, query: String) -> PgWireResult<Schema> {
        let stream = MyRecordStream::query(self.client.clone(), query).await?;
        Ok(stream.schema())
    }
}

#[async_trait::async_trait]
impl QueryExecutor for MySqlQueryExecutor {
    // #[tracing::instrument(skip(self, stmt), fields(stmt = %stmt))]
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        // only support SELECT statements
        match stmt {
            Statement::Query(query) => {
                let mut query = query.clone();
                ast::rewrite_query(&self.peer_name, &mut query);
                let query = query.to_string();
                tracing::info!("mysql rewritten query: {}", query);

                let cursor = self.query(query).await?;
                Ok(QueryOutput::Stream(Box::pin(cursor)))
            }
            Statement::Declare { name, query, .. } => {
                let mut query = query.clone();
                ast::rewrite_query(&self.peer_name, &mut query);
                let query_stmt = Statement::Query(query);
                self.cursor_manager
                    .create_cursor(&name.value, &query_stmt, self)
                    .await?;

                Ok(QueryOutput::Cursor(CursorModification::Created(
                    name.value.clone(),
                )))
            }
            Statement::Fetch {
                name, direction, ..
            } => {
                tracing::info!("fetching cursor for mysql: {}", name.value);

                // Attempt to extract the count from the direction
                let count = match direction {
                    FetchDirection::Count {
                        limit: sqlparser::ast::Value::Number(n, _),
                    }
                    | FetchDirection::Forward {
                        limit: Some(sqlparser::ast::Value::Number(n, _)),
                    } => n.parse::<usize>(),
                    _ => {
                        return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "fdw_error".to_owned(),
                            "only FORWARD count and COUNT count are supported in FETCH".to_owned(),
                        ))))
                    }
                };

                // If parsing the count resulted in an error, return an internal error
                let count = match count {
                    Ok(c) => c,
                    Err(err) => return Err(PgWireError::ApiError(err.into())),
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
                    "only SELECT statements are supported in mysql. got: {}",
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

    // describe the output of the query
    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<Schema>> {
        // print the statement
        tracing::info!("[mysql] describe: {}", stmt);
        // only support SELECT statements
        match stmt {
            Statement::Query(query) => {
                let mut query = query.clone();
                ast::rewrite_query(&self.peer_name, &mut query);
                Ok(Some(self.query_schema(query.to_string()).await?))
            }
            Statement::Declare { query, .. } => {
                let mut query = query.clone();
                ast::rewrite_query(&self.peer_name, &mut query);
                let query_stmt = Statement::Query(query);
                self.describe(&query_stmt).await
            }
            _ => PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                "only SELECT statements are supported in mysql".to_owned(),
            )))),
        }
    }
}
