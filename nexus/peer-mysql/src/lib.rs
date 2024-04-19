mod cursor;
mod stream;

use cursor::MySqlCursorManager;
use mysql_async::{Conn, ResultSetStream, Row, TextProtocol};
use mysql_async::prelude::Queryable;
use peer_connections::PeerConnectionTracker;
use peer_cursor::{CursorModification, QueryExecutor, QueryOutput, Schema};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use sqlparser::ast::{CloseCursor, Expr, FetchDirection, Statement, Value};
use stream::{MyRecordStream, MySchema};

pub struct MySqlQueryExecutor {
    peer_name: String,
    project_id: String,
    dataset_id: String,
    peer_connections: PeerConnectionTracker,
    client: Box<Conn>,
    cursor_manager: MySqlCursorManager,
}

impl MySqlQueryExecutor {
    async fn query<'a, 'b: 'a>(&'a mut self, query: &'b str) -> PgWireResult<ResultSetStream<'a, 'a, 'static, Row, TextProtocol>> {
        self.client.query_stream(query).await.map_err(|err| PgWireError::ApiError(err.into()))
    }
}

#[async_trait::async_trait]
impl QueryExecutor for MySqlQueryExecutor {
    // #[tracing::instrument(skip(self, stmt), fields(stmt = %stmt))]
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        // only support SELECT statements
        match stmt {
            Statement::Query(query) => {
                let query = query.to_string();
                tracing::info!("bq rewritten query: {}", query);

                let result_set = self.query(&query).await?;

                let cursor = MyRecordStream::new(result_set);
                tracing::info!(
                    "retrieved rows for query {}",
                    query
                );
                Ok(QueryOutput::Stream(Box::pin(cursor)))
            }
            Statement::Declare { name, query, .. } => {
                let query_stmt = Statement::Query(query.clone());
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
                tracing::info!("fetching cursor for bigquery: {}", name.value);

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
                let mut closed_cursors = vec![];
                match cursor {
                    CloseCursor::All => {
                        closed_cursors = self.cursor_manager.close_all_cursors().await?;
                    }
                    CloseCursor::Specific { name } => {
                        self.cursor_manager.close(&name.value).await?;
                        closed_cursors.push(name.value.clone());
                    }
                };
                Ok(QueryOutput::Cursor(CursorModification::Closed(
                    closed_cursors,
                )))
            }
            _ => {
                let error = format!(
                    "only SELECT statements are supported in bigquery. got: {}",
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
                // add LIMIT 0 to the root level query.
                // this is a workaround for the bigquery API not supporting DESCRIBE
                // queries.
                query.limit = Some(Expr::Value(Value::Number("0".to_owned(), false)));

                let query = query.to_string();
                let result_set = self.query(&query).await?;
                let schema = MySchema::from_columns(result_set.columns_ref());

                // log the schema
                tracing::info!("[mysql] schema: {:?}", schema);

                Ok(Some(schema.schema()))
            }
            Statement::Declare { query, .. } => {
                let query_stmt = Statement::Query(query.clone());
                self.describe(&query_stmt).await
            }
            _ => PgWireResult::Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                "only SELECT statements are supported in bigquery".to_owned(),
            )))),
        }
    }
}

