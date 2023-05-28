use std::sync::Arc;

use peer_cursor::{QueryExecutor, QueryOutput, Schema, SchemaRef};
use pgerror::PgError;
use pgwire::{
    api::results::{FieldFormat, FieldInfo},
    error::{PgWireError, PgWireResult},
};
use pt::peers::PostgresConfig;
use sqlparser::ast::Statement;
use tokio_postgres::Client;

mod ast;
mod stream;

// PostgresQueryExecutor is a QueryExecutor that uses a Postgres database as its
// backing store.
pub struct PostgresQueryExecutor {
    _config: PostgresConfig,
    peername: Option<String>,
    client: Box<Client>,
}

fn get_connection_string(config: &PostgresConfig) -> String {
    let mut connection_string = String::new();
    connection_string.push_str("host=");
    connection_string.push_str(&config.host);
    connection_string.push_str(" port=");
    connection_string.push_str(&config.port.to_string());
    connection_string.push_str(" user=");
    connection_string.push_str(&config.user);
    if !config.password.is_empty() {
        connection_string.push_str(" password=");
        connection_string.push_str(&config.password);
    }
    connection_string.push_str(" dbname=");
    connection_string.push_str(&config.database);
    connection_string
}

impl PostgresQueryExecutor {
    pub async fn new(peername: Option<String>, config: &PostgresConfig) -> anyhow::Result<Self> {
        let connection_string = get_connection_string(config);

        let (client, connection) =
            tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("error encountered while connecting to postgres {:?}", e)
                })?;

        tokio::task::Builder::new()
            .name("PostgresQueryExecutor connection")
            .spawn(async move {
                if let Err(e) = connection.await {
                    tracing::info!("connection error: {}", e)
                }
            })?;

        Ok(Self {
            _config: config.clone(),
            peername,
            client: Box::new(client),
        })
    }

    pub async fn schema_from_query(&self, query: &str) -> anyhow::Result<SchemaRef> {
        let prepared = self.client.prepare_typed(query, &[]).await?;

        let fields: Vec<FieldInfo> = prepared
            .columns()
            .iter()
            .map(|c| {
                let name = c.name().to_string();
                FieldInfo::new(name, None, None, c.type_().clone(), FieldFormat::Text)
            })
            .collect();

        Ok(Arc::new(Schema { fields }))
    }
}

#[async_trait::async_trait]
impl QueryExecutor for PostgresQueryExecutor {
    #[tracing::instrument(skip(self, stmt), fields(stmt = %stmt))]
    async fn execute(&self, stmt: &Statement) -> PgWireResult<QueryOutput> {
        // if the query is a select statement, we need to fetch the rows
        // and return them as a QueryOutput::Stream, else we return the
        // number of affected rows.
        match stmt {
            Statement::Query(query) => {
                let ast = ast::PostgresAst {
                    peername: self.peername.clone(),
                };

                let mut query = query.clone();
                ast.rewrite(&mut query);
                let rewritten_query = query.to_string();

                // first fetch the schema as this connection will be
                // short lived, only then run the query as the query
                // could hold the pin on the connection for a long time.
                let schema = self
                    .schema_from_query(&rewritten_query)
                    .await
                    .map_err(|e| {
                        PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: format!("error getting schema: {}", e),
                        }))
                    })?;

                tracing::info!("[peer-postgres] rewritten query: {}", rewritten_query);
                // given that there could be a lot of rows returned, we
                // need to use a cursor to stream the rows back to the
                // client.
                let stream = self
                    .client
                    .query_raw(&rewritten_query, std::iter::empty::<&str>())
                    .await
                    .map_err(|e| {
                        PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: format!("error executing query: {}", e),
                        }))
                    })?;

                // log that raw query execution has completed
                tracing::info!("[peer-postgres] raw query execution completed");

                let cursor = stream::PgRecordStream::new(stream, schema);
                Ok(QueryOutput::Stream(Box::pin(cursor)))
            }
            _ => {
                let query_str = stmt.to_string();
                let rows_affected = self.client.execute(&query_str, &[]).await.map_err(|e| {
                    PgWireError::ApiError(Box::new(PgError::Internal {
                        err_msg: format!("error executing query: {}", e),
                    }))
                })?;
                Ok(QueryOutput::AffectedRows(rows_affected as usize))
            }
        }
    }

    async fn describe(&self, stmt: &Statement) -> PgWireResult<Option<SchemaRef>> {
        match stmt {
            Statement::Query(_query) => {
                let schema = self
                    .schema_from_query(&stmt.to_string())
                    .await
                    .map_err(|e| {
                        PgWireError::ApiError(Box::new(PgError::Internal {
                            err_msg: format!("error getting schema: {}", e),
                        }))
                    })?;
                Ok(Some(schema))
            }
            _ => Ok(None),
        }
    }
}
