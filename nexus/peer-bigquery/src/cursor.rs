use std::collections::HashMap;
use tokio::sync::Mutex;

use futures::StreamExt;
use peer_cursor::{QueryExecutor, QueryOutput, Record, SchemaRef, SendableStream};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use sqlparser::ast::Statement;

use crate::BigQueryQueryExecutor;

pub struct BigQueryCursor {
    stmt: Statement,
    position: usize,
    stream: SendableStream,
    schema: SchemaRef,
}

pub struct BigQueryCursorManager {
    cursors: Mutex<HashMap<String, BigQueryCursor>>,
}

impl BigQueryCursorManager {
    pub fn new() -> Self {
        Self {
            cursors: Mutex::new(HashMap::new()),
        }
    }

    pub async fn create_cursor(
        &self,
        name: &str,
        stmt: &Statement,
        executor: &BigQueryQueryExecutor,
    ) -> PgWireResult<()> {
        // Execute the query to obtain a stream of records
        let output = executor.execute(stmt).await?;

        match output {
            QueryOutput::Stream(stream) => {
                // Get the schema from the stream
                let schema = stream.schema();

                // Create a new cursor
                let cursor = BigQueryCursor {
                    stmt: stmt.clone(),
                    position: 0,
                    stream,
                    schema,
                };

                // Store the cursor
                self.cursors.lock().await.insert(name.to_string(), cursor);

                Ok(())
            }
            QueryOutput::AffectedRows(_) => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                "Cannot create a cursor for a non-SELECT statement".to_owned(),
            )))),
        }
    }

    pub async fn fetch(&self, name: &str, count: usize) -> PgWireResult<Vec<Record>> {
        let mut cursors = self.cursors.lock().await;
        let cursor = cursors.get_mut(name).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                format!("Cursor {} does not exist", name),
            )))
        })?;

        let mut records = Vec::new();
        while cursor.position < count {
            match cursor.stream.next().await {
                Some(Ok(record)) => {
                    records.push(record);
                    cursor.position += 1;
                }
                Some(Err(err)) => return Err(err),
                None => break,
            }
        }

        Ok(records)
    }

    pub async fn close(&self, name: &str) -> PgWireResult<()> {
        let mut cursors = self.cursors.lock().await;
        cursors
            .remove(name)
            .ok_or_else(|| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "fdw_error".to_owned(),
                    format!("Cursor {} does not exist", name),
                )))
            })
            .map(|_| ())
    }
}
