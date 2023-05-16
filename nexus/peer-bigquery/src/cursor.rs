use std::collections::HashMap;
use tokio::sync::Mutex;

use futures::StreamExt;
use peer_cursor::{QueryExecutor, QueryOutput, Record, Records, SchemaRef, SendableStream};
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
                let mut cursors = self.cursors.lock().await;
                cursors.insert(name.to_string(), cursor);

                // log the cursor and statement
                println!("Created cursor {} for statement '{}'", name, stmt);

                // log all the stored cursor names
                println!("Stored cursors: {:?}", cursors.keys());

                Ok(())
            }
            _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                "Only SELECT queries can be used with cursors".to_owned(),
            )))),
        }
    }

    pub async fn fetch(&self, name: &str, count: usize) -> PgWireResult<Records> {
        let mut cursors = self.cursors.lock().await;

        // log all the stored cursor names
        println!("Stored cursors: {:?}", cursors.keys());

        let cursor = cursors.get_mut(name).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                format!("[bigquery] Cursor {} does not exist", name),
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

        Ok(Records {
            records,
            schema: cursor.schema.clone(),
        })
    }

    pub async fn close(&self, name: &str) -> PgWireResult<()> {
        let mut cursors = self.cursors.lock().await;

        // log that we are removing the cursor from bq
        println!("Removing cursor {} from BigQuery", name);

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

    // close all the cursors
    pub async fn close_all_cursors(&self) -> PgWireResult<Vec<String>> {
        let mut cursors = self.cursors.lock().await;

        // log that we are removing all the cursors from bq
        println!("Removing all cursors from BigQuery");

        let names = cursors.keys().cloned().collect::<Vec<_>>();
        cursors.clear();
        Ok(names)
    }
}
