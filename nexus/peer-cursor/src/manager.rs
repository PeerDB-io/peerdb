use dashmap::DashMap;

use futures::StreamExt;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use sqlparser::ast::Statement;

use crate::{Cursor, QueryExecutor, QueryOutput, Records};

#[derive(Default)]
pub struct CursorManager {
    cursors: DashMap<String, Cursor>,
}

impl CursorManager {
    pub async fn create_cursor(
        &self,
        name: &str,
        stmt: &Statement,
        executor: &dyn QueryExecutor,
    ) -> PgWireResult<()> {
        let output = executor.execute(stmt).await?;

        match output {
            QueryOutput::Stream(stream) => {
                let schema = stream.schema();

                let cursor = Cursor {
                    position: 0,
                    stream,
                    schema,
                };

                self.cursors.insert(name.to_string(), cursor);

                tracing::info!("Created cursor {} for statement '{}'", name, stmt);

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
        let mut cursor = self.cursors.get_mut(name).ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "fdw_error".to_owned(),
                format!("Cursor {} does not exist", name),
            )))
        })?;

        let mut records = Vec::new();
        while records.len() < count {
            match cursor.stream.next().await {
                Some(Ok(record)) => {
                    records.push(record);
                }
                Some(Err(err)) => return Err(err),
                None => break,
            }
        }

        tracing::info!("Cursor {} fetched {} records", name, records.len());
        cursor.position += records.len();

        Ok(Records {
            records,
            schema: cursor.schema.clone(),
        })
    }

    pub async fn close(&self, name: &str) -> PgWireResult<()> {
        tracing::info!("Removing cursor {}", name);

        self.cursors
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

    pub async fn close_all_cursors(&self) -> PgWireResult<Vec<String>> {
        tracing::info!("Removing all cursors");

        let keys: Vec<_> = self
            .cursors
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        self.cursors.clear();
        Ok(keys)
    }
}
