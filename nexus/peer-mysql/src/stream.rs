use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::Stream;
use peer_cursor::{Record, RecordStream, Schema};
use pgwire::{
    api::{
        results::{FieldFormat, FieldInfo},
        Type,
    },
    error::{PgWireError, PgWireResult},
};
use mysql_async::prelude::Queryable;
use mysql_async::{Column, ResultSetStream, Row, TextProtocol};
use mysql_async::consts::ColumnType;
use value::Value;

#[derive(Debug)]
pub struct MySchema {
    schema: Schema,
}

pub struct MyRecordStream {
    conn: mysql_async::Conn,
    results: Pin<Box<Vec<Row>>>,
    schema: MySchema,
    idx: usize,
}

// convert ColumnType to pgwire FieldInfo's Type
fn convert_field_type(field_type: ColumnType) -> Type {
    match field_type {
        ColumnType::MYSQL_TYPE_NULL => Type::VOID,
        ColumnType::MYSQL_TYPE_BIT => Type::BOOL,
        ColumnType::MYSQL_TYPE_FLOAT => Type::FLOAT4,
        ColumnType::MYSQL_TYPE_DOUBLE => Type::FLOAT8,
        ColumnType::MYSQL_TYPE_TINY => Type::INT2,
        ColumnType::MYSQL_TYPE_SHORT => Type::INT2,
        ColumnType::MYSQL_TYPE_INT24 => Type::INT4,
        ColumnType::MYSQL_TYPE_LONG => Type::INT4,
        ColumnType::MYSQL_TYPE_LONGLONG => Type::INT8,
        ColumnType::MYSQL_TYPE_DECIMAL |
        ColumnType::MYSQL_TYPE_NEWDECIMAL
            => Type::NUMERIC,
        ColumnType::MYSQL_TYPE_VARCHAR => Type::TEXT,
        ColumnType::MYSQL_TYPE_TINY_BLOB |
        ColumnType::MYSQL_TYPE_MEDIUM_BLOB |
        ColumnType::MYSQL_TYPE_LONG_BLOB |
        ColumnType::MYSQL_TYPE_BLOB => Type::BYTEA,
        ColumnType::MYSQL_TYPE_TIMESTAMP => Type::TIMESTAMP,
        ColumnType::MYSQL_TYPE_GEOMETRY => Type::POLYGON_ARRAY,
        ColumnType::MYSQL_TYPE_JSON => Type::JSON,
        _ => todo!(),
    }
}

impl MySchema {
    pub fn from_columns(columns: &[Column]) -> Self {
        let schema = Arc::new(
            columns
                .iter()
                .map(|column| {
                    let datatype = convert_field_type(column.column_type());
                    FieldInfo::new(column.name_str().into_owned(), None, None, datatype, FieldFormat::Text)
                })
                .collect(),
        );

        Self { schema }
    }

    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }
}

impl MyRecordStream {
    pub async fn query(mut conn: mysql_async::Conn, query: String) -> PgWireResult<Self> {
        // TODO query_stream
        // let results: mysql_async::ResultSetStream<'static, 'static, 'static, Row, TextProtocol> = conn.query_stream(query).await.map_err(|err| PgWireError::ApiError(err.into()))?;
        let results: mysql_async::QueryResult<'_, 'static, mysql_async::TextProtocol> = conn.query_iter(query).await.map_err(|err| PgWireError::ApiError(err.into()))?;
        let my_schema = MySchema::from_columns(results.columns_ref());
        let results = results.collect_and_drop::<Row>().await.map_err(|err| PgWireError::ApiError(err.into()))?;

        Ok(Self {
            conn,
            results: Box::pin(results),
            schema: my_schema,
            idx: 0,
        })
    }

    /*
    pub fn convert_result_set_item(&self, result_set: &ResultSet) -> anyhow::Result<Record> {
        let mut values = Vec::with_capacity(self.schema.fields.len());
        for field in &self.schema.fields {
            let field_type = &field.r#type;
            let field_name = &field.name;

            let value = match result_set.get_json_value_by_name(&field.name)? {
                _ => todo!(),
            };
            // values.push(value.unwrap_or(Value::Null));
        }

        Ok(Record {
            values,
            schema: self.schema.schema(),
        })
    }
    */
}

// TODO cleanup unwrap
pub fn mysql_row_to_values(row: &Row) -> Vec<Value> {
    row.columns_ref().iter().enumerate().map(|(i, col)| {
        match col.column_type() {
            ColumnType::MYSQL_TYPE_NULL => Value::Null,
            ColumnType::MYSQL_TYPE_BIT => Value::Bool(row.get(i).unwrap()),
            ColumnType::MYSQL_TYPE_FLOAT => Value::Float(row.get(i).unwrap()),
            ColumnType::MYSQL_TYPE_DOUBLE => Value::Double(row.get(i).unwrap()),
            // TODO rest of types
            _ => Value::Null,
        }
    }).collect()
}

impl Stream for MyRecordStream {
    type Item = PgWireResult<Record>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let idx = self.idx;
        self.idx += 1;
        if idx >= self.results.len() {
            Poll::Ready(None)
        } else {
            Poll::Ready(Some(Ok(Record{
                schema: self.schema(),
                values: mysql_row_to_values(&self.results[idx]),
            })))
        }
    }
}

impl RecordStream for MyRecordStream {
    fn schema(&self) -> Schema {
        self.schema.schema()
    }
}
