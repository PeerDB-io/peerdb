use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::client::{self, MyClient};
use futures::Stream;
use mysql_async::consts::ColumnType;
use mysql_async::{Column, Row};
use peer_cursor::{Record, RecordStream, Schema};
use pgwire::{
    api::{
        results::{FieldFormat, FieldInfo},
        Type,
    },
    error::{PgWireError, PgWireResult},
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use value::Value;

pub struct MyRecordStream {
    schema: Schema,
    stream: ReceiverStream<client::Response>,
}

// convert ColumnType to pgwire FieldInfo's Type
fn convert_field_type(field_type: ColumnType) -> Type {
    match field_type {
        ColumnType::MYSQL_TYPE_NULL | ColumnType::MYSQL_TYPE_UNKNOWN => Type::VOID,
        ColumnType::MYSQL_TYPE_FLOAT => Type::FLOAT4,
        ColumnType::MYSQL_TYPE_DOUBLE => Type::FLOAT8,
        ColumnType::MYSQL_TYPE_YEAR => Type::INT2,
        ColumnType::MYSQL_TYPE_TINY => Type::INT2,
        ColumnType::MYSQL_TYPE_SHORT => Type::INT2,
        ColumnType::MYSQL_TYPE_INT24 => Type::INT4,
        ColumnType::MYSQL_TYPE_LONG => Type::INT4,
        ColumnType::MYSQL_TYPE_LONGLONG => Type::INT8,
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => Type::NUMERIC,
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING
        | ColumnType::MYSQL_TYPE_ENUM
        | ColumnType::MYSQL_TYPE_SET => Type::TEXT,
        ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_BLOB
        | ColumnType::MYSQL_TYPE_BIT
        | ColumnType::MYSQL_TYPE_VECTOR
        | ColumnType::MYSQL_TYPE_GEOMETRY => Type::BYTEA,
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => Type::DATE,
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => Type::TIME,
        ColumnType::MYSQL_TYPE_TIMESTAMP
        | ColumnType::MYSQL_TYPE_TIMESTAMP2
        | ColumnType::MYSQL_TYPE_DATETIME
        | ColumnType::MYSQL_TYPE_DATETIME2 => Type::TIMESTAMP,
        ColumnType::MYSQL_TYPE_JSON => Type::JSONB,
        ColumnType::MYSQL_TYPE_TYPED_ARRAY => Type::VOID,
    }
}

pub fn schema_from_columns(columns: &[Column]) -> Schema {
    Arc::new(
        columns
            .iter()
            .map(|column| {
                let datatype = convert_field_type(column.column_type());
                FieldInfo::new(
                    column.name_str().into_owned(),
                    None,
                    None,
                    datatype,
                    FieldFormat::Text,
                )
            })
            .collect(),
    )
}

impl MyRecordStream {
    pub async fn query(conn: MyClient, query: String) -> PgWireResult<Self> {
        let (send, mut recv) = mpsc::channel::<client::Response>(1);
        conn.chan
            .send(client::Message {
                query,
                response: send,
            })
            .await
            .ok();

        if let Some(first) = recv.recv().await {
            match first {
                client::Response::Row(..) => panic!("row received without schema"),
                client::Response::Schema(schema) => Ok(MyRecordStream {
                    schema: schema_from_columns(&schema),
                    stream: ReceiverStream::new(recv),
                }),
                client::Response::Err(err) => Err(PgWireError::ApiError(err.into())),
            }
        } else {
            Err(PgWireError::InvalidStartupMessage)
        }
    }
}

pub fn mysql_row_to_values(row: Row) -> Vec<Value> {
    use mysql_async::from_value;
    let columns = row.columns();
    row.unwrap()
        .into_iter()
        .zip(columns.iter())
        .map(|(val, col)| {
            if val == mysql_async::Value::NULL {
                Value::Null
            } else {
                match col.column_type() {
                    ColumnType::MYSQL_TYPE_NULL | ColumnType::MYSQL_TYPE_UNKNOWN => Value::Null,
                    ColumnType::MYSQL_TYPE_TINY => Value::TinyInt(from_value(val)),
                    ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                        Value::SmallInt(from_value(val))
                    }
                    ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                        Value::Integer(from_value(val))
                    }
                    ColumnType::MYSQL_TYPE_LONGLONG => Value::BigInt(from_value(val)),
                    ColumnType::MYSQL_TYPE_FLOAT => Value::Float(from_value(val)),
                    ColumnType::MYSQL_TYPE_DOUBLE => Value::Double(from_value(val)),
                    ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                        Value::Numeric(from_value(val))
                    }
                    ColumnType::MYSQL_TYPE_VARCHAR
                    | ColumnType::MYSQL_TYPE_VAR_STRING
                    | ColumnType::MYSQL_TYPE_STRING
                    | ColumnType::MYSQL_TYPE_ENUM
                    | ColumnType::MYSQL_TYPE_SET => Value::Text(from_value(val)),
                    ColumnType::MYSQL_TYPE_TINY_BLOB
                    | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                    | ColumnType::MYSQL_TYPE_LONG_BLOB
                    | ColumnType::MYSQL_TYPE_BLOB
                    | ColumnType::MYSQL_TYPE_BIT
                    | ColumnType::MYSQL_TYPE_VECTOR
                    | ColumnType::MYSQL_TYPE_GEOMETRY => {
                        Value::Binary(from_value::<Vec<u8>>(val).into())
                    }
                    ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
                        Value::Date(from_value(val))
                    }
                    ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
                        Value::Time(from_value(val))
                    }
                    ColumnType::MYSQL_TYPE_TIMESTAMP
                    | ColumnType::MYSQL_TYPE_TIMESTAMP2
                    | ColumnType::MYSQL_TYPE_DATETIME
                    | ColumnType::MYSQL_TYPE_DATETIME2 => Value::PostgresTimestamp(from_value(val)),
                    ColumnType::MYSQL_TYPE_JSON => Value::JsonB(from_value(val)),
                    ColumnType::MYSQL_TYPE_TYPED_ARRAY => Value::Null,
                }
            }
        })
        .collect()
}

impl Stream for MyRecordStream {
    type Item = PgWireResult<Record>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let row_stream = &mut self.stream;
        match Pin::new(row_stream).poll_next(cx) {
            Poll::Ready(Some(client::Response::Row(row))) => Poll::Ready(Some(Ok(Record {
                schema: self.schema.clone(),
                values: mysql_row_to_values(row),
            }))),
            Poll::Ready(Some(client::Response::Schema(..))) => Poll::Ready(Some(Err(
                PgWireError::ApiError("second schema received".into()),
            ))),
            Poll::Ready(Some(client::Response::Err(e))) => {
                Poll::Ready(Some(Err(PgWireError::ApiError(e.into()))))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordStream for MyRecordStream {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }
}
