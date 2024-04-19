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
use mysql_async::{Column, ResultSetStream, Row, TextProtocol};
use mysql_async::consts::ColumnType;

#[derive(Debug)]
pub struct MySchema {
    schema: Schema,
}

pub struct MyRecordStream<'a> {
    result_set: Mutex<Pin<Box<ResultSetStream<'a, 'a, 'static, Row, TextProtocol>>>>,
    schema: MySchema,
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
        ColumnType::MYSQL_TYPE_DECIMAL => Type::NUMERIC,
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

impl<'a> MyRecordStream<'a> {
    pub fn new(result_set: ResultSetStream<'a, 'a, 'static, Row, TextProtocol>) -> Self {
        let my_schema = MySchema::from_columns(result_set.columns_ref());

        Self {
            result_set: Mutex::new(Box::pin(result_set)),
            schema: my_schema,
        }
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

pub fn mysql_row_to_values(_row: Row) -> Vec<value::Value> {
    // TODO
    Vec::new()
}

impl<'a> Stream for MyRecordStream<'a> {
    type Item = PgWireResult<Record>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut substream = self.result_set.lock().expect("poisoned mutex");
        substream.as_mut().poll_next(cx).map(|result| {
            result.map(|result| {
                match result {
                    Ok(row) => Ok(Record{
                        schema: self.schema(),
                        values: mysql_row_to_values(row),
                    }),
                    Err(err) => Err(PgWireError::ApiError(err.into())),
                }

            })
        })
    }
}

impl<'a> RecordStream for MyRecordStream<'a> {
    fn schema(&self) -> Schema {
        self.schema.schema()
    }
}
