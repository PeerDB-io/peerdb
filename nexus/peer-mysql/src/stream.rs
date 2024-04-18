use std::{
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll},
};

use chrono::DateTime;
use futures::Stream;
use peer_cursor::{Record, RecordStream, Schema};
use pgwire::{
    api::{
        results::{FieldFormat, FieldInfo},
        Type,
    },
    error::{PgWireError, PgWireResult},
};
use mysql_async::QueryResult;
use mysql_async::consts::ColumnType;
use rust_decimal::Decimal;
use value::Value;

#[derive(Debug)]
pub struct MySchema {
    schema: Schema,
    fields: Vec<TableFieldSchema>,
}

pub struct MyRecordStream {
    result_set: QueryResult,
    schema: MySchema,
    num_records: usize,
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
    }
}

impl MySchema {
    pub fn from_result_set(result: QueryResult) -> Self {
        let my_schema = result.columns_ref();
        let schema = Arc::new(
            my_schema
                .iter()
                .map(|column| {
                    let datatype = convert_field_type(column.column_type());
                    FieldInfo::new(column.name_str().into_owned(), None, None, datatype, FieldFormat::Text)
                })
                .collect(),
        );

        Self {
            schema,
            fields: fields.clone(),
        }
    }

    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }
}

impl MyRecordStream {
    pub fn new(result_set: ResultSet) -> Self {
        let my_schema = MySchema::from_result_set(&result_set);
        let num_records = result_set.row_count();

        Self {
            result_set,
            schema: my_schema,
            num_records,
        }
    }

    pub fn get_num_records(&self) -> usize {
        self.num_records
    }

    pub fn convert_result_set_item(&self, result_set: &ResultSet) -> anyhow::Result<Record> {
        let mut values = Vec::with_capacity(self.schema.fields.len());
        for field in &self.schema.fields {
            let field_type = &field.r#type;
            let field_name = &field.name;

            let value = match result_set.get_json_value_by_name(&field.name)? {
                _ => todo!(),
            };
            values.push(value.unwrap_or(Value::Null));
        }

        Ok(Record {
            values,
            schema: self.schema.schema(),
        })
    }
}

impl Stream for MyRecordStream {
    type Item = PgWireResult<Record>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.result_set.next_row() {
            let record = self.convert_result_set_item(&self.result_set);
            let result = record.map_err(|e| PgWireError::ApiError(e.into()));
            Poll::Ready(Some(result))
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordStream for MyRecordStream {
    fn schema(&self) -> Schema {
        self.schema.schema()
    }
}
