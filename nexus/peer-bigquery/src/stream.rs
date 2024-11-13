use std::{
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Context, Poll},
};

use chrono::DateTime;
use futures::Stream;
use gcp_bigquery_client::model::{
    field_type::FieldType,
    query_response::{QueryResponse, ResultSet},
    table_field_schema::TableFieldSchema,
};
use peer_cursor::{Record, RecordStream, Schema};
use pgwire::{
    api::{
        results::{FieldFormat, FieldInfo},
        Type,
    },
    error::{PgWireError, PgWireResult},
};
use rust_decimal::Decimal;
use value::Value;

#[derive(Debug)]
pub struct BqSchema {
    schema: Schema,
    fields: Vec<TableFieldSchema>,
}

pub struct BqRecordStream {
    result_set: ResultSet,
    schema: BqSchema,
    num_records: usize,
}

// convert FieldType to pgwire FieldInfo's Type
fn convert_field_type(field_type: &FieldType) -> Type {
    match field_type {
        FieldType::Bool => Type::BOOL,
        FieldType::Bytes => Type::BYTEA,
        FieldType::Date => Type::DATE,
        FieldType::Datetime => Type::TIMESTAMP,
        FieldType::Float64 => Type::FLOAT8,
        FieldType::Int64 => Type::INT8,
        FieldType::Numeric => Type::NUMERIC,
        FieldType::String => Type::TEXT,
        FieldType::Integer => Type::INT4,
        FieldType::Float => Type::FLOAT4,
        FieldType::Bignumeric => Type::NUMERIC,
        FieldType::Boolean => Type::BOOL,
        FieldType::Timestamp => Type::TIMESTAMP,
        FieldType::Time => Type::TIME,
        FieldType::Record => Type::JSONB,
        FieldType::Struct => Type::JSONB,
        FieldType::Geography => Type::POLYGON_ARRAY,
        FieldType::Json => Type::JSON,
    }
}

impl From<&QueryResponse> for BqSchema {
    fn from(query_response: &QueryResponse) -> Self {
        let bq_schema = query_response
            .schema
            .as_ref()
            .expect("Schema is not present");
        let fields = bq_schema
            .fields
            .as_ref()
            .expect("Schema fields are not present");

        let schema = Arc::new(
            fields
                .iter()
                .map(|field| {
                    let datatype = convert_field_type(&field.r#type);
                    FieldInfo::new(field.name.clone(), None, None, datatype, FieldFormat::Text)
                })
                .collect(),
        );

        Self {
            schema,
            fields: fields.clone(),
        }
    }
}

impl BqSchema {
    pub fn schema(&self) -> Schema {
        self.schema.clone()
    }
}

impl From<QueryResponse> for BqRecordStream {
    fn from(query_response: QueryResponse) -> Self {
        let schema = BqSchema::from(&query_response);
        let result_set = ResultSet::new_from_query_response(query_response);
        let num_records = result_set.row_count();

        Self {
            result_set,
            schema,
            num_records,
        }
    }
}

impl BqRecordStream {
    pub fn get_num_records(&self) -> usize {
        self.num_records
    }

    pub fn convert_result_set_item(&self, result_set: &ResultSet) -> anyhow::Result<Record> {
        let mut values = Vec::with_capacity(self.schema.fields.len());
        for field in &self.schema.fields {
            let field_type = &field.r#type;
            let field_name = &field.name;

            let value = match result_set.get_json_value_by_name(&field.name)? {
                Some(serde_json::Value::Array(mut arr)) => {
                    for item in arr.iter_mut() {
                        if let Some(obj) = item.as_object_mut() {
                            if let Some(value) = obj.remove("v") {
                                *item = value;
                            }
                        }
                    }

                    Some(Value::from_serde_json_value(&serde_json::Value::Array(arr)))
                }
                _ => match field_type {
                    FieldType::String => {
                        result_set.get_string_by_name(field_name)?.map(Value::Text)
                    }
                    FieldType::Bytes => result_set
                        .get_string_by_name(field_name)?
                        .map(|s| Value::Binary(s.into_bytes().into())),
                    FieldType::Int64 | FieldType::Integer => {
                        result_set.get_i64_by_name(field_name)?.map(Value::BigInt)
                    }
                    FieldType::Float | FieldType::Float64 => {
                        result_set.get_f64_by_name(field_name)?.map(Value::Double)
                    }
                    FieldType::Bignumeric | FieldType::Numeric => {
                        let result_string = result_set.get_string_by_name(field_name)?;
                        if let Some(result) = result_string {
                            let decimal = Decimal::from_str(&result)?;
                            Some(Value::Numeric(decimal))
                        } else {
                            None
                        }
                    }
                    FieldType::Boolean | FieldType::Bool => {
                        result_set.get_bool_by_name(field_name)?.map(Value::Bool)
                    }
                    FieldType::Date | FieldType::Datetime | FieldType::Time => {
                        result_set.get_string_by_name(field_name)?.map(Value::Text)
                    }
                    FieldType::Timestamp => {
                        let timestamp = result_set.get_i64_by_name(field_name)?;
                        if let Some(ts) = timestamp {
                            let datetime = DateTime::from_timestamp(ts, 0)
                                .ok_or(anyhow::Error::msg("Invalid datetime"))?;
                            Some(Value::Timestamp(datetime))
                        } else {
                            None
                        }
                    }
                    FieldType::Record => todo!(),
                    FieldType::Struct => todo!(),
                    FieldType::Geography => todo!(),
                    FieldType::Json => todo!(),
                },
            };
            values.push(value.unwrap_or(Value::Null));
        }

        Ok(Record {
            values,
            schema: self.schema.schema(),
        })
    }
}

impl Stream for BqRecordStream {
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

impl RecordStream for BqRecordStream {
    fn schema(&self) -> Schema {
        self.schema.schema()
    }
}
