use crate::{auth::SnowflakeAuth, PartitionResult, ResultSet};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::Stream;
use peer_cursor::Schema;
use peer_cursor::{Record, RecordStream, SchemaRef};
use pgerror::PgError;
use pgwire::{
    api::{
        results::{FieldFormat, FieldInfo},
        Type,
    },
    error::{PgWireError, PgWireResult},
};
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde_json;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use value::Value::{
    self, BigInt, Binary, Bool, Date, Float, PostgresTimestamp, Text, Time, TimestampWithTimeZone,
};

#[derive(Clone, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SnowflakeDataType {
    Fixed,
    Real,
    Text,
    Binary,
    Boolean,
    Date,
    Time,
    #[serde(rename = "timestamp_ltz")]
    TimestampLtz,
    #[serde(rename = "timestamp_ntz")]
    TimestampNtz,
    #[serde(rename = "timestamp_tz")]
    TimestampTz,
    Variant,
}

pub struct SnowflakeSchema {
    schema: SchemaRef,
}

fn convert_field_type(field_type: &SnowflakeDataType) -> Type {
    match field_type {
        SnowflakeDataType::Fixed => Type::NUMERIC,
        SnowflakeDataType::Real => Type::FLOAT8,
        SnowflakeDataType::Text => Type::TEXT,
        SnowflakeDataType::Binary => Type::BYTEA,
        SnowflakeDataType::Boolean => Type::BOOL,
        SnowflakeDataType::Date => Type::DATE,
        SnowflakeDataType::Time => Type::TIME,
        SnowflakeDataType::TimestampLtz => Type::TIMESTAMPTZ,
        SnowflakeDataType::TimestampNtz => Type::TIMESTAMP,
        SnowflakeDataType::TimestampTz => Type::TIMESTAMPTZ,
        SnowflakeDataType::Variant => Type::JSONB,
    }
}

impl SnowflakeSchema {
    pub fn from_result_set(result_set: &ResultSet) -> Self {
        let fields = result_set.resultSetMetaData.rowType.clone();

        let schema = SchemaRef::new(Schema {
            fields: fields
                .iter()
                .map(|field| {
                    let datatype = convert_field_type(&field.r#type);
                    FieldInfo::new(field.name.clone(), None, None, datatype, FieldFormat::Text)
                })
                .collect(),
        });

        Self { schema }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub struct SnowflakeRecordStream {
    result_set: ResultSet,
    partition_index: usize,
    partition_number: usize,
    schema: SnowflakeSchema,
    auth: SnowflakeAuth,

    endpoint_url: String,
}

impl SnowflakeRecordStream {
    pub fn new(
        result_set: ResultSet,
        partition_index: usize,
        partition_number: usize,
        endpoint_url: String,
        auth: SnowflakeAuth,
    ) -> Self {
        let sf_schema = SnowflakeSchema::from_result_set(&result_set);

        Self {
            result_set,
            schema: sf_schema,
            partition_index,
            partition_number,
            endpoint_url,
            auth,
        }
    }

    pub fn convert_result_set_item(&mut self) -> anyhow::Result<Record> {
        let mut row_values = Vec::new();

        for (index, value) in self.result_set.data[self.partition_index]
            .iter()
            .enumerate()
        {
            const DATE_PARSE_FORMAT: &str = "%Y/%m/%d";
            const TIME_PARSE_FORMAT: &str = "%H:%M:%S.%9f";
            const TIMESTAMP_PARSE_FORMAT: &str = "%FT%T.%9f";
            const TIMESTAMP_TZ_PARSE_FORMAT: &str = "%FT%T%.9f%z";
            let row_value = match value {
                None => None,
                Some(elem) => Some(
                    match self.result_set.resultSetMetaData.rowType[index].r#type {
                        SnowflakeDataType::Fixed => match elem.parse::<i64>() {
                            Ok(_) => BigInt(elem.parse()?),
                            Err(_) => Text(elem.to_string()),
                        },
                        SnowflakeDataType::Real => Float(elem.parse()?),
                        SnowflakeDataType::Text => Text(elem.to_string()),
                        SnowflakeDataType::Binary => Binary(hex::decode(elem)?.into()),
                        SnowflakeDataType::Boolean => Bool(elem.parse()?),
                        SnowflakeDataType::Date => {
                            println!("Entered Date. elem: {:#?}", elem);
                            Date(NaiveDate::parse_from_str(elem, DATE_PARSE_FORMAT)?)
                        }
                        SnowflakeDataType::Time => {
                            Time(NaiveTime::parse_from_str(elem, TIME_PARSE_FORMAT)?)
                        }
                        // really hacky workaround for parsing the UTC timezone specifically.
                        SnowflakeDataType::TimestampLtz => {
                            match DateTime::parse_from_str(elem, TIMESTAMP_TZ_PARSE_FORMAT) {
                                Ok(_) => TimestampWithTimeZone(DateTime::<Utc>::from_utc(
                                    DateTime::parse_from_str(elem, TIMESTAMP_TZ_PARSE_FORMAT)?
                                        .naive_utc(),
                                    Utc,
                                )),
                                Err(_) => TimestampWithTimeZone(DateTime::<Utc>::from_utc(
                                    DateTime::parse_from_str(
                                        &elem.replace("Z", "+0000"),
                                        TIMESTAMP_TZ_PARSE_FORMAT,
                                    )?
                                    .naive_utc(),
                                    Utc,
                                )),
                            }
                        }
                        SnowflakeDataType::TimestampNtz => PostgresTimestamp(
                            NaiveDateTime::parse_from_str(elem, TIMESTAMP_PARSE_FORMAT)?,
                        ),
                        SnowflakeDataType::TimestampTz => {
                            match DateTime::parse_from_str(elem, TIMESTAMP_TZ_PARSE_FORMAT) {
                                Ok(_) => TimestampWithTimeZone(DateTime::<Utc>::from_utc(
                                    DateTime::parse_from_str(elem, TIMESTAMP_TZ_PARSE_FORMAT)?
                                        .naive_utc(),
                                    Utc,
                                )),
                                Err(_) => TimestampWithTimeZone(DateTime::<Utc>::from_utc(
                                    DateTime::parse_from_str(
                                        &elem.replace("Z", "+0000"),
                                        TIMESTAMP_TZ_PARSE_FORMAT,
                                    )?
                                    .naive_utc(),
                                    Utc,
                                )),
                            }
                        }
                        SnowflakeDataType::Variant => {
                            let jsonb: serde_json::Value = serde_json::from_str(&elem)?;
                            Value::JsonB(jsonb)
                        }
                    },
                ),
            };

            row_values.push(row_value.unwrap_or(Value::Null));
        }

        self.partition_index = self.partition_index + 1;

        Ok(Record {
            values: row_values,
            schema: self.schema.schema(),
        })
    }

    fn advance_partition(&mut self) -> anyhow::Result<bool> {
        if (self.partition_number + 1) == self.result_set.resultSetMetaData.partitionInfo.len() {
            return Ok(false);
        }
        self.partition_number = self.partition_number + 1;
        self.partition_index = 0;
        let partition_number = self.partition_number;
        let secret = self.auth.get_jwt().expose_secret().clone();
        let statement_handle = self.result_set.statementHandle.clone();
        let url = self.endpoint_url.clone();
        println!("Secret: {:#?}", secret);
        let response: PartitionResult = ureq::get(&format!("{}/{}", url, statement_handle))
            .query("partition", &partition_number.to_string())
            .set("Authorization", &format!("Bearer {}", secret))
            .set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .set("user-agent", "ureq")
            .call()?
            .into_json()
            .map_err(|_| anyhow::anyhow!("get_partition failed"))?;
        println!("Response: {:#?}", response.data);

        self.result_set.data = response.data;
        Ok(true)
    }

    fn advance(&mut self) -> anyhow::Result<bool> {
        Ok((self.partition_index < self.result_set.data.len()) || self.advance_partition()?)
    }
}

impl Stream for SnowflakeRecordStream {
    type Item = PgWireResult<Record>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.advance() {
            Ok(true) => {
                let record = this.convert_result_set_item();
                let result = record.map_err(|e| {
                    PgWireError::ApiError(Box::new(PgError::Internal {
                        err_msg: format!("error getting current row: {}", e),
                    }))
                });
                Poll::Ready(Some(result))
            }
            Ok(false) => Poll::Ready(None),
            Err(err) => Poll::Ready(Some(Err(PgWireError::ApiError(Box::new(
                PgError::Internal {
                    err_msg: format!("Checking for next row in result set failed: {}", err),
                },
            ))))),
        }
    }
}

impl RecordStream for SnowflakeRecordStream {
    fn schema(&self) -> SchemaRef {
        self.schema.schema()
    }
}
