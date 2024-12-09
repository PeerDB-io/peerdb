use std::error::Error;

use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use pgwire::types::ToSqlText;
use postgres_types::{IsNull, ToSql, Type};
use uuid::{fmt::Hyphenated, Uuid};

#[derive(Debug, PartialEq, Clone)]
pub enum ArrayValue {
    Empty,
    Bool(Vec<bool>),
    TinyInt(Vec<i8>),
    SmallInt(Vec<i16>),
    Integer(Vec<i32>),
    BigInt(Vec<i64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    Numeric(Vec<String>),
    Char(Vec<char>),
    VarChar(Vec<String>),
    Text(Vec<String>),
    Binary(Vec<Bytes>),
    VarBinary(Vec<Bytes>),
    Uuid(Vec<Uuid>),
    Date(Vec<NaiveDate>),
    Time(Vec<NaiveTime>),
    TimeWithTimeZone(Vec<NaiveTime>),
    Timestamp(Vec<DateTime<Utc>>),
    TimestampWithTimeZone(Vec<DateTime<Utc>>),
}

impl ArrayValue {
    pub fn to_serde_json_value(&self) -> serde_json::Value {
        match self {
            ArrayValue::Empty => serde_json::Value::Null,
            ArrayValue::Bool(arr) => {
                serde_json::Value::Array(arr.iter().map(|&v| serde_json::Value::Bool(v)).collect())
            }
            ArrayValue::TinyInt(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::Number(v.into()))
                    .collect(),
            ),
            ArrayValue::SmallInt(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::Number(v.into()))
                    .collect(),
            ),
            ArrayValue::Integer(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::Number(v.into()))
                    .collect(),
            ),
            ArrayValue::BigInt(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| {
                        serde_json::Value::Number(serde_json::Number::from_f64(v as f64).unwrap())
                    })
                    .collect(),
            ),
            ArrayValue::Float(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| {
                        serde_json::Value::Number(serde_json::Number::from_f64(v as f64).unwrap())
                    })
                    .collect(),
            ),
            ArrayValue::Double(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::Number(serde_json::Number::from_f64(v).unwrap()))
                    .collect(),
            ),
            ArrayValue::Numeric(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| serde_json::Value::String(v.clone()))
                    .collect(),
            ),
            ArrayValue::Char(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_string()))
                    .collect(),
            ),
            ArrayValue::VarChar(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| serde_json::Value::String(v.clone()))
                    .collect(),
            ),
            ArrayValue::Binary(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| serde_json::Value::String(hex::encode(v)))
                    .collect(),
            ),
            ArrayValue::VarBinary(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| serde_json::Value::String(hex::encode(v)))
                    .collect(),
            ),
            ArrayValue::Uuid(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| serde_json::Value::String(v.to_string()))
                    .collect(),
            ),
            ArrayValue::Date(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_string()))
                    .collect(),
            ),
            ArrayValue::Time(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_string()))
                    .collect(),
            ),
            ArrayValue::TimeWithTimeZone(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_string()))
                    .collect(),
            ),
            ArrayValue::Timestamp(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_rfc3339()))
                    .collect(),
            ),
            ArrayValue::TimestampWithTimeZone(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|&v| serde_json::Value::String(v.to_rfc3339()))
                    .collect(),
            ),
            ArrayValue::Text(arr) => serde_json::Value::Array(
                arr.iter()
                    .map(|v| serde_json::Value::String(v.clone()))
                    .collect(),
            ),
        }
    }
}

impl ToSql for ArrayValue {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            ArrayValue::Bool(arr) => arr.to_sql(ty, out)?,
            ArrayValue::TinyInt(arr) => arr.to_sql(ty, out)?,
            ArrayValue::SmallInt(arr) => arr.to_sql(ty, out)?,
            ArrayValue::Integer(arr) => arr.to_sql(ty, out)?,
            ArrayValue::BigInt(arr) => arr.to_sql(ty, out)?,
            ArrayValue::Float(arr) => arr.to_sql(ty, out)?,
            ArrayValue::Double(arr) => arr.to_sql(ty, out)?,
            ArrayValue::Numeric(arr) => arr.to_sql(ty, out)?,
            ArrayValue::Char(arr) => {
                let stringified: Vec<i8> = arr.iter().map(|c| *c as i8).collect();
                stringified.to_sql(ty, out)?
            }
            ArrayValue::VarChar(arr) => arr.to_sql(ty, out)?,
            ArrayValue::Text(arr) => arr.to_sql(ty, out)?,
            ArrayValue::Binary(_arr) => todo!("support encoding array of binary"),
            ArrayValue::VarBinary(_arr) => todo!("support encoding array of varbinary"),
            ArrayValue::Uuid(arr) => arr.to_sql(ty, out)?,
            ArrayValue::Date(arr) => arr.to_sql(ty, out)?,
            ArrayValue::Time(arr) => arr.to_sql(ty, out)?,
            ArrayValue::TimeWithTimeZone(arr) => arr.to_sql(ty, out)?,
            ArrayValue::Timestamp(arr) => arr.to_sql(ty, out)?,
            ArrayValue::TimestampWithTimeZone(arr) => arr.to_sql(ty, out)?,
            ArrayValue::Empty => IsNull::Yes,
        };

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(
            *ty,
            Type::BOOL_ARRAY
                | Type::INT2_ARRAY
                | Type::INT4_ARRAY
                | Type::INT8_ARRAY
                | Type::FLOAT4_ARRAY
                | Type::FLOAT8_ARRAY
                | Type::NUMERIC_ARRAY
                | Type::CHAR_ARRAY
                | Type::VARCHAR_ARRAY
                | Type::TEXT_ARRAY
                | Type::BYTEA_ARRAY
                | Type::DATE_ARRAY
                | Type::TIME_ARRAY
                | Type::TIMETZ_ARRAY
                | Type::TIMESTAMP_ARRAY
                | Type::TIMESTAMPTZ_ARRAY
        )
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if !<Self as ToSql>::accepts(ty) {
            return Err("Invalid type".into());
        }

        ToSql::to_sql(self, ty, out)
    }
}

macro_rules! array_to_sql_text {
    ($arr:expr, $ty:expr, $out:expr) => {{
        for v in $arr {
            v.to_sql_text($ty, $out)?;
            $out.put_slice(b",");
        }
    }};
}

impl ToSqlText for ArrayValue {
    fn to_sql_text(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // We start array values with '{'
        out.put_slice(b"{");

        match self {
            ArrayValue::Bool(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::TinyInt(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::SmallInt(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::Integer(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::BigInt(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::Float(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::Double(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::Numeric(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::Char(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::VarChar(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::Text(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::Binary(_arr) => todo!("implement encoding array of binary"),
            ArrayValue::VarBinary(_arr) => todo!("implement encoding array of varbinary"),
            ArrayValue::Uuid(arr) => {
                let mut buf = [0u8; Hyphenated::LENGTH];
                for v in arr {
                    out.put_slice(b"'");
                    out.put_slice(v.hyphenated().encode_lower(&mut buf).as_bytes());
                    out.put_slice(b"',");
                }
            }
            ArrayValue::Date(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::Time(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::TimeWithTimeZone(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::Timestamp(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::TimestampWithTimeZone(arr) => array_to_sql_text!(arr, ty, out),
            ArrayValue::Empty => {}
        }

        // remove trailing comma
        if out.last() == Some(&b',') {
            out.truncate(out.len() - 1);
        }

        // We end array values with '}'
        out.put_slice(b"}");

        Ok(IsNull::No)
    }
}
