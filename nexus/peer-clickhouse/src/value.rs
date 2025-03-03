use anyhow::anyhow;
use pgwire::api::Type;
use memchr::memchr;
use value::Value;

#[derive(Clone)]
pub enum ClickHouseType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Float16,
    Float32,
    Float64,
    UUID,
    IPv4,
    IPv6,
    String,
    Array(Box<ClickHouseType>),
    Nullable(Box<ClickHouseType>),
    LowCardinality(Box<ClickHouseType>),
}

impl ClickHouseType {
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<ClickHouseType> {
        if let Some(lparen) = memchr(b'(', bytes) {
            let param = &bytes[lparen+1..bytes.len()-1];
            Ok(match &bytes[..lparen] {
                b"Array" => ClickHouseType::Array(Box::new(ClickHouseType::from_bytes(param)?)),
                b"Nullable" => ClickHouseType::Nullable(Box::new(ClickHouseType::from_bytes(param)?)),
                b"LowCardinality" => ClickHouseType::LowCardinality(Box::new(ClickHouseType::from_bytes(param)?)),
                _ => return Err(anyhow!("unknown type {:?}", bytes)),
            })
        } else {
            Ok(match bytes {
                b"Bool" => ClickHouseType::Bool,
                b"Int8" => ClickHouseType::Int8,
                b"Int16" => ClickHouseType::Int16,
                b"Int32" => ClickHouseType::Int32,
                b"Int64" => ClickHouseType::Int64,
                b"BFloat16" => ClickHouseType::Float16,
                b"Float32" => ClickHouseType::Float32,
                b"Float64" => ClickHouseType::Float64,
                b"UUID" => ClickHouseType::UUID,
                b"IPv4" => ClickHouseType::IPv4,
                b"IPv6" => ClickHouseType::IPv6,
                b"String" => ClickHouseType::String,
                _ => return Err(anyhow!("unknown type {:?}", bytes)),
            })
        }
    }

    pub fn pgtype(&self) -> Type {
        match self {
            ClickHouseType::Bool => Type::BOOL,
            ClickHouseType::Int8 => Type::INT2,
            ClickHouseType::Int16 => Type::INT2,
            ClickHouseType::Int32 => Type::INT4,
            ClickHouseType::Int64 => Type::INT8,
            ClickHouseType::Float16 => Type::FLOAT4,
            ClickHouseType::Float32 => Type::FLOAT4,
            ClickHouseType::Float64 => Type::FLOAT8,
            ClickHouseType::UUID => Type::UUID,
            ClickHouseType::IPv4 | ClickHouseType::IPv6 => Type::CIDR,
            ClickHouseType::String => Type::BYTEA,
            ClickHouseType::Array(ref x) => match &**x {
                ClickHouseType::Int8 => Type::INT2_ARRAY,
                ClickHouseType::Int16 => Type::INT2_ARRAY,
                ClickHouseType::Int32 => Type::INT4_ARRAY,
                ClickHouseType::Int64 => Type::INT8_ARRAY,
                ClickHouseType::String => Type::TEXT_ARRAY,
                ClickHouseType::Nullable(x) => ClickHouseType::Array(x.clone()).pgtype(),
                ClickHouseType::LowCardinality(x) => ClickHouseType::Array(x.clone()).pgtype(),
                ClickHouseType::Array(x) => x.pgtype(),
                _ => Type::UNKNOWN,
            },
            ClickHouseType::Nullable(x) => x.pgtype(),
            ClickHouseType::LowCardinality(x) => x.pgtype(),
        }
    }

    pub fn parse(&self, bytes: &[u8]) -> anyhow::Result<Value> {
        Ok(match self {
            ClickHouseType::Bool => {
                Value::Bool(bytes == b"true")
            }
            ClickHouseType::Int8 => {
                Value::TinyInt(std::str::from_utf8(bytes)?.parse::<i8>()?)
            }
            ClickHouseType::Int16 => {
                Value::SmallInt(std::str::from_utf8(bytes)?.parse::<i16>()?)
            }
            ClickHouseType::Int32 => {
                Value::Integer(std::str::from_utf8(bytes)?.parse::<i32>()?)
            }
            ClickHouseType::Int64 => {
                Value::BigInt(std::str::from_utf8(bytes)?.parse::<i64>()?)
            }
            ClickHouseType::Float16 => {
                Value::Float(std::str::from_utf8(bytes)?.parse::<f32>()?)
            }
            ClickHouseType::Float32 => {
                Value::Float(std::str::from_utf8(bytes)?.parse::<f32>()?)
            }
            ClickHouseType::Float64 => {
                Value::Double(std::str::from_utf8(bytes)?.parse::<f64>()?)
            }
            ClickHouseType::String => {
                let mut s = Vec::with_capacity(bytes.len());
                let mut bs = bytes.iter().cloned();
                while let Some(b) = bs.next() {
                    s.push(if b == b'\\' {
                        match bs.next() {
                            Some(b'b') => 8,
                            Some(b'n') => b'\n',
                            Some(b'r') => b'\r',
                            Some(b't') => b'\t',
                            Some(x) => x,
                            None => return Ok(Value::Binary(s.into())),
                        }
                    } else {
                        b
                    })
                }
                Value::Binary(s.into())
            }
            ClickHouseType::Nullable(x) => {
                if bytes == b"\\N" {
                    Value::Null
                } else {
                    x.parse(bytes)?
                }
            }
            _ => return Err(anyhow!("unsupported type"))
        })
    }
}
