use sqlparser::ast::{Array, ArrayElemTypeDef, DataType, Expr};

/// Flatten Cast EXPR to list of values with right type.
/// e.g. Value(SingleQuotedString("{hash1,hash2}")) becomes
/// vec![Value(SingleQuotedString("hash1")), Value(SingleQuotedString("hash2"))]
pub fn flatten_expr_to_in_list(expr: &Expr) -> anyhow::Result<Vec<Expr>> {
    let mut list = vec![];
    if let Expr::Cast {
        expr, data_type, ..
    } = expr
    {
        if let Expr::Value(sqlparser::ast::ValueWithSpan {
            value: sqlparser::ast::Value::SingleQuotedString(s),
            ..
        }) = expr.as_ref()
        {
            let s = s.trim_start_matches('{').trim_end_matches('}');
            let split = s.split(',');
            match data_type {
                DataType::Array(ArrayElemTypeDef::AngleBracket(inner))
                | DataType::Array(ArrayElemTypeDef::SquareBracket(inner, _)) => {
                    match inner.as_ref() {
                        DataType::Text | DataType::Char(_) | DataType::Varchar(_) => {
                            for s in split {
                                list.push(Expr::Value(
                                    sqlparser::ast::Value::SingleQuotedString(s.to_string()).into(),
                                ));
                            }
                        }
                        DataType::Integer(_)
                        | DataType::Float(_)
                        | DataType::BigInt(_)
                        | DataType::BigIntUnsigned(_)
                        | DataType::IntegerUnsigned(_)
                        | DataType::SmallIntUnsigned(_)
                        | DataType::TinyIntUnsigned(_)
                        | DataType::TinyInt(_)
                        | DataType::IntUnsigned(_) => {
                            for s in split {
                                list.push(Expr::Value(
                                    sqlparser::ast::Value::Number(s.to_string(), false).into(),
                                ));
                            }
                        }
                        _ => {
                            return Err(anyhow::anyhow!(
                                "Unsupported inner data type for IN list: {:?}",
                                data_type
                            ));
                        }
                    }
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported data type for IN list: {:?}",
                        data_type
                    ));
                }
            }
        } else if let Expr::Array(arr) = expr.as_ref() {
            list = pour_array_into_list(arr, list)?;
        }
    } else if let Expr::Array(arr) = expr {
        list = pour_array_into_list(arr, list)?;
    }

    Ok(list)
}

fn pour_array_into_list(arr: &Array, mut list: Vec<Expr>) -> anyhow::Result<Vec<Expr>> {
    for element in &arr.elem {
        match &element {
            Expr::Value(val) => match &val.value {
                sqlparser::ast::Value::Number(_, _) => {
                    list.push(Expr::Value(
                        sqlparser::ast::Value::Number(element.to_string(), false).into(),
                    ));
                }
                sqlparser::ast::Value::SingleQuotedString(_) => {
                    list.push(Expr::Value(
                        sqlparser::ast::Value::SingleQuotedString(element.to_string()).into(),
                    ));
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported data type for IN list: {:?}",
                        val
                    ));
                }
            },
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported element for IN list: {:?}",
                    element
                ));
            }
        }
    }
    Ok(list)
}
