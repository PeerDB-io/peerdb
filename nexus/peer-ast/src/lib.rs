use sqlparser::ast::{Array, ArrayElemTypeDef, DataType, Expr};

/// Flatten Cast EXPR to List with right value type
/// For example Value(SingleQuotedString("{hash1,hash2}") must return
/// a vector Value(SingleQuotedString("hash1"), Value(SingleQuotedString("hash2")))
pub fn flatten_expr_to_in_list(expr: &Expr) -> anyhow::Result<Vec<Expr>> {
    let mut list = vec![];
    // check if expr is of type Cast
    if let Expr::Cast {
        expr, data_type, ..
    } = expr
    {
        // assert that expr is of type SingleQuotedString
        if let Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) = expr.as_ref() {
            // trim the starting and ending curly braces
            let s = s.trim_start_matches('{').trim_end_matches('}');
            // split string by comma
            let split = s.split(',');
            // match on data type, and create a vector of Expr::Value
            match data_type {
                DataType::Array(ArrayElemTypeDef::AngleBracket(inner))
                | DataType::Array(ArrayElemTypeDef::SquareBracket(inner)) => match inner.as_ref() {
                    DataType::Text | DataType::Char(_) | DataType::Varchar(_) => {
                        for s in split {
                            list.push(Expr::Value(sqlparser::ast::Value::SingleQuotedString(
                                s.to_string(),
                            )));
                        }
                    }
                    DataType::Integer(_)
                    | DataType::Float(_)
                    | DataType::BigInt(_)
                    | DataType::UnsignedBigInt(_)
                    | DataType::UnsignedInteger(_)
                    | DataType::UnsignedSmallInt(_)
                    | DataType::UnsignedTinyInt(_)
                    | DataType::TinyInt(_)
                    | DataType::UnsignedInt(_) => {
                        for s in split {
                            list.push(Expr::Value(sqlparser::ast::Value::Number(
                                s.to_string(),
                                false,
                            )));
                        }
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Unsupported inner data type for IN list: {:?}",
                            data_type
                        ))
                    }
                },
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported data type for IN list: {:?}",
                        data_type
                    ))
                }
            }
        } else if let Expr::Array(arr) = expr.as_ref() {
            list = pour_array_into_list(arr, list).expect("Failed to transfer array to list");
        }
    } else if let Expr::Array(arr) = expr {
        list = pour_array_into_list(arr, list).expect("Failed to transfer array to list");
    }

    Ok(list)
}

fn pour_array_into_list(arr: &Array, mut list: Vec<Expr>) -> anyhow::Result<Vec<Expr>> {
    for element in &arr.elem {
        match &element {
            Expr::Value(val) => match val {
                sqlparser::ast::Value::Number(_, _) => {
                    list.push(Expr::Value(sqlparser::ast::Value::Number(
                        element.to_string(),
                        false,
                    )));
                }
                sqlparser::ast::Value::SingleQuotedString(_) => {
                    list.push(Expr::Value(sqlparser::ast::Value::UnQuotedString(
                        element.to_string(),
                    )));
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported data type for IN list: {:?}",
                        val
                    ))
                }
            },
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported element for IN list: {:?}",
                    element
                ))
            }
        }
    }
    Ok(list)
}
