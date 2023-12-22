use std::ops::ControlFlow;

use sqlparser::ast::Value::Number;

use sqlparser::ast::{
    visit_expressions_mut, visit_function_arg_mut, visit_relations_mut, visit_setexpr_mut, Array,
    ArrayElemTypeDef, BinaryOperator, DataType, DateTimeField, Expr, Function, FunctionArg,
    FunctionArgExpr, Ident, ObjectName, Query, SetExpr, SetOperator, SetQuantifier, TimezoneInfo,
};

#[derive(Default)]
pub struct BigqueryAst {}

impl BigqueryAst {
    pub fn is_timestamp_returning_function(&self, name: &str) -> bool {
        if name.eq_ignore_ascii_case("now")
            || name.eq_ignore_ascii_case("date_trunc")
            || name.eq_ignore_ascii_case("make_timestamp")
            || name.eq_ignore_ascii_case("current_timestamp")
        {
            return true;
        }
        false
    }

    pub fn is_timestamp_expr(&self, e: &Expr) -> bool {
        if let Expr::Cast {
            data_type: DataType::Timestamp(_, _),
            ..
        } = e
        {
            return true;
        }

        if let Expr::Function(Function {
            name: ObjectName(v),
            ..
        }) = e
        {
            if self.is_timestamp_returning_function(&v[0].value) {
                return true;
            }
        }

        if let Expr::Interval { .. } = e {
            return true;
        }

        false
    }

    pub fn convert_to_datetimefield(&self, t: &str) -> Option<DateTimeField> {
        if t.eq_ignore_ascii_case("day") || t.eq_ignore_ascii_case("days") {
            return Some(DateTimeField::Day);
        }
        if t.eq_ignore_ascii_case("hour") || t.eq_ignore_ascii_case("hours") {
            return Some(DateTimeField::Hour);
        }
        if t.eq_ignore_ascii_case("minute") || t.eq_ignore_ascii_case("minutes") {
            return Some(DateTimeField::Minute);
        }
        if t.eq_ignore_ascii_case("second") || t.eq_ignore_ascii_case("seconds") {
            return Some(DateTimeField::Second);
        }
        if t.eq_ignore_ascii_case("millisecond") || t.eq_ignore_ascii_case("milliseconds") {
            return Some(DateTimeField::Milliseconds);
        }
        None
    }

    pub fn rewrite(&self, dataset: &str, query: &mut Query) -> anyhow::Result<()> {
        // replace peername with the connected dataset.
        visit_relations_mut(query, |table| {
            table.0[0] = dataset.into();
            ControlFlow::<()>::Continue(())
        });

        visit_function_arg_mut(query, |node| {
            if let FunctionArgExpr::Expr(arg_expr) = node {
                if let Expr::Cast {
                    data_type: DataType::Array(_),
                    ..
                } = arg_expr
                {
                    let list = self
                        .flatten_expr_to_in_list(arg_expr)
                        .expect("failed to flatten in function");
                    let rewritten_array = Array {
                        elem: list,
                        named: true,
                    };
                    *node = FunctionArgExpr::Expr(Expr::Array(rewritten_array));
                }
            }

            ControlFlow::<()>::Continue(())
        });

        visit_expressions_mut(query, |node| {
            // CAST AS Text to CAST AS String
            if let Expr::Cast { data_type: dt, .. } = node {
                if let DataType::Text = dt {
                    *dt = DataType::String(None);
                }

                if let DataType::Timestamp(_, tz) = dt {
                    *tz = TimezoneInfo::None;
                }
            }

            if let Expr::Function(Function {
                name: ObjectName(v),
                ..
            }) = node
            {
                // now() to CURRENT_TIMESTAMP
                if v[0].value.eq_ignore_ascii_case("now") {
                    v[0].value = "CURRENT_TIMESTAMP".into();
                }
            }

            // interval rewrite
            if let Expr::Interval(sqlparser::ast::Interval {
                value,
                leading_field,
                ..
            }) = node
            {
                if let Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) = value.as_ref() {
                    /*
                    postgres will have interval '1 Day'
                    rewriting that to interval 1 Day in BQ
                    */
                    let split = s.split(' ');
                    let vec = split.collect::<Vec<&str>>();
                    let val_string: String = vec[0].into();
                    let date_time_field = self.convert_to_datetimefield(vec[1]);
                    *(value.as_mut()) = Expr::Value(Number(val_string, false));
                    if date_time_field.is_none() {
                        // Error handling - Nexus for BQ only supports Day, Hour, Minute, Second, Millisecond
                    }
                    *leading_field = date_time_field;
                } else {
                    // Error handling - not a valid postgres interval
                }
            }

            ControlFlow::<()>::Continue(())
        });

        /*
        this rewrites non-leaf changes in the query tree.
         */
        visit_expressions_mut(query, |node| {
            /*
            rewriting + & - for timestamps
            change + to DATE_ADD
            change - to DATE_SUB
            */
            if let Expr::BinaryOp { left, op, right } = node {
                if self.is_timestamp_expr(left.as_ref()) || self.is_timestamp_expr(right.as_ref()) {
                    if let BinaryOperator::Minus = op {
                        *node = Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("DATE_SUB".to_string())]),
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*left.clone())),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*right.clone())),
                            ],
                            null_treatment: None,
                            filter: None,
                            over: None,
                            distinct: false,
                            special: false,
                            order_by: vec![],
                        })
                    } else if let BinaryOperator::Plus = op {
                        *node = Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("DATE_ADD".to_string())]),
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*left.clone())),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*right.clone())),
                            ],
                            null_treatment: None,
                            filter: None,
                            over: None,
                            distinct: false,
                            special: false,
                            order_by: vec![],
                        })
                    }
                }
            }
            if let Expr::Function(Function {
                name: ObjectName(v),
                args: a,
                ..
            }) = node
            {
                if v[0].value.eq_ignore_ascii_case("date_trunc") {
                    let mut date_part = a[0].to_string();
                    let date_expression = &a[1];
                    a[0] = date_expression.clone();
                    date_part.remove(0);
                    date_part.pop();
                    let tmp = Expr::Identifier(Ident {
                        value: date_part,
                        quote_style: None,
                    });
                    a[1] = FunctionArg::Unnamed(FunctionArgExpr::Expr(tmp));
                }
            }

            ControlFlow::<()>::Continue(())
        });

        // Replace UNION with UNION DISTINCT (only if there is no SetQuantifier after UNION)
        visit_setexpr_mut(query, |node| {
            if let SetExpr::SetOperation {
                op: SetOperator::Union,
                set_quantifier: SetQuantifier::None,
                left,
                right,
            } = node
            {
                *node = SetExpr::SetOperation {
                    op: SetOperator::Union,
                    set_quantifier: SetQuantifier::Distinct,
                    left: left.clone(),
                    right: right.clone(),
                };
            }

            ControlFlow::<()>::Continue(())
        });

        // flatten ANY to IN operation overall.
        visit_expressions_mut(query, |node| {
            if let Expr::AnyOp {
                left,
                compare_op,
                right,
            } = node
            {
                if matches!(compare_op, BinaryOperator::Eq | BinaryOperator::NotEq) {
                    let list = self
                        .flatten_expr_to_in_list(right)
                        .expect("failed to flatten");
                    *node = Expr::InList {
                        expr: left.clone(),
                        list,
                        negated: matches!(compare_op, BinaryOperator::NotEq),
                    };
                }
            }

            ControlFlow::<()>::Continue(())
        });

        Ok(())
    }

    fn pour_array_into_list(&self, arr: &Array, mut list: Vec<Expr>) -> anyhow::Result<Vec<Expr>> {
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
    /// Flatten Cast EXPR to List with right value type
    /// For example Value(SingleQuotedString("{hash1,hash2}") must return
    /// a vector Value(SingleQuotedString("hash1"), Value(SingleQuotedString("hash2")))
    fn flatten_expr_to_in_list(&self, expr: &Expr) -> anyhow::Result<Vec<Expr>> {
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
                    | DataType::Array(ArrayElemTypeDef::SquareBracket(inner)) => {
                        match inner.as_ref() {
                            DataType::Text | DataType::Char(_) | DataType::Varchar(_) => {
                                for s in split {
                                    list.push(Expr::Value(
                                        sqlparser::ast::Value::SingleQuotedString(s.to_string()),
                                    ));
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
                        }
                    }
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Unsupported data type for IN list: {:?}",
                            data_type
                        ))
                    }
                }
            } else if let Expr::Array(arr) = expr.as_ref() {
                list = self
                    .pour_array_into_list(arr, list)
                    .expect("Failed to transfer array to list");
            }
        } else if let Expr::Array(arr) = expr {
            list = self
                .pour_array_into_list(arr, list)
                .expect("Failed to transfer array to list");
        }

        Ok(list)
    }
}
