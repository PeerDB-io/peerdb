use std::ops::ControlFlow;

use sqlparser::ast::Value::Number;

use sqlparser::ast::{
    visit_expressions_mut, visit_relations_mut, BinaryOperator, DataType, DateTimeField, Expr,
    Function, FunctionArg, FunctionArgExpr, Ident, ObjectName, Query,
};

#[derive(Default)]
pub struct BigqueryAst {}

impl BigqueryAst {
    pub fn is_timestamp_returning_function(&self, name: String) -> bool {
        if name == "now"
            || name == "date_trunc"
            || name == "make_timestamp"
            || name == "current_timestamp"
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
            if self.is_timestamp_returning_function(v[0].to_string().to_lowercase()) {
                return true;
            }
        }

        if let Expr::Interval { .. } = e {
            return true;
        }

        false
    }

    pub fn convert_to_datetimefield(&self, t: String) -> Option<DateTimeField> {
        let t_lower = t.to_lowercase();
        if t_lower == "day" || t_lower == "days" {
            return Some(DateTimeField::Day);
        }
        if t_lower == "hour" || t_lower == "hours" {
            return Some(DateTimeField::Hour);
        }
        if t_lower == "minute" || t_lower == "minutes" {
            return Some(DateTimeField::Minute);
        }
        if t_lower == "second" || t_lower == "Seconds" {
            return Some(DateTimeField::Second);
        }
        if t_lower == "millisecond" || t_lower == "milliseconds" {
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

        visit_expressions_mut(query, |node| {
            // CAST AS Text to CAST AS String
            if let Expr::Cast {
                expr: _,
                data_type: dt,
            } = node
            {
                if let DataType::Text = dt {
                    *dt = DataType::String;
                }
            }

            // now() to CURRENT_TIMESTAMP
            if let Expr::Function(Function {
                name: ObjectName(v),
                ..
            }) = node
            {
                if v[0].to_string().to_lowercase() == "now" {
                    v[0].value = "CURRENT_TIMESTAMP".into();
                }
            }

            // interval rewrite
            if let Expr::Interval {
                value,
                leading_field,
                ..
            } = node
            {
                if let Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) = value.as_mut() {
                    /*
                    postgres will have interval '1 Day'
                    rewriting that to interval 1 Day in BQ
                    */
                    let split = s.split(' ');
                    let vec = split.collect::<Vec<&str>>();
                    let val_string: String = vec[0].into();
                    let date_time_field_string: String = vec[1].into();
                    *(value.as_mut()) = Expr::Value(Number(val_string, false));
                    let date_time_field = self.convert_to_datetimefield(date_time_field_string);
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
                if self.is_timestamp_expr(left.as_mut()) || self.is_timestamp_expr(right.as_mut()) {
                    if let BinaryOperator::Minus = op {
                        *node = Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("DATE_SUB".to_string())]),
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*left.clone())),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*right.clone())),
                            ],
                            over: None,
                            distinct: false,
                            special: false,
                        })
                    } else if let BinaryOperator::Plus = op {
                        *node = Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("DATE_ADD".to_string())]),
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*left.clone())),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(*right.clone())),
                            ],
                            over: None,
                            distinct: false,
                            special: false,
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
                if v[0].to_string().to_lowercase() == "date_trunc" {
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

        // flatten ANY operand in BINARY to IN operation overall.
        visit_expressions_mut(query, |node| {
            if let Expr::BinaryOp { left, op, right } = node {
                // check if right is ANY
                if let Expr::AnyOp(expr) = right.as_mut() {
                    // check if left is a column
                    if let Expr::Identifier(_) = left.as_mut() {
                        let list = self
                            .flatten_expr_to_in_list(expr)
                            .expect("failed to flatten");
                        // check if op is =
                        if let BinaryOperator::Eq = op {
                            // rewrite to IN
                            *node = Expr::InList {
                                expr: left.clone(),
                                list,
                                negated: false,
                            };
                        }
                        // if op is != rewrite to NOT IN
                        else if let BinaryOperator::NotEq = op {
                            *node = Expr::InList {
                                expr: left.clone(),
                                list,
                                negated: true,
                            };
                        }
                    }
                }
            }

            ControlFlow::<()>::Continue(())
        });

        Ok(())
    }

    /// Flatten Cast EXPR to List with right value type
    /// For example Value(SingleQuotedString("{hash1,hash2}") must return
    /// a vector Value(SingleQuotedString("hash1"), Value(SingleQuotedString("hash2")))
    fn flatten_expr_to_in_list(&self, expr: &Expr) -> anyhow::Result<Vec<Expr>> {
        let mut list = vec![];
        // check if expr is of type Cast
        if let Expr::Cast { expr, data_type } = expr {
            // assert that expr is of type SingleQuotedString
            if let Expr::Value(sqlparser::ast::Value::SingleQuotedString(s)) = expr.as_ref() {
                // trim the starting and ending curly braces
                let s = s.trim_start_matches('{').trim_end_matches('}');
                // split string by comma
                let split = s.split(',');
                // match on data type, and create a vector of Expr::Value
                match data_type {
                    DataType::Array(Some(inner)) => match inner.as_ref() {
                        DataType::Text | DataType::Char(_) | DataType::Varchar(_) => {
                            for s in split {
                                list.push(Expr::Value(sqlparser::ast::Value::SingleQuotedString(
                                    s.to_string(),
                                )));
                            }
                        }
                        DataType::Integer(_) | DataType::Float(_) => {
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
            }
        }

        Ok(list)
    }
}
