use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_expressions_mut, visit_relations_mut, visit_statements_mut, DataType, Expr, Function,
    FunctionArg, FunctionArgExpr, Ident, JsonOperator, ObjectName, Query, Statement, TimezoneInfo,
};

pub struct SnowflakeAst;

impl SnowflakeAst {
    pub fn rewrite(&self, query: &mut Query) -> anyhow::Result<()> {
        visit_relations_mut(query, |table| {
            if table.0.len() > 1 {
                table.0.remove(0);
            }
            ControlFlow::<()>::Continue(())
        });

        visit_expressions_mut(query, |expr: &mut Expr| {
            self.rewrite_json_access(expr);
            self.rewrite_timestamp_for_cast(expr);
            self.rewrite_timestamp_for_at_time_zone(expr);
            ControlFlow::<()>::Continue(())
        });

        visit_statements_mut(query, |stmt: &mut Statement| {
            self.rewrite_timestamp_for_create(stmt);
            ControlFlow::<()>::Continue(())
        });

        Ok(())
    }

    fn rewrite_json_access(&self, expr: &mut Expr) {
        if let sqlparser::ast::Expr::JsonAccess { operator, .. } = expr {
            *operator = JsonOperator::Colon;
        };
    }

    // TODO: Snowflake does not support the Postgres INTERVAL type. In future, encode this better [via a BackendMessage]
    fn rewrite_timestamp_for_cast(&self, expr: &mut Expr) {
        if let sqlparser::ast::Expr::Cast { data_type, .. } = expr {
            if *data_type == DataType::Timestamp(None, TimezoneInfo::None) {
                *data_type = DataType::SnowflakeTimestamp
            }
        }

        if let sqlparser::ast::Expr::TryCast { data_type, .. } = expr {
            if *data_type == DataType::Timestamp(None, TimezoneInfo::None) {
                *data_type = DataType::SnowflakeTimestamp
            }
        }
    }

    fn rewrite_timestamp_for_create(&self, stmt: &mut Statement) {
        if let sqlparser::ast::Statement::CreateTable { columns, .. } = stmt {
            for column in columns.iter_mut() {
                if column.data_type == DataType::Timestamp(None, TimezoneInfo::None) {
                    column.data_type = DataType::SnowflakeTimestamp
                }
            }
        }
    }

    fn rewrite_timestamp_for_at_time_zone(&self, expr: &mut Expr) {
        if let sqlparser::ast::Expr::AtTimeZone {
            timestamp,
            time_zone,
        } = expr
        {
            *expr = Expr::Function(Function {
                name: ObjectName(vec![Ident::new("CONVERT_TIMEZONE".to_string())]),
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                        sqlparser::ast::Value::SingleQuotedString(time_zone.to_string()),
                    ))),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(*timestamp.clone())),
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
