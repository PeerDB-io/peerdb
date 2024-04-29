use std::ops::ControlFlow;

use peer_ast::flatten_expr_to_in_list;
use sqlparser::ast::{
    visit_expressions_mut, visit_function_arg_mut, visit_relations_mut, Array, BinaryOperator,
    DataType, Expr, FunctionArgExpr, Query,
};

pub fn rewrite_query(peername: &str, query: &mut Query) {
    visit_relations_mut(query, |table| {
        // if peer name is first part of table name, remove first part
        if peername.eq_ignore_ascii_case(&table.0[0].value) {
            table.0.remove(0);
        }
        ControlFlow::<()>::Continue(())
    });

    // postgres_fdw sends `limit 1` as `limit 1::bigint` which mysql chokes on
    if let Some(Expr::Cast { expr, .. }) = &query.limit {
        query.limit = Some((**expr).clone());
    }

    visit_function_arg_mut(query, |node| {
        if let FunctionArgExpr::Expr(arg_expr) = node {
            if let Expr::Cast {
                data_type: DataType::Array(_),
                ..
            } = arg_expr
            {
                let list =
                    flatten_expr_to_in_list(arg_expr).expect("failed to flatten in function");
                let rewritten_array = Array {
                    elem: list,
                    named: true,
                };
                *node = FunctionArgExpr::Expr(Expr::Array(rewritten_array));
            }
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
                let list = flatten_expr_to_in_list(right).expect("failed to flatten");
                *node = Expr::InList {
                    expr: left.clone(),
                    list,
                    negated: matches!(compare_op, BinaryOperator::NotEq),
                };
            }
        }

        ControlFlow::<()>::Continue(())
    });
}
