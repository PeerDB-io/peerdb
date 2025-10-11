use std::ops::ControlFlow;

use sqlparser::ast::{ObjectType, Query, Statement, visit_relations_mut, visit_statements_mut};

#[derive(Default)]
pub struct PostgresAst {
    pub peername: Option<String>,
}

impl PostgresAst {
    pub fn rewrite_query(&self, query: &mut Query) {
        let _ = visit_relations_mut(query, |table| {
            // if peer name is first part of table name, remove first part
            if let Some(ref peername) = self.peername
                && table.0.len() > 1
                && peername.eq_ignore_ascii_case(&table.0[0].value)
            {
                table.0.remove(0);
            }
            ControlFlow::<()>::Continue(())
        });
    }

    pub fn rewrite_statement(&self, stmt: &mut Statement) -> anyhow::Result<()> {
        // DROP statement needs to be handled separately
        let _ = visit_statements_mut(stmt, |stmt| {
            if let Statement::Drop {
                object_type, names, ..
            } = stmt
                && object_type == &ObjectType::Table
                && let Some(ref peername) = self.peername
                && let Some(table) = names.first_mut()
                && peername.eq_ignore_ascii_case(&table.0[0].value)
            {
                table.0.remove(0);
            }
            ControlFlow::<()>::Continue(())
        });

        let _ = visit_relations_mut(stmt, |table| {
            // if peer name is first part of table name, remove first part
            if let Some(ref peername) = self.peername
                && peername.eq_ignore_ascii_case(&table.0[0].value)
            {
                table.0.remove(0);
            }
            ControlFlow::<()>::Continue(())
        });
        Ok(())
    }
}
