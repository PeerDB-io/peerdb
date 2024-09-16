use std::ops::ControlFlow;

use sqlparser::ast::{visit_relations_mut, visit_statements_mut, ObjectType, Query, Statement};

#[derive(Default)]
pub struct PostgresAst {
    pub peername: Option<String>,
}

impl PostgresAst {
    pub fn rewrite_query(&self, query: &mut Query) {
        visit_relations_mut(query, |table| {
            // if peer name is first part of table name, remove first part
            if let Some(ref peername) = self.peername {
                if table.0.len() > 1 && peername.eq_ignore_ascii_case(&table.0[0].value) {
                    table.0.remove(0);
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }

    pub fn rewrite_statement(&self, stmt: &mut Statement) -> anyhow::Result<()> {
        // DROP statement needs to be handled separately
        visit_statements_mut(stmt, |stmnt| {
            if let Statement::Drop {
                ref object_type,
                ref mut names,
                ..
            } = stmnt
            {
                if object_type == &ObjectType::Table {
                    if let Some(ref peername) = self.peername {
                        if let Some(table) = names.first_mut() {
                            if peername.eq_ignore_ascii_case(&table.0[0].value) {
                                table.0.remove(0);
                            }
                        }
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });

        visit_relations_mut(stmt, |table| {
            // if peer name is first part of table name, remove first part
            if let Some(ref peername) = self.peername {
                if peername.eq_ignore_ascii_case(&table.0[0].value) {
                    table.0.remove(0);
                }
            }
            ControlFlow::<()>::Continue(())
        });
        Ok(())
    }
}
