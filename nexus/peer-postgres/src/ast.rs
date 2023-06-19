use std::ops::ControlFlow;

use sqlparser::ast::{visit_relations_mut, visit_statements_mut, ObjectType, Query, Statement};

#[derive(Default)]
pub struct PostgresAst {
    pub peername: Option<String>,
}

impl PostgresAst {
    pub fn rewrite_query(&self, query: &mut Query) {
        visit_relations_mut(query, |table| {
            // if the peer name is the first part of the table name,
            // remove it.
            if Some(table.0[0].value.clone().to_lowercase()) == self.peername {
                table.0.remove(0);
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
                    let table = names.get_mut(0).unwrap();
                    if Some(table.0[0].value.clone().to_lowercase()) == self.peername {
                        table.0.remove(0);
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });

        visit_relations_mut(stmt, |table| {
            // if the peer name is the first part of the table name,
            // remove it.
            if Some(table.0[0].value.clone().to_lowercase()) == self.peername {
                table.0.remove(0);
            }
            ControlFlow::<()>::Continue(())
        });
        Ok(())
    }
}
