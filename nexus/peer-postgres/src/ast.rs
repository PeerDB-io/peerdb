use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_relations_mut, ObjectName, ObjectType, Query, Statement, TableFactor, TableWithJoins,
};

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

    #[tracing::instrument(name = "peer_postgres::rewrite_statement", skip(self))]
    pub fn rewrite_statement(&self, stmt: &mut Statement) -> anyhow::Result<()> {
        /* could be written less clunkily when https://github.com/rust-lang/rust/issues/53667 is stabilized.
        Tracking in #225 */
        let table_name: &mut ObjectName = if let Statement::Insert {
            ref mut table_name, ..
        } = stmt
        {
            table_name
        } else if let Statement::Update { ref mut table, .. } = stmt {
            if let TableFactor::Table { ref mut name, .. } = table.relation {
                name
            } else {
                return Err(anyhow::anyhow!(
                    "attempted to UPDATE something that doesn't appear to be a simple table."
                ));
            }
        } else if let Statement::Delete { ref mut from, .. } = stmt {
            if from.len() > 1 {
                return Err(anyhow::anyhow!("Multi-table delete not supported."));
            } else {
                let ref mut table = from[0];
                if let TableFactor::Table { ref mut name, .. } = table.relation {
                    name
                } else {
                    return Err(anyhow::anyhow!(
                        "attempted to delete something which does not seem to be a table."
                    ));
                }
            }
        } else if let Statement::CreateTable { ref mut name, .. } = stmt {
            name
        } else if let Statement::Drop {
            ref object_type,
            ref mut names,
            ..
        } = stmt
        {
            if object_type == &ObjectType::Table {
                names.get_mut(0).unwrap()
            } else {
                return Err(anyhow::anyhow!(
                    "attempted to DROP something that doesn't appear to be a table."
                ));
            }
        } else if let Statement::Truncate {
            ref mut table_name, ..
        } = stmt
        {
            table_name
        } else {
            return Err(anyhow::anyhow!("only INSERT, UPDATE, DELETE, CREATE TABLE, TRUNCATE TABLE and DROP TABLE supported so far."));
        };

        table_name.0.remove(0);

        Ok(())
    }
}
