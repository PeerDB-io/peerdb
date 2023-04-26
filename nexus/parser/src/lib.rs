use std::sync::Arc;

use analyzer::{
    PeerDDL, PeerDDLAnalyzer, PeerExistanceAnalyzer, QueryAssocation, StatementAnalyzer,
};
use catalog::Catalog;
use pgwire::{
    api::{stmt::QueryParser, Type},
    error::{ErrorInfo, PgWireError, PgWireResult},
};
use sqlparser::{ast::Statement, dialect::PostgreSqlDialect, parser::Parser};
use tokio::sync::Mutex;

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

pub struct NexusQueryParser {
    catalog: Arc<Mutex<Catalog>>,
}

#[derive(Debug, Clone)]
pub enum NexusStatement {
    PeerDDL {
        stmt: Statement,
        ddl: PeerDDL,
    },
    PeerQuery {
        stmt: Statement,
        assoc: QueryAssocation,
    },
}

impl NexusStatement {
    pub fn new(catalog: Arc<Mutex<Catalog>>, stmt: &Statement) -> PgWireResult<Self> {
        let ddl = futures::executor::block_on(async move {
            let pdl: PeerDDLAnalyzer = Default::default();
            pdl.analyze(stmt).await.map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "internal_error".to_owned(),
                    e.to_string(),
                )))
            })
        })?;

        if let Some(ddl) = ddl {
            return Ok(NexusStatement::PeerDDL {
                stmt: stmt.clone(),
                ddl,
            });
        }

        let assoc = futures::executor::block_on(async move {
            let mut catalog = catalog.lock().await;
            let pea = PeerExistanceAnalyzer::new(&mut catalog);
            pea.analyze(stmt).await.map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "feature_not_supported".to_owned(),
                    e.to_string(),
                )))
            })
        })?;

        Ok(NexusStatement::PeerQuery {
            stmt: stmt.clone(),
            assoc,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NexusParsedStatement {
    pub statement: NexusStatement,
    pub query: String,
}

impl NexusQueryParser {
    pub fn new(catalog: Arc<Mutex<Catalog>>) -> Self {
        Self { catalog }
    }

    pub fn parse_simple_sql(&self, sql: &str) -> PgWireResult<NexusParsedStatement> {
        let mut stmts =
            Parser::parse_sql(&DIALECT, sql).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if stmts.len() > 1 {
            let err_msg = format!("unsupported sql: {}, statements: {:?}", sql, stmts);

            // TODO (kaushik): Better error message for this. When do we start seeing multiple statements?
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P14".to_owned(),
                err_msg,
            ))))
        } else {
            let stmt = stmts.remove(0);
            let nexus_stmt = NexusStatement::new(self.catalog.clone(), &stmt)?;
            Ok(NexusParsedStatement {
                statement: nexus_stmt,
                query: sql.to_owned(),
            })
        }
    }
}

impl QueryParser for NexusQueryParser {
    type Statement = NexusParsedStatement;

    fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let mut stmts =
            Parser::parse_sql(&DIALECT, sql).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if stmts.len() > 1 {
            let err_msg = format!("unsupported sql: {}, statements: {:?}", sql, stmts);
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P14".to_owned(),
                err_msg,
            ))))
        } else {
            let stmt = stmts.remove(0);
            let nexus_stmt = NexusStatement::new(self.catalog.clone(), &stmt)?;
            Ok(NexusParsedStatement {
                statement: nexus_stmt,
                query: sql.to_owned(),
            })
        }
    }
}
