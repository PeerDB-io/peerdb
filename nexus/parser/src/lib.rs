use std::{collections::HashMap, sync::Arc};

use analyzer::{
    CursorEvent, PeerCursorAnalyzer, PeerDDL, PeerDDLAnalyzer, PeerExistanceAnalyzer,
    QueryAssocation, StatementAnalyzer,
};
use async_trait::async_trait;
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
    PeerCursor {
        stmt: Statement,
        cursor: CursorEvent,
    },
    Empty,
}

impl NexusStatement {
    pub fn new(peers: HashMap<String, pt::peers::Peer>, stmt: &Statement) -> PgWireResult<Self> {
        let ddl = {
            let pdl: PeerDDLAnalyzer = Default::default();
            pdl.analyze(stmt).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "internal_error".to_owned(),
                    e.to_string(),
                )))
            })
        }?;

        if let Some(ddl) = ddl {
            return Ok(NexusStatement::PeerDDL {
                stmt: stmt.clone(),
                ddl,
            });
        }

        let peer_cursor: PeerCursorAnalyzer = Default::default();
        if let Ok(Some(cursor)) = peer_cursor.analyze(stmt) {
            return Ok(NexusStatement::PeerCursor {
                stmt: stmt.clone(),
                cursor,
            });
        }

        let assoc = {
            let pea = PeerExistanceAnalyzer::new(&peers);
            pea.analyze(stmt).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "feature_not_supported".to_owned(),
                    e.to_string(),
                )))
            })
        }?;

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

    pub fn get_peers_bridge(&self) -> PgWireResult<HashMap<String, pt::peers::Peer>> {
        let peers = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                let catalog = self.catalog.lock().await;
                let peers = catalog.get_peers().await;
                peers
            })
        });

        peers.map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "internal_error".to_owned(),
                e.to_string(),
            )))
        })
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
        } else if stmts.is_empty() {
            Ok(NexusParsedStatement {
                statement: NexusStatement::Empty,
                query: sql.to_owned(),
            })
        } else {
            let stmt = stmts.remove(0);
            let peers = self.get_peers_bridge()?;
            let nexus_stmt = NexusStatement::new(peers, &stmt)?;
            Ok(NexusParsedStatement {
                statement: nexus_stmt,
                query: sql.to_owned(),
            })
        }
    }
}

#[async_trait]
impl QueryParser for NexusQueryParser {
    type Statement = NexusParsedStatement;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        let mut stmts =
            Parser::parse_sql(&DIALECT, sql).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if stmts.len() > 1 {
            let err_msg = format!("unsupported sql: {}, statements: {:?}", sql, stmts);
            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P14".to_owned(),
                err_msg,
            ))))
        } else if stmts.is_empty() {
            Ok(NexusParsedStatement {
                statement: NexusStatement::Empty,
                query: sql.to_owned(),
            })
        } else {
            let stmt = stmts.remove(0);
            let peers = self.get_peers_bridge()?;
            let nexus_stmt = NexusStatement::new(peers, &stmt)?;
            Ok(NexusParsedStatement {
                statement: nexus_stmt,
                query: sql.to_owned(),
            })
        }
    }
}
