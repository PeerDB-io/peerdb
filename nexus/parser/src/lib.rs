pub use sqlparser::*;

pub mod ast_helpers;
pub mod ast_peerdb;
mod parser_peerdb;

use std::fmt;

use sqlparser::ast::{Ident, ObjectName, SqlOption, Statement};
use sqlparser::dialect::Dialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use ast_peerdb::{CreateMirror, MappingType, PeerType};

fn comma_separated<T: fmt::Display>(slice: &[T]) -> String {
    slice
        .iter()
        .map(|t| t.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Debug, Clone, PartialEq)]
pub enum PeerDBStatement {
    CreatePeer {
        if_not_exists: bool,
        peer_name: ObjectName,
        peer_type: PeerType,
        with_options: Vec<SqlOption>,
    },
    DropPeer {
        if_exists: bool,
        peer_name: ObjectName,
    },
    CreateMirror {
        if_not_exists: bool,
        create_mirror: CreateMirror,
    },
    DropMirror {
        if_exists: bool,
        mirror_name: ObjectName,
    },
    ExecuteMirror {
        mirror_name: Ident,
    },
    ResyncMirror {
        if_exists: bool,
        mirror_name: ObjectName,
        with_options: Vec<SqlOption>,
    },
    PauseMirror {
        if_exists: bool,
        mirror_name: ObjectName,
    },
    ResumeMirror {
        if_exists: bool,
        mirror_name: ObjectName,
    },
    Statement(Box<Statement>),
}

impl fmt::Display for PeerDBStatement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PeerDBStatement::CreatePeer {
                if_not_exists,
                peer_name,
                peer_type,
                with_options,
            } => {
                write!(
                    f,
                    "CREATE PEER {if_not_exists}{peer_name} FROM {peer_type}",
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                )?;
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", comma_separated(with_options))?;
                }
                Ok(())
            }
            PeerDBStatement::DropPeer {
                if_exists,
                peer_name,
            } => write!(
                f,
                "DROP PEER {if_exists}{peer_name}",
                if_exists = if *if_exists { "IF EXISTS " } else { "" },
            ),
            PeerDBStatement::CreateMirror {
                if_not_exists,
                create_mirror,
            } => match create_mirror {
                CreateMirror::CDC(cdc) => {
                    write!(
                        f,
                        "CREATE MIRROR {not_exists}{mirror_name} FROM {source} TO {target} WITH {mapping_type} MAPPING ({mappings})",
                        not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                        mirror_name = cdc.mirror_name,
                        source = cdc.source_peer,
                        target = cdc.target_peer,
                        mappings = comma_separated(&cdc.mapping_options),
                        mapping_type = if cdc.mapping_type == MappingType::Table { "TABLE" } else { "SCHEMA" }
                    )?;
                    if !cdc.with_options.is_empty() {
                        write!(f, " WITH ({})", comma_separated(&cdc.with_options))?;
                    }
                    Ok(())
                }
                CreateMirror::Select(select) => {
                    write!(
                        f,
                        "CREATE MIRROR {not_exists}{mirror_name} FROM {source} TO {target} FOR $${query_string}$$",
                        not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                        mirror_name = select.mirror_name,
                        source = select.source_peer,
                        target = select.target_peer,
                        query_string = select.query_string
                    )?;
                    if !select.with_options.is_empty() {
                        write!(f, " WITH ({})", comma_separated(&select.with_options))?;
                    }
                    Ok(())
                }
            },
            PeerDBStatement::DropMirror {
                if_exists,
                mirror_name,
            } => write!(
                f,
                "DROP MIRROR {if_exists}{mirror_name}",
                if_exists = if *if_exists { "IF EXISTS " } else { "" },
            ),
            PeerDBStatement::ExecuteMirror { mirror_name } => {
                write!(f, "EXECUTE MIRROR {mirror_name}")
            }
            PeerDBStatement::ResyncMirror {
                if_exists,
                mirror_name,
                with_options,
            } => {
                write!(
                    f,
                    "RESYNC MIRROR {if_exists}{mirror_name}",
                    if_exists = if *if_exists { "IF EXISTS " } else { "" },
                )?;
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", comma_separated(with_options))?;
                }
                Ok(())
            }
            PeerDBStatement::PauseMirror {
                if_exists,
                mirror_name,
            } => write!(
                f,
                "PAUSE MIRROR {if_exists}{mirror_name}",
                if_exists = if *if_exists { "IF EXISTS " } else { "" },
            ),
            PeerDBStatement::ResumeMirror {
                if_exists,
                mirror_name,
            } => write!(
                f,
                "RESUME MIRROR {if_exists}{mirror_name}",
                if_exists = if *if_exists { "IF EXISTS " } else { "" },
            ),
            PeerDBStatement::Statement(stmt) => write!(f, "{stmt}"),
        }
    }
}

/// Parse SQL, intercepting PeerDB-specific statements before delegating to upstream.
pub fn parse_sql(dialect: &dyn Dialect, sql: &str) -> Result<Vec<PeerDBStatement>, ParserError> {
    let mut parser = Parser::new(dialect).try_with_sql(sql)?;
    let mut stmts = vec![];

    loop {
        while parser.consume_token(&Token::SemiColon) {}
        if parser.peek_token().token == Token::EOF {
            break;
        }

        if let Some(peerdb_stmt) = parser_peerdb::try_parse_peerdb(&mut parser)? {
            stmts.push(peerdb_stmt);
        } else {
            stmts.push(PeerDBStatement::Statement(Box::new(parser.parse_statement()?)));
        }

        // expect semicolon or EOF between statements
        if !parser.consume_token(&Token::SemiColon) {
            if parser.peek_token().token != Token::EOF {
                return Err(ParserError::ParserError(format!(
                    "expected semicolon or EOF, got {}",
                    parser.peek_token()
                )));
            }
            break;
        }
    }

    Ok(stmts)
}
