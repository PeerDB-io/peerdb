use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::Token;

use crate::ast_peerdb::{
    CreateMirror, CreateMirrorForCDC, CreateMirrorForSelect, MappingOptions, MappingType, PeerType,
};
use crate::PeerDBStatement;

fn word_matches(token: &Token, target: &str) -> bool {
    matches!(token, Token::Word(w) if w.value.eq_ignore_ascii_case(target))
}

/// Consume next token if it's a Word matching target (case-insensitive).
/// Returns true if consumed.
fn consume_word(parser: &mut Parser, target: &str) -> bool {
    if word_matches(&parser.peek_token().token, target) {
        parser.next_token();
        true
    } else {
        false
    }
}

fn expect_word(parser: &mut Parser, target: &str) -> Result<(), ParserError> {
    if consume_word(parser, target) {
        Ok(())
    } else {
        let tok = parser.peek_token();
        Err(ParserError::ParserError(format!(
            "expected {target}, got {tok}"
        )))
    }
}

/// Peek at upcoming tokens to detect PeerDB-specific statements.
/// Returns parsed PeerDBStatement or None to fall through to upstream.
pub fn try_parse_peerdb(parser: &mut Parser) -> Result<Option<PeerDBStatement>, ParserError> {
    let tok1 = parser.peek_token().token.clone();
    let tok2 = parser.peek_nth_token(1).token.clone();

    let (Token::Word(w1), Token::Word(w2)) = (&tok1, &tok2) else {
        return Ok(None);
    };

    let mirror = w2.value.eq_ignore_ascii_case("MIRROR");
    let peer = w2.value.eq_ignore_ascii_case("PEER");
    let w1 = &w1.value;

    let handler: fn(&mut Parser) -> Result<PeerDBStatement, ParserError> =
        if w1.eq_ignore_ascii_case("CREATE") && peer {
            parse_create_peer
        } else if w1.eq_ignore_ascii_case("CREATE") && mirror {
            parse_create_mirror
        } else if w1.eq_ignore_ascii_case("DROP") && peer {
            parse_drop_peer
        } else if w1.eq_ignore_ascii_case("DROP") && mirror {
            parse_drop_mirror
        } else if w1.eq_ignore_ascii_case("EXECUTE") && mirror {
            parse_execute_mirror
        } else if w1.eq_ignore_ascii_case("RESYNC") && mirror {
            parse_resync_mirror
        } else if w1.eq_ignore_ascii_case("PAUSE") && mirror {
            parse_pause_mirror
        } else if w1.eq_ignore_ascii_case("RESUME") && mirror {
            parse_resume_mirror
        } else {
            return Ok(None);
        };

    parser.next_token();
    parser.next_token();
    handler(parser).map(Some)
}

fn parse_peer_type(parser: &mut Parser) -> Result<PeerType, ParserError> {
    let tok = parser.next_token();
    match &tok.token {
        Token::Word(w) => match w.value.to_uppercase().as_str() {
            "BIGQUERY" => Ok(PeerType::Bigquery),
            "MONGO" => Ok(PeerType::Mongo),
            "SNOWFLAKE" => Ok(PeerType::Snowflake),
            "POSTGRES" => Ok(PeerType::Postgres),
            "KAFKA" => Ok(PeerType::Kafka),
            "S3" => Ok(PeerType::S3),
            "SQLSERVER" => Ok(PeerType::SQLServer),
            "MYSQL" => Ok(PeerType::MySql),
            "EVENTHUBS" => Ok(PeerType::Eventhubs),
            "PUBSUB" => Ok(PeerType::PubSub),
            "ELASTICSEARCH" => Ok(PeerType::Elasticsearch),
            "CLICKHOUSE" => Ok(PeerType::Clickhouse),
            other => Err(ParserError::ParserError(format!(
                "expected peer type, got {other}"
            ))),
        },
        _ => Err(ParserError::ParserError(format!(
            "expected peer type, got {tok}"
        ))),
    }
}

fn parse_with_options(parser: &mut Parser) -> Result<Vec<sqlparser::ast::SqlOption>, ParserError> {
    if parser.parse_keyword(Keyword::WITH) {
        parser.expect_token(&Token::LParen)?;
        let options = parser.parse_comma_separated(Parser::parse_sql_option)?;
        parser.expect_token(&Token::RParen)?;
        Ok(options)
    } else {
        Ok(vec![])
    }
}

fn parse_create_peer(parser: &mut Parser) -> Result<PeerDBStatement, ParserError> {
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let peer_name = parser.parse_object_name(false)?;
    parser.expect_keyword(Keyword::FROM)?;
    let peer_type = parse_peer_type(parser)?;
    let with_options = parse_with_options(parser)?;

    Ok(PeerDBStatement::CreatePeer {
        if_not_exists,
        peer_name,
        peer_type,
        with_options,
    })
}

fn parse_create_mirror(parser: &mut Parser) -> Result<PeerDBStatement, ParserError> {
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let mirror_name = parser.parse_object_name(false)?;
    parser.expect_keyword(Keyword::FROM)?;
    let source_peer = parser.parse_object_name(false)?;
    parser.expect_keyword(Keyword::TO)?;
    let target_peer = parser.parse_object_name(false)?;

    // SELECT-based mirror: CREATE MIRROR ... FOR $$query$$
    if parser.parse_keyword(Keyword::FOR) {
        let token = parser.next_token();
        match &token.token {
            Token::DollarQuotedString(s) => {
                let query_string = s.value.clone();
                let with_options = parse_with_options(parser)?;
                Ok(PeerDBStatement::CreateMirror {
                    if_not_exists,
                    create_mirror: CreateMirror::Select(CreateMirrorForSelect {
                        mirror_name,
                        source_peer,
                        target_peer,
                        query_string,
                        with_options,
                    }),
                })
            }
            _ => Err(ParserError::ParserError(format!(
                "expected $$query string$$, got {token}"
            ))),
        }
    } else {
        // CDC mirror: CREATE MIRROR ... WITH TABLE/SCHEMA MAPPING (...)
        parser.expect_keyword(Keyword::WITH)?;
        let mapping_type = if parser.parse_keyword(Keyword::TABLE) {
            MappingType::Table
        } else if parser.parse_keyword(Keyword::SCHEMA) {
            MappingType::Schema
        } else {
            return Err(ParserError::ParserError(
                "expected TABLE or SCHEMA".to_string(),
            ));
        };
        expect_word(parser, "MAPPING")?;

        parser.expect_token(&Token::LParen)?;
        let mapping_options = parser.parse_comma_separated(parse_mapping)?;
        parser.expect_token(&Token::RParen)?;

        let with_options = parse_with_options(parser)?;

        Ok(PeerDBStatement::CreateMirror {
            if_not_exists,
            create_mirror: CreateMirror::CDC(CreateMirrorForCDC {
                mirror_name,
                source_peer,
                target_peer,
                mapping_options,
                with_options,
                mapping_type,
            }),
        })
    }
}

fn parse_drop_peer(parser: &mut Parser) -> Result<PeerDBStatement, ParserError> {
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let peer_name = parser.parse_object_name(false)?;
    Ok(PeerDBStatement::DropPeer {
        if_exists,
        peer_name,
    })
}

fn parse_drop_mirror(parser: &mut Parser) -> Result<PeerDBStatement, ParserError> {
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let mirror_name = parser.parse_object_name(false)?;
    Ok(PeerDBStatement::DropMirror {
        if_exists,
        mirror_name,
    })
}

fn parse_execute_mirror(parser: &mut Parser) -> Result<PeerDBStatement, ParserError> {
    let mirror_name = parser.parse_identifier()?;
    Ok(PeerDBStatement::ExecuteMirror { mirror_name })
}

fn parse_resync_mirror(parser: &mut Parser) -> Result<PeerDBStatement, ParserError> {
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let mirror_name = parser.parse_object_name(false)?;
    let with_options = parse_with_options(parser)?;
    Ok(PeerDBStatement::ResyncMirror {
        if_exists,
        mirror_name,
        with_options,
    })
}

fn parse_pause_mirror(parser: &mut Parser) -> Result<PeerDBStatement, ParserError> {
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let mirror_name = parser.parse_object_name(false)?;
    Ok(PeerDBStatement::PauseMirror {
        if_exists,
        mirror_name,
    })
}

fn parse_resume_mirror(parser: &mut Parser) -> Result<PeerDBStatement, ParserError> {
    let if_exists = parser.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
    let mirror_name = parser.parse_object_name(false)?;
    Ok(PeerDBStatement::ResumeMirror {
        if_exists,
        mirror_name,
    })
}

fn parse_mapping(parser: &mut Parser) -> Result<MappingOptions, ParserError> {
    // v2 format: {from: src, to: dst, key: k, exclude: [a, b]}
    if parser.consume_token(&Token::LBrace) {
        let mut source = None;
        let mut destination = None;
        let mut partition_key = None;
        let mut exclude = None;

        loop {
            let kw = parser.expect_one_of_keywords(&[
                Keyword::FROM,
                Keyword::TO,
                Keyword::KEY,
                Keyword::EXCLUDE,
            ])?;
            parser.expect_token(&Token::Colon)?;
            match kw {
                Keyword::FROM => {
                    if source.is_some() {
                        return Err(ParserError::ParserError("Duplicate FROM".to_string()));
                    }
                    source = Some(parser.parse_object_name(false)?);
                }
                Keyword::TO => {
                    if destination.is_some() {
                        return Err(ParserError::ParserError("Duplicate TO".to_string()));
                    }
                    destination = Some(parser.parse_object_name(false)?);
                }
                Keyword::KEY => {
                    if partition_key.is_some() {
                        return Err(ParserError::ParserError("Duplicate KEY".to_string()));
                    }
                    partition_key = Some(parser.parse_identifier()?);
                }
                Keyword::EXCLUDE => {
                    if exclude.is_some() {
                        return Err(ParserError::ParserError("Duplicate EXCLUDE".to_string()));
                    }
                    let mut ex = Vec::new();
                    parser.expect_token(&Token::LBracket)?;
                    loop {
                        ex.push(parser.parse_identifier()?);
                        let next_token = parser.next_token();
                        if next_token.token == Token::RBracket {
                            break;
                        }
                        if next_token.token != Token::Comma {
                            return Err(ParserError::ParserError(format!(
                                "expected comma, got {next_token}"
                            )));
                        }
                        if parser.consume_token(&Token::RBracket) {
                            break;
                        }
                    }
                    exclude = Some(ex);
                }
                _ => unreachable!(),
            }
            if !parser.consume_token(&Token::Comma) {
                break;
            }
        }
        parser.expect_token(&Token::RBrace)?;

        match (source, destination) {
            (Some(source), Some(destination)) => Ok(MappingOptions {
                source,
                destination,
                partition_key,
                exclude,
            }),
            _ => Err(ParserError::ParserError("Expected FROM/TO".to_string())),
        }
    } else {
        // v1 format: source:destination
        let source = parser.parse_object_name(false)?;
        parser.expect_token(&Token::Colon)?;
        let destination = parser.parse_object_name(false)?;
        Ok(MappingOptions {
            source,
            destination,
            partition_key: None,
            exclude: None,
        })
    }
}
