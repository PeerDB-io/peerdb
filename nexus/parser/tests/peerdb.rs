use parser::ast_peerdb::*;
use parser::dialect::PostgreSqlDialect;
use parser::{parse_sql, PeerDBStatement};

fn parse(sql: &str) -> Vec<PeerDBStatement> {
    parse_sql(&PostgreSqlDialect {}, sql).unwrap()
}

fn parse_one(sql: &str) -> PeerDBStatement {
    let mut stmts = parse(sql);
    assert_eq!(stmts.len(), 1);
    stmts.remove(0)
}

fn roundtrip(sql: &str) {
    let stmt = parse_one(sql);
    let rendered = stmt.to_string();
    let reparsed = parse_one(&rendered);
    assert_eq!(
        stmt, reparsed,
        "roundtrip failed:\n  original: {sql}\n  rendered: {rendered}"
    );
}

// -- CREATE PEER --

#[test]
fn create_peer_postgres() {
    let sql = "CREATE PEER my_peer FROM POSTGRES WITH (host = 'localhost', port = '5432')";
    let stmt = parse_one(sql);
    match &stmt {
        PeerDBStatement::CreatePeer {
            if_not_exists,
            peer_name,
            peer_type,
            with_options,
        } => {
            assert!(!if_not_exists);
            assert_eq!(peer_name.to_string(), "my_peer");
            assert_eq!(*peer_type, PeerType::Postgres);
            assert_eq!(with_options.len(), 2);
        }
        other => panic!("expected CreatePeer, got {other:?}"),
    }
    roundtrip(sql);
}

#[test]
fn create_peer_if_not_exists() {
    let sql = "CREATE PEER IF NOT EXISTS bq FROM BIGQUERY WITH (project_id = 'test')";
    let stmt = parse_one(sql);
    match &stmt {
        PeerDBStatement::CreatePeer { if_not_exists, .. } => assert!(if_not_exists),
        other => panic!("expected CreatePeer, got {other:?}"),
    }
    roundtrip(sql);
}

#[test]
fn create_peer_all_types() {
    let types = [
        "BIGQUERY",
        "MONGO",
        "SNOWFLAKE",
        "POSTGRES",
        "S3",
        "SQLSERVER",
        "MYSQL",
        "KAFKA",
        "EVENTHUBS",
        "PUBSUB",
        "ELASTICSEARCH",
        "CLICKHOUSE",
    ];
    for t in types {
        let sql = format!("CREATE PEER p FROM {t}");
        let stmt = parse_one(&sql);
        match stmt {
            PeerDBStatement::CreatePeer { peer_type, .. } => {
                assert_eq!(peer_type.to_string(), t);
            }
            other => panic!("expected CreatePeer for {t}, got {other:?}"),
        }
    }
}

// -- DROP PEER --

#[test]
fn drop_peer() {
    let sql = "DROP PEER my_peer";
    let stmt = parse_one(sql);
    match &stmt {
        PeerDBStatement::DropPeer {
            if_exists,
            peer_name,
        } => {
            assert!(!if_exists);
            assert_eq!(peer_name.to_string(), "my_peer");
        }
        other => panic!("expected DropPeer, got {other:?}"),
    }
    roundtrip(sql);
}

#[test]
fn drop_peer_if_exists() {
    let sql = "DROP PEER IF EXISTS my_peer";
    let stmt = parse_one(sql);
    match &stmt {
        PeerDBStatement::DropPeer { if_exists, .. } => assert!(if_exists),
        other => panic!("expected DropPeer, got {other:?}"),
    }
    roundtrip(sql);
}

// -- CREATE MIRROR (CDC) --

#[test]
fn create_mirror_cdc_table_mapping() {
    let sql = "CREATE MIRROR my_mirror FROM src_peer TO dst_peer WITH TABLE MAPPING (src_schema.src_table:dst_schema.dst_table)";
    let stmt = parse_one(sql);
    match &stmt {
        PeerDBStatement::CreateMirror {
            if_not_exists,
            create_mirror: CreateMirror::CDC(cdc),
        } => {
            assert!(!if_not_exists);
            assert_eq!(cdc.mirror_name.to_string(), "my_mirror");
            assert_eq!(cdc.source_peer.to_string(), "src_peer");
            assert_eq!(cdc.target_peer.to_string(), "dst_peer");
            assert_eq!(cdc.mapping_type, MappingType::Table);
            assert_eq!(cdc.mapping_options.len(), 1);
            assert_eq!(
                cdc.mapping_options[0].source.to_string(),
                "src_schema.src_table"
            );
            assert_eq!(
                cdc.mapping_options[0].destination.to_string(),
                "dst_schema.dst_table"
            );
        }
        other => panic!("expected CreateMirror CDC, got {other:?}"),
    }
}

#[test]
fn create_mirror_cdc_schema_mapping() {
    let sql = "CREATE MIRROR m FROM s TO d WITH SCHEMA MAPPING (sch1:sch2)";
    let stmt = parse_one(sql);
    match &stmt {
        PeerDBStatement::CreateMirror {
            create_mirror: CreateMirror::CDC(cdc),
            ..
        } => {
            assert_eq!(cdc.mapping_type, MappingType::Schema);
        }
        other => panic!("expected CreateMirror CDC schema, got {other:?}"),
    }
}

#[test]
fn create_mirror_cdc_v2_mapping() {
    let sql = "CREATE MIRROR m FROM s TO d WITH TABLE MAPPING ({from: src.t1, to: dst.t1, key: id, exclude: [col1, col2]})";
    let stmt = parse_one(sql);
    match &stmt {
        PeerDBStatement::CreateMirror {
            create_mirror: CreateMirror::CDC(cdc),
            ..
        } => {
            assert_eq!(cdc.mapping_options.len(), 1);
            let m = &cdc.mapping_options[0];
            assert_eq!(m.source.to_string(), "src.t1");
            assert_eq!(m.destination.to_string(), "dst.t1");
            assert_eq!(m.partition_key.as_ref().unwrap().to_string(), "id");
            assert_eq!(m.exclude.as_ref().unwrap().len(), 2);
        }
        other => panic!("expected CreateMirror CDC v2, got {other:?}"),
    }
}

#[test]
fn create_mirror_cdc_with_options() {
    let sql = "CREATE MIRROR m FROM s TO d WITH TABLE MAPPING (a:b) WITH (batch_size = '1000')";
    let stmt = parse_one(sql);
    match &stmt {
        PeerDBStatement::CreateMirror {
            create_mirror: CreateMirror::CDC(cdc),
            ..
        } => {
            assert_eq!(cdc.with_options.len(), 1);
        }
        other => panic!("expected CreateMirror CDC with options, got {other:?}"),
    }
}

// -- CREATE MIRROR (Select) --

#[test]
fn create_mirror_select() {
    let sql = "CREATE MIRROR m FROM s TO d FOR $$SELECT * FROM t$$";
    let stmt = parse_one(sql);
    match &stmt {
        PeerDBStatement::CreateMirror {
            create_mirror: CreateMirror::Select(sel),
            ..
        } => {
            assert_eq!(sel.query_string, "SELECT * FROM t");
        }
        other => panic!("expected CreateMirror Select, got {other:?}"),
    }
}

#[test]
fn create_mirror_select_with_options() {
    let sql =
        "CREATE MIRROR m FROM s TO d FOR $$SELECT 1$$ WITH (mode = 'append', batch_size = '100')";
    let stmt = parse_one(sql);
    match &stmt {
        PeerDBStatement::CreateMirror {
            create_mirror: CreateMirror::Select(sel),
            ..
        } => {
            assert_eq!(sel.with_options.len(), 2);
        }
        other => panic!("expected CreateMirror Select with options, got {other:?}"),
    }
}

// -- DROP MIRROR --

#[test]
fn drop_mirror() {
    roundtrip("DROP MIRROR my_mirror");
}

#[test]
fn drop_mirror_if_exists() {
    roundtrip("DROP MIRROR IF EXISTS my_mirror");
}

// -- EXECUTE MIRROR --

#[test]
fn execute_mirror() {
    let sql = "EXECUTE MIRROR my_mirror";
    let stmt = parse_one(sql);
    match &stmt {
        PeerDBStatement::ExecuteMirror { mirror_name } => {
            assert_eq!(mirror_name.to_string(), "my_mirror");
        }
        other => panic!("expected ExecuteMirror, got {other:?}"),
    }
    roundtrip(sql);
}

// -- RESYNC MIRROR --

#[test]
fn resync_mirror() {
    roundtrip("RESYNC MIRROR my_mirror");
}

#[test]
fn resync_mirror_if_exists_with_options() {
    let sql = "RESYNC MIRROR IF EXISTS my_mirror WITH (opt = 'val')";
    roundtrip(sql);
}

// -- PAUSE/RESUME MIRROR --

#[test]
fn pause_mirror() {
    roundtrip("PAUSE MIRROR my_mirror");
}

#[test]
fn pause_mirror_if_exists() {
    roundtrip("PAUSE MIRROR IF EXISTS my_mirror");
}

#[test]
fn resume_mirror() {
    roundtrip("RESUME MIRROR my_mirror");
}

#[test]
fn resume_mirror_if_exists() {
    roundtrip("RESUME MIRROR IF EXISTS my_mirror");
}

// -- Standard SQL delegation --

#[test]
fn standard_select() {
    let sql = "SELECT 1";
    let stmt = parse_one(sql);
    assert!(matches!(stmt, PeerDBStatement::Statement(_)));
}

#[test]
fn standard_create_table() {
    let sql = "CREATE TABLE t (id INT)";
    let stmt = parse_one(sql);
    assert!(matches!(stmt, PeerDBStatement::Statement(_)));
}

#[test]
fn multiple_statements() {
    let sql = "SELECT 1; DROP PEER p; SELECT 2";
    let stmts = parse(sql);
    assert_eq!(stmts.len(), 3);
    assert!(matches!(stmts[0], PeerDBStatement::Statement(_)));
    assert!(matches!(stmts[1], PeerDBStatement::DropPeer { .. }));
    assert!(matches!(stmts[2], PeerDBStatement::Statement(_)));
}
