use postgres::NoTls;

use postgres::Client;
use std::env;
use std::fs::read_to_string;

fn hydrate(pg_peer: &mut Client) {
    let table_dump_path = "tests/assets/seed.sql";
    let dump_contents =
        read_to_string(table_dump_path).expect("[seed-postgres]:failed to read seed file");
    pg_peer
        .batch_execute(&dump_contents)
        .expect("[seed-postgres]:failed to seed postgres");
}

pub fn create(nexus: &mut Client) {
    dotenvy::dotenv().ok();
    let peer_host = env::var("PEERDB_CATALOG_HOST").expect("PEERDB_CATALOG_HOST not set");
    let peer_port = env::var("PEERDB_CATALOG_PORT").expect("PEERDB_CATALOG_PORT not set");
    let peer_database =
        env::var("PEERDB_CATALOG_DATABASE").expect("PEERDB_CATALOG_DATABASE not set");
    let peer_user = env::var("PEERDB_CATALOG_USER").expect("PEERDB_CATALOG_USER not set");
    let peer_password =
        env::var("PEERDB_CATALOG_PASSWORD").expect("PEERDB_CATALOG_PASSWORD not set");

    // Hydrate peer first
    let peer_conn_str = format!(
        "postgresql://{}:{}@{}:{}",
        peer_user, peer_password, peer_host, peer_port
    );
    let mut pg_client =
        Client::connect(&peer_conn_str, NoTls).expect("failed to connect to pg peer");
    hydrate(&mut pg_client);

    let create_stmt = format!(
        "
    CREATE PEER pg_test FROM POSTGRES WITH
    (
        host = '{}',
        port = '{}',
        user = '{}',
        password = '{}',
        database = '{}'
    );",
        &peer_host, &peer_port, &peer_user, &peer_password, &peer_database
    );

    let creation_status = nexus.simple_query(&create_stmt);
    match creation_status {
        Ok(_) => (),
        Err(err) => {
            let create_err = err
                .as_db_error()
                .expect("failed to unwrap create peer error");
            let already_exists_case = create_err.message().contains("(pg_test) already exists");
            if !already_exists_case {
                panic!("failed to create pg peer: {}", err)
            }
        }
    }
}
