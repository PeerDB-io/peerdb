use postgres::NoTls;
use pt::peers::PostgresConfig;

use postgres::Client;
use std::fs::{read_to_string, File};
use std::io::Read;
fn hydrate(pg_peer: &mut Client) {
    let table_dump_path = "tests/assets/seed.sql";
    let dump_contents =
        read_to_string(table_dump_path).expect("[seed-postgres]:failed to read seed file");
    pg_peer
        .batch_execute(&dump_contents)
        .expect("[seed-postgres]:failed to seed postgres");
}

pub fn create(nexus: &mut Client) {
    let mut file = File::open("tests/assets/pg.json").expect("failed to open pg.json");
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("failed to read pg.json");

    let pg_config: PostgresConfig =
        serde_json::from_str(&contents).expect("failed to parse pg.json");

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
        &pg_config.host, &pg_config.port, &pg_config.user, &pg_config.password, &pg_config.database
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

    let peer_conn_str = format!(
        "postgresql://{}:{}@{}:{}/{}",
        pg_config.user, pg_config.password, pg_config.host, pg_config.port, pg_config.database,
    );
    let mut pg_client =
        Client::connect(&peer_conn_str, NoTls).expect("failed to connect to pg peer");
    hydrate(&mut pg_client);
}
