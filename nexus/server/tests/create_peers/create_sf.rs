use pt::peerdb_peers::SnowflakeConfig;

use postgres::Client;
use std::env;
use std::fs::File;
use std::io::Read;

pub fn create(nexus: &mut Client) {
    dotenvy::dotenv().ok();
    let config_file_path = env::var("TEST_SF_CREDS").expect("TEST_SF_CREDS not set");
    let mut file = File::open(config_file_path).expect("failed to open snowflake json");
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("failed to read snowflake json");

    let sf_config: SnowflakeConfig =
        serde_json::from_str(&contents).expect("failed to parse snowflake json");

    let create_stmt = format!(
        "
    CREATE PEER sf_test FROM SNOWFLAKE WITH
        (
        account_id = '{}',
        username = '{}',
        private_key = '{}',
        database = '{}',
        warehouse = '{}',
        role = '{}',
        query_timeout = '{}',
        s3_integration = '{}'
        );",
        &sf_config.account_id,
        &sf_config.username,
        &sf_config.private_key,
        "SNOWFLAKE_CI",
        &sf_config.warehouse,
        &sf_config.role,
        &sf_config.query_timeout,
        &sf_config.s3_integration
    );

    let creation_status = nexus.simple_query(&create_stmt);
    match creation_status {
        Ok(_) => (),
        Err(err) => {
            let create_err = err
                .as_db_error()
                .expect("failed to unwrap create peer error");
            let already_exists_case = create_err.message().contains("(sf_test) already exists");
            if already_exists_case {
                return ();
            }
            panic!("failed to create snowflake peer:{}", err)
        }
    }
}
