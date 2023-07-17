use postgres::Client;
use pt::peerdb_peers::BigqueryConfig;
use std::env;
use std::fs::File;
use std::io::Read;
pub fn create(nexus: &mut Client) {
    dotenvy::dotenv().ok();
    let service_file_path = env::var("TEST_BQ_CREDS").expect("TEST_BQ_CREDS not set");
    let mut file = File::open(service_file_path).expect("failed to open bigquery json");
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .expect("failed to read bigquery json");

    let bq_config: BigqueryConfig =
        serde_json::from_str(&contents).expect("failed to parse bq.json");

    let create_stmt = format!(
        "
    CREATE PEER bq_test FROM BIGQUERY WITH
    (
        type = '{}',
        project_id = '{}',
        private_key_id = '{}',
        private_key = '{}',
        client_email = '{}',
        client_id = '{}',
        auth_uri = '{}',
        token_uri = '{}',
        auth_provider_x509_cert_url = '{}',
        client_x509_cert_url = '{}',
        dataset_id = '{}'
    );
    ",
        &bq_config.auth_type,
        &bq_config.project_id,
        &bq_config.private_key_id,
        &bq_config.private_key,
        &bq_config.client_email,
        &bq_config.client_id,
        &bq_config.auth_uri,
        &bq_config.token_uri,
        &bq_config.auth_provider_x509_cert_url,
        &bq_config.client_x509_cert_url,
        &bq_config.dataset_id
    );

    let creation_status = nexus.simple_query(&create_stmt);
    match creation_status {
        Ok(_) => (),
        Err(err) => {
            let create_err = err
                .as_db_error()
                .expect("failed to unwrap create peer error");
            let already_exists_case = create_err.message().contains("(bq_test) already exists");
            if already_exists_case {
                return ();
            }
            panic!("failed to create bigquery peer: {}", err)
        }
    }
}
