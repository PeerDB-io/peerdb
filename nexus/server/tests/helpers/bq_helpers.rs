use dotenvy::dotenv;
use gcp_bigquery_client::model::{dataset::Dataset, query_request::QueryRequest};
use gcp_bigquery_client::Client;
use peer_bigquery::bq_client_from_config;
use pt::peers::BigqueryConfig;
use rand::Rng;
use serde::Deserialize;
use std::env;
use std::fs::File;
use std::io::Read;
#[derive(Debug, Deserialize)]
pub struct BQConfig {
    pub auth_type: String,
    pub project_id: String,
    pub private_key_id: String,
    pub private_key: String,
    pub client_email: String,
    pub client_id: String,
    pub auth_uri: String,
    pub token_uri: String,
    pub auth_provider_x509_cert_url: String,
    pub client_x509_cert_url: String,
    pub dataset_id: String,
}

pub fn create_bq_peer_query() -> String {
    let bq_peer_query = format!(
    "CREATE PEER my_bq_b FROM BIGQUERY WITH
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
    );",
    "service_account",
    "custom-program-353117",
    "b7dac5e15ee004899d8010ec090507c5f9877323",
    "peerdb-test@custom-program-353117.iam.gserviceaccount.com",
    "114881672386215878659",
    "https://accounts.google.com/o/oauth2/auth",
    "https://oauth2.googleapis.com/token",
    "https://www.googleapis.com/oauth2/v1/certs",
    "https://www.googleapis.com/robot/v1/metadata/x509/peerdb-test%40custom-program-353117.iam.gserviceaccount.com",
    "test_dataset",
    "-----BEGIN PRIVATE KEY-----
    MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDKhNRI6pKuXYhv
    ChdruRptrVFooEG+dx4ZL+EdAvfQOC6BgjeD/rTrX03o9bIml95Qlr9PxFVwWmlQ
    ZfLxCYu6TsQA4pgymf/qnW57XcJbCbwISRHpS7KIIeSz/e8nc6BcNMO0Hx3wqrCg
    XOgKdx5kLsOn9iKsBeRXR4NeZ2ABFFlfjilSjkhJUSDZIphI2M45MK3OavJCYfL6
    HW0thwQEUgs2VfBbXNZ7gIG7JwGfGhn5f2Itk1Bk6Af96Mn5BthsDUeFPCy1Oj/D
    j1I7wI1sk0xkBMQ+Q89ADaF9UBhio8TxjbpnK7aIDr0EBjoLxvhJ4UBEen6pzqhB
    wpCkaJUJAgMBAAECggEAGYyga4KMdsr+B2QyiDiWCPgwqQ5uvOSyO2M69zoEoZQ0
    6cnPzys3D+q6HYXBr/TA61HZWOrgDIibeLUj7RzTL/H38FSl9txO2P57jbzKZEna
    GwF87P3LzDjabYq/vS5tMLTHa9XxJQejuNd/vrJAd0sfC/WVcjZcFFbFUAeiqP/6
    p5Nzo+WbDTLmDfZLwbo9/A1KM0wklhz2mrOckvZCNJ72JSO0Ju+gHJnMZhyJvHLu
    g0g5vc4ow6vyaqydo8cmJJMrP9r9gDeCy6kNqTBuKi85mRMlsM6OLZYjy+5R43fS
    UL48QDPWTEUYr2LwzGhojkwx+nNaYEIvPWy/o2vT8QKBgQDx/WuhWdZYRFM5Zef3
    bVER12DNKOQoS2G19fOK8JAm/8+60alc3MwWYQyvIZK9nzkFhI9e4B1H1cBki9VW
    T7WQ8qilXG0iEkVef4dr4tnMkUa+mnKpwyVxvr49bYy1ppVFssd+2pjMND0JxCZl
    0p1LvsQbeCsiAn/7o47SRonhBwKBgQDWPmaMsrRBLc1grG/0dMu9LZHCKTs+UJ7+
    O/Oo0MEzIPVo6aICQmF4A9o3atu7fDR2udZ3yJJwiY88sSVmhWBUXP2YSBLesanb
    WLf5mT04qdbm82jGguniRNxqEO+rAhp0k5hggUz8aAaOFEpVyqZ8Z8sFoSsr/fxU
    M1GuwZYlbwKBgQDdBy/LzJavQJkTkDT0FnE57pOIUJU1CMVSwjeU7G1+caF3bhFX
    tITk2/gN7ohtkoUuuQmLCwEzn9V/AQn3MA8TOdE4WNeFi1K5IZq7vBRbeUY4yjF5
    Rblpz2NMEEe5k1I8uzLkdx2hRwWJahP4ZQsvKCtPO8+J3OaxHY9SGQPO+QKBgAv3
    zmi7ruAZO+jTmSlxwNPfkM3k2b9gZ5FSLglXKAPAKpViv78akDFRHcaMvJubk56y
    QO3OEYgh1xOP3cP9XWU5EJ2KISu5hwCO0zApREc/DZc3L7ovI/uU1y9BpPHLm0i5
    2gBCGNfcw8j6DzD9shEvByNYXn4FoSve9ggqHkYXAoGBANLgosHw8ETrFXfwkKXN
    Qa4AC2xp19bdp7yeu0SWJ/C77Hrbj4UbDjjX5pUs3bNQ6/ZWyG3PmajLGIX3cP0w
    fM+JbJwxeyKaT2VD1ByaBRCLXKCw8AqN1uFSSpXDT2UxVgOq9QaxdbAhh4VMh4a1
    e0LqLFOGh3gT5+OjMke09GfY
    -----END PRIVATE KEY-----",

 );
    bq_peer_query
}

/// Parses a JSON file
/// containing the BigQuery configuration,
/// whose path is in environment.
pub fn fetch_bq_config() -> BigqueryConfig {
    let mut rng = rand::thread_rng();
    let run_id = rng.gen::<i64>();
    dotenv().ok();
    let bq_config_path = env::var("PEERDB_BQ_CONFIG_PATH").unwrap();
    let mut bq_config_file = File::open(bq_config_path).expect("Failed to open BQ config");
    let mut bq_config_contents = String::new();
    bq_config_file
        .read_to_string(&mut bq_config_contents)
        .expect("Failed to read BQ config");
    let parsed_bq_config: BQConfig =
        serde_json::from_str(&bq_config_contents).expect("Failed to parse BQ Config JSON");
    let config_for_client: BigqueryConfig = BigqueryConfig {
        auth_type: parsed_bq_config.auth_type,
        project_id: parsed_bq_config.project_id,
        private_key_id: parsed_bq_config.private_key_id,
        private_key: parsed_bq_config.private_key,
        client_email: parsed_bq_config.client_email,
        client_id: parsed_bq_config.client_id,
        auth_uri: parsed_bq_config.auth_uri,
        token_uri: parsed_bq_config.token_uri,
        auth_provider_x509_cert_url: parsed_bq_config.auth_provider_x509_cert_url,
        client_x509_cert_url: parsed_bq_config.client_x509_cert_url,
        // Suffix with Run ID
        dataset_id: format!("{}_{}", parsed_bq_config.dataset_id, run_id),
    };
    config_for_client
}

/// Initialises a BQ client (for running tests on it)
pub async fn get_bq_client(bq_config: BigqueryConfig) -> anyhow::Result<Client> {
    let bq_client = bq_client_from_config(bq_config).await?;
    Ok(bq_client)
}

pub struct BigQueryHelper {
    client: Box<Client>,
    config: BigqueryConfig,
}

impl BigQueryHelper {
    pub async fn new() -> anyhow::Result<Self> {
        let config = fetch_bq_config();
        let client = get_bq_client(config.clone())
            .await
            .expect("Failed to get BQ client and setup BQ helper");
        Ok(Self {
            config,
            client: Box::new(client),
        })
    }

    /// Deletes dataset with all its contents.
    pub async fn delete_dataset(&self) -> anyhow::Result<()> {
        let project_id = &self.config.project_id;
        let dataset_id = &self.config.dataset_id;
        self.client
            .dataset()
            .delete(project_id, dataset_id, true)
            .await?;
        Ok(())
    }

    /// Sequentially runs all the seeq sql queries for BQ.
    pub async fn run_bq_seed_queries(&self, queries: Vec<String>) -> anyhow::Result<()> {
        let project_id = &self.config.project_id;
        for query in queries {
            self.client
                .job()
                .query(project_id, QueryRequest::new(query))
                .await?;
        }
        Ok(())
    }

    /// Recreate dataset: delete the dataset if it exists
    /// and then create a new one
    pub async fn recreate_dataset(&self) {
        let project_id = &self.config.project_id;
        let dataset_id = &self.config.dataset_id;
        self.client
            .dataset()
            .delete_if_exists(project_id, dataset_id, true)
            .await;
        self.client
            .dataset()
            .create(
                Dataset::new(project_id, dataset_id)
                    .friendly_name("Dataset for PeerDB OSS tests")
                    .location("US")
                    .label("env", "test"),
            )
            .await
            .expect("Failed to create dataset");
    }
}
