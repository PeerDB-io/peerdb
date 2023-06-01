use dotenvy::dotenv;
use gcp_bigquery_client::model::{dataset::Dataset, query_request::QueryRequest};
use gcp_bigquery_client::{dataset, Client};
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
    pub async fn run_bq_queries(&self, queries: Vec<String>) -> anyhow::Result<()> {
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
