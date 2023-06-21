use std::{fs::remove_file, time::Duration};

use pt::peers::KafkaConfig;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    ClientConfig,
};

pub async fn kf_connection_valid(config: KafkaConfig) -> anyhow::Result<bool> {
    let kafka_client = KafkaQueryExecutor::new(&config)?;
    let _ = remove_file("kafka.pem");
    let _ = kafka_client
        ._consumer
        .fetch_metadata(None, Duration::from_secs(20))?;

    Ok(true)
}

pub struct KafkaQueryExecutor {
    _config: KafkaConfig,
    _consumer: BaseConsumer,
}

impl KafkaQueryExecutor {
    pub fn new(config: &KafkaConfig) -> anyhow::Result<Self> {
        let kafka_config = config.clone();
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", kafka_config.servers)
            .set("security.protocol", kafka_config.security_protocol)
            .set("ssl.ca.location", "kafka.pem")
            .set("sasl.mechanism", "PLAIN")
            .set("sasl.username", kafka_config.username)
            .set("sasl.password", kafka_config.password)
            .create()?;

        Ok(Self {
            _config: config.clone(),
            _consumer: consumer,
        })
    }
}
