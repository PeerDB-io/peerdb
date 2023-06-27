use pt::peers::KafkaConfig;
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    ClientConfig,
};
use std::{
    fs::{remove_file, File},
    io::Write,
    time::Duration,
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
        let mut consumer_config = &mut ClientConfig::new();
        consumer_config = consumer_config.set("bootstrap.servers", kafka_config.servers);
        if kafka_config.security_protocol == "SASL_SSL" {
            let mut ssl_certificate = File::create("kafka.pem")?;
            let cert = kafka_config.ssl_certificate.as_bytes().to_vec();
            let _ = ssl_certificate.write_all(&cert);
            consumer_config = consumer_config
                .set("security.protocol", kafka_config.security_protocol)
                .set("ssl.ca.location", "kafka.pem")
                .set("sasl.mechanism", "PLAIN")
                .set("sasl.username", kafka_config.username)
                .set("sasl.password", kafka_config.password)
        }
        let consumer: BaseConsumer = consumer_config.create()?;

        Ok(Self {
            _config: config.clone(),
            _consumer: consumer,
        })
    }
}
