use std::fmt::Write;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use pt::peerdb_peers::PostgresConfig;

pub fn get_pg_connection_string(config: &PostgresConfig) -> String {
    let mut connection_string = String::from("postgres://");

    connection_string.push_str(&config.user);
    if !config.password.is_empty() {
        connection_string.push(':');
        connection_string.push_str(&urlencoding::encode(&config.password));
    }

    // Add the timeout as a query parameter, sslmode changes here appear to be useless
    write!(connection_string, "@{}:{}/{}?connect_timeout=15", config.host, config.port, config.database).ok();

    connection_string
}

pub async fn connect_postgres(config: &PostgresConfig) -> anyhow::Result<tokio_postgres::Client> {
    let connection_string = get_pg_connection_string(config);

    let mut builder = SslConnector::builder(SslMethod::tls())?;
    // NONE seems roughly equivalent to the guarantees of sslmode=prefer or sslmode=require
    // PEER seems to be equivalent to sslmode=verify-ca or sslmode=verify-full, requires presence of root certs.
    builder.set_verify(SslVerifyMode::NONE);

    let tls_connector = MakeTlsConnector::new(builder.build());
    let (client, connection) = tokio_postgres::connect(&connection_string, tls_connector)
        .await
        .map_err(|e| anyhow::anyhow!("error encountered while connecting to postgres {:?}", e))?;

    tokio::task::Builder::new()
        .name("PostgresQueryExecutor connection")
        .spawn(async move {
            if let Err(e) = connection.await {
                tracing::info!("connection error: {}", e)
            }
        })?;

    Ok(client)
}
