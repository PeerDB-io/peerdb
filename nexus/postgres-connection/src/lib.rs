use pt::peerdb_peers::PostgresConfig;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::fmt::Write;
use std::sync::Arc;
use tokio_postgres_rustls::MakeRustlsConnect;

#[derive(Copy, Clone, Debug)]
struct NoCertificateVerification;

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub fn get_pg_connection_string(config: &PostgresConfig) -> String {
    let mut connection_string = String::from("postgres://");

    connection_string.push_str(&config.user);
    if !config.password.is_empty() {
        connection_string.push(':');
        connection_string.push_str(&urlencoding::encode(&config.password));
    }

    // Add the timeout as a query parameter, sslmode changes here appear to be useless
    write!(
        connection_string,
        "@{}:{}/{}?connect_timeout=15",
        config.host, config.port, config.database
    )
    .ok();

    connection_string
}

pub async fn connect_postgres(config: &PostgresConfig) -> anyhow::Result<tokio_postgres::Client> {
    let connection_string = get_pg_connection_string(config);

    let mut config = ClientConfig::builder()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoCertificateVerification));
    let tls_connector = MakeRustlsConnect::new(config);
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
