use pt::peerdb_peers::{PostgresConfig, SshConfig};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::fmt::Write;
use std::io;
use std::sync::Arc;
use tokio::net::UnixStream;
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_util::compat::FuturesAsyncReadCompatExt;

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

    connection_string.push_str(&urlencoding::encode(&config.user));
    if !config.password.is_empty() {
        connection_string.push(':');
        connection_string.push_str(&urlencoding::encode(&config.password));
    }

    // Add the timeout as a query parameter, sslmode changes here appear to be useless
    write!(
        connection_string,
        "@{}:{}/{}?connect_timeout=15&application_name=peerdb_nexus",
        config.host,
        config.port,
        urlencoding::encode(&config.database)
    )
    .ok();

    connection_string
}

pub async fn create_tunnel(
    tcp: std::net::TcpStream,
    ssh_config: &SshConfig,
    remote_server: String,
    remote_port: u16,
) -> io::Result<(ssh2::Session, UnixStream)> {
    let mut session = ssh2::Session::new()?;
    session.set_tcp_stream(tcp);
    session.set_compress(true);
    session.handshake()?;
    if !ssh_config.password.is_empty() {
        session.userauth_password(&ssh_config.user, &ssh_config.password)?;
    }
    if !ssh_config.private_key.is_empty() {
        session.userauth_pubkey_memory(&ssh_config.user, None, &ssh_config.private_key, None)?;
    }
    if !ssh_config.host_key.is_empty() {
        let mut known_hosts = session.known_hosts()?;
        known_hosts.read_str(&ssh_config.host_key, ssh2::KnownHostFileKind::OpenSSH)?;
    }
    let (mut stream1, stream2) = tokio::net::UnixStream::pair()?;
    let channel = session.channel_direct_tcpip(remote_server.as_str(), remote_port, None)?;
    tracing::info!(
        "tunnel to {:}:{:} opened",
        remote_server.as_str(),
        remote_port
    );

    session.set_blocking(false);
    tokio::spawn(async move {
        let mut channel_stream = futures_util::io::AllowStdIo::new(channel.stream(0)).compat();
        loop {
            if let Err(err) = tokio::io::copy_bidirectional(&mut stream1, &mut channel_stream).await
            {
                if err.kind() == io::ErrorKind::WouldBlock {
                    tokio::time::sleep(std::time::Duration::new(0, 123456789)).await;
                    continue;
                }
                tracing::error!(
                    "tunnel to {:}:{:} failed: {:}",
                    remote_server.as_str(),
                    remote_port,
                    err
                );
            }
            break;
        }
    });

    Ok((session, stream2))
}

pub async fn connect_postgres(
    config: &PostgresConfig,
) -> anyhow::Result<(tokio_postgres::Client, Option<ssh2::Session>)> {
    if let Some(ssh_config) = &config.ssh_config {
        let tcp = std::net::TcpStream::connect((ssh_config.host.as_str(), ssh_config.port as u16))?;
        tcp.set_nodelay(true)?;
        let (session, stream) =
            create_tunnel(tcp, ssh_config, config.host.clone(), config.port as u16).await?;
        let (client, connection) = tokio_postgres::Config::default()
            .user(&config.user)
            .password(&config.password)
            .dbname(&config.database)
            .application_name("peerdb_nexus")
            .connect_raw(stream, tokio_postgres::NoTls)
            .await?;
        tokio::task::spawn(async move {
            if let Err(e) = connection.await {
                tracing::info!("connection error: {}", e)
            }
        });
        Ok((client, Some(session)))
    } else {
        let connection_string = get_pg_connection_string(config);

        let mut tls_config = ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        tls_config
            .dangerous()
            .set_certificate_verifier(Arc::new(NoCertificateVerification));
        let tls_connector = MakeRustlsConnect::new(tls_config);
        let (client, connection) = tokio_postgres::connect(&connection_string, tls_connector)
            .await
            .map_err(|e| {
                anyhow::anyhow!("error encountered while connecting to postgres {:?}", e)
            })?;
        tokio::task::spawn(async move {
            if let Err(e) = connection.await {
                tracing::info!("connection error: {}", e)
            }
        });
        Ok((client, None))
    }
}
