use pt::peerdb_peers::{PostgresConfig, SshConfig};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::fmt::Write;
use std::os::unix::net::UnixStream;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
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

// from https://github.com/alexcrichton/ssh2-rs/issues/218#issuecomment-1698814611
pub fn create_tunnel(
    tcp: std::net::TcpStream,
    ssh_config: &SshConfig,
    remote_server: String,
    remote_port: u16,
) -> std::io::Result<(ssh2::Session, UnixStream)> {
    let mut session = ssh2::Session::new()?;
    session.set_tcp_stream(tcp);
    session.set_compress(true);
    session.set_timeout(15000);
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
    let (stream1, stream2) = UnixStream::pair()?;
    let channel = session.channel_direct_tcpip(remote_server.as_str(), remote_port, None)?;
    tracing::info!(
        "tunnel to {:}:{:} opened",
        remote_server.as_str(),
        remote_port
    );

    tokio::task::spawn_blocking(move || {
        let closed = Arc::new(AtomicBool::new(false));
        let mut reader_stream = stream1;
        let mut writer_stream = reader_stream.try_clone().unwrap();

        let mut writer_channel = channel.stream(0); //open two streams on the same channel, so we can read and write separately
        let mut reader_channel = channel.stream(0);

        //pipe stream output into channel
        let write_closed = closed.clone();
        tokio::task::spawn_blocking(move || loop {
            match std::io::copy(&mut reader_stream, &mut writer_channel) {
                Ok(_) => (),
                Err(err) => {
                    tracing::info!("failed to write to channel, reason: {:?}", err);
                }
            }
            if write_closed.load(Ordering::SeqCst) {
                break;
            }
        });

        //pipe channel output into stream
        let read_closed = closed.clone();
        tokio::task::spawn_blocking(move || loop {
            match std::io::copy(&mut reader_channel, &mut writer_stream) {
                Ok(_) => (),
                Err(err) => {
                    tracing::info!("failed to read from channel, reason: {:?}", err);
                }
            }
            if read_closed.load(Ordering::SeqCst) {
                break;
            }
        });
        tracing::info!(
            "tunnel to {:}:{:} closed",
            remote_server.as_str(),
            remote_port
        );
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
            create_tunnel(tcp, ssh_config, config.host.clone(), config.port as u16)?;
        stream.set_nonblocking(true)?;
        let stream = tokio::net::UnixStream::from_std(stream)?;
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
