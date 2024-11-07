use pt::peerdb_peers::{PostgresConfig, SshConfig};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::fmt::Write;
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
    host_server: String,
    host_port: u16,
    remote_server: String,
    remote_port: u16,
) -> std::io::Result<(ssh2::Session, std::net::SocketAddr)> {
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
    let listener = std::net::TcpListener::bind((host_server.as_str(), host_port))?;
    let local_addr = listener.local_addr()?;
    let channel = session.channel_direct_tcpip(remote_server.as_str(), remote_port, None)?;
    tracing::info!(
        "tunnel on {:}:{:} to {:}:{:} opened",
        host_server.as_str(),
        host_port,
        remote_server.as_str(),
        remote_port
    );

    tokio::task::spawn_blocking(move || {
        let mut stream_id = 0;
        let closed = Arc::new(AtomicBool::new(false));
        loop {
            match listener.accept() {
                Err(err) => {
                    tracing::info!("failed to accept connection, reason {:?}", err);
                    closed.store(false, Ordering::SeqCst);
                    break;
                }
                Ok((stream, socket)) => {
                    tracing::debug!("new TCP stream from socket {:}", socket);

                    let mut reader_stream = stream;
                    let mut writer_stream = reader_stream.try_clone().unwrap();

                    let mut writer_channel = channel.stream(stream_id); //open two streams on the same channel, so we can read and write separately
                    let mut reader_channel = channel.stream(stream_id);
                    stream_id = stream_id.wrapping_add(1);

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
                }
            }
        }
        tracing::info!(
            "tunnel on {:}:{:} to {:}:{:} closed",
            host_server.as_str(),
            host_port,
            remote_server.as_str(),
            remote_port
        );
    });

    Ok((session, local_addr))
}

pub async fn connect_postgres(
    config: &PostgresConfig,
) -> anyhow::Result<(tokio_postgres::Client, Option<ssh2::Session>)> {
    let (connection_string, session) = if let Some(ssh_config) = &config.ssh_config {
        let tcp = std::net::TcpStream::connect((ssh_config.host.as_str(), ssh_config.port as u16))?;
        let (session, local_addr) = create_tunnel(
            tcp,
            ssh_config,
            String::from("localhost"),
            0,
            config.host.clone(),
            config.port as u16,
        )?;
        let mut newconfig = config.clone();
        newconfig.host = local_addr.ip().to_string();
        newconfig.port = local_addr.port() as u32;
        (get_pg_connection_string(&newconfig), Some(session))
    } else {
        (get_pg_connection_string(config), None)
    };

    let mut tls_config = ClientConfig::builder()
        .with_root_certificates(RootCertStore::empty())
        .with_no_client_auth();
    tls_config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoCertificateVerification));
    let tls_connector = MakeRustlsConnect::new(tls_config);
    let (client, connection) = tokio_postgres::connect(&connection_string, tls_connector)
        .await
        .map_err(|e| anyhow::anyhow!("error encountered while connecting to postgres {:?}", e))?;

    tokio::task::spawn(async move {
        if let Err(e) = connection.await {
            tracing::info!("connection error: {}", e)
        }
    });

    Ok((client, session))
}
