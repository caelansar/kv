use std::io::{BufReader, Cursor};
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::rustls::server::{AllowAnyAuthenticatedClient, NoClientAuth};
use tokio_rustls::rustls::{
    Certificate, ClientConfig, ServerConfig, ALL_CIPHER_SUITES, ALL_VERSIONS,
};
use tokio_rustls::rustls::{PrivateKey, RootCertStore};
use tokio_rustls::rustls::{ServerName, DEFAULT_CIPHER_SUITES, DEFAULT_VERSIONS};
use tokio_rustls::TlsConnector;
use tokio_rustls::{
    client::TlsStream as ClientTlsStream, server::TlsStream as ServerTlsStream, TlsAcceptor,
};

use crate::network::{Acceptor, Connector};
use crate::KvError;

const ALPN: &str = "kv";

#[derive(Clone)]
pub struct TlsServer {
    inner: Arc<ServerConfig>,
}

#[derive(Clone)]
pub struct TlsClient {
    config: Arc<ClientConfig>,
    domain: Arc<String>,
}

impl TlsServer {
    pub fn new(cert: &str, key: &str, client_ca: Option<&str>) -> Result<Self, KvError> {
        let certs = load_certs(cert)?;
        let key = load_key(key)?;

        let suites = ALL_CIPHER_SUITES.to_vec();
        let versions = ALL_VERSIONS.to_vec();

        let mut client_auth = NoClientAuth::new();
        if client_ca.is_some() {
            let roots = load_certs(client_ca.unwrap())?;
            let mut client_auth_roots = RootCertStore::empty();
            for root in roots {
                client_auth_roots.add(&root).unwrap();
            }
            client_auth = AllowAnyAuthenticatedClient::new(client_auth_roots);
        }

        let mut config = ServerConfig::builder()
            .with_cipher_suites(&suites)
            .with_safe_default_kx_groups()
            .with_protocol_versions(&versions)
            .expect("inconsistent cipher-suites/versions specified")
            .with_client_cert_verifier(client_auth)
            .with_single_cert_with_ocsp_and_sct(certs, key, vec![], vec![])
            .expect("bad certificates/private key");

        config.alpn_protocols = vec![Vec::from(ALPN)];

        Ok(Self {
            inner: Arc::new(config),
        })
    }

    pub async fn accept<S>(&self, stream: S) -> Result<ServerTlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let acceptor = TlsAcceptor::from(self.inner.clone());
        Ok(acceptor
            .accept_with(stream, |x| {
                println!(">alpn_protocol{:?}", x.alpn_protocol());
                println!(">is_handshaking{:?}", x.is_handshaking());
            })
            .await?)
    }
}

impl<S> Acceptor<S> for TlsServer
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ServerTlsStream<S>;
    type Error = KvError;
    async fn accept(&self, input: S) -> anyhow::Result<Self::Output, Self::Error> {
        TlsServer::accept(self, input).await
    }
}

impl TlsClient {
    pub fn new(
        domain: impl Into<String>,
        identity: Option<(&str, &str)>,
        server_ca: Option<&str>,
    ) -> Result<Self, KvError> {
        let builder = ClientConfig::builder();

        let suites = DEFAULT_CIPHER_SUITES.to_vec();
        let versions = DEFAULT_VERSIONS.to_vec();

        let mut root_store = RootCertStore::empty();
        if let Some(server_ca) = server_ca {
            let server_ca = Cursor::new(server_ca);
            let mut reader = BufReader::new(server_ca);

            root_store.add_parsable_certificates(&rustls_pemfile::certs(&mut reader).unwrap());
        }

        let builder = builder
            .with_cipher_suites(&suites)
            .with_safe_default_kx_groups()
            .with_protocol_versions(&versions)
            .map_err(|_| KvError::CertifcateParseError("client", "protocol_version"))?
            .with_root_certificates(root_store);

        if let Some((cert, key)) = identity {
            let certs = load_certs(cert)?;
            let key = load_key(key)?;
            let config = builder
                .with_single_cert(certs, key)
                .map_err(|_| KvError::CertifcateParseError("client", "cert"))?;
            Ok(Self {
                config: Arc::new(config),
                domain: Arc::new(domain.into()),
            })
        } else {
            let config = builder.with_no_client_auth();
            Ok(Self {
                config: Arc::new(config),
                domain: Arc::new(domain.into()),
            })
        }
    }

    pub async fn connect<S>(&self, stream: S) -> Result<ClientTlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let dns = ServerName::try_from(self.domain.as_str())
            .map_err(|_| KvError::Internal("Invalid DNS name".into()))?;

        let stream = TlsConnector::from(self.config.clone())
            .connect(dns, stream)
            .await?;

        Ok(stream)
    }
}

impl<S> Connector<S> for TlsClient
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ClientTlsStream<S>;
    type Error = KvError;
    async fn connect(&self, input: S) -> anyhow::Result<Self::Output, Self::Error> {
        TlsClient::connect(self, input).await
    }
}

fn load_certs(cert: &str) -> Result<Vec<Certificate>, KvError> {
    let cert = Cursor::new(cert);
    let mut reader = BufReader::new(cert);
    Ok(rustls_pemfile::certs(&mut reader)
        .map_err(|_| KvError::CertifcateParseError("server", "cert"))?
        .into_iter()
        .map(Certificate)
        .collect())
}

fn load_key(key: &str) -> Result<PrivateKey, KvError> {
    let cursor = Cursor::new(key);
    let mut reader = BufReader::new(cursor);

    loop {
        match rustls_pemfile::read_one(&mut reader).expect("cannot parse private key .pem file") {
            Some(rustls_pemfile::Item::RSAKey(key)) => return Ok(PrivateKey(key)),
            Some(rustls_pemfile::Item::PKCS8Key(key)) => return Ok(PrivateKey(key)),
            None => break,
            _ => {}
        }
    }

    Err(KvError::CertifcateParseError("private", "key"))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use anyhow::Result;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    const CA_CERT: &str = include_str!("../../certs/ca.crt");
    const CLIENT_CERT: &str = include_str!("../../certs/client.crt");
    const CLIENT_KEY: &str = include_str!("../../certs/client.key");
    const SERVER_CERT: &str = include_str!("../../certs/server.crt");
    const SERVER_KEY: &str = include_str!("../../certs/server.key");

    #[tokio::test]
    async fn tls_should_work() -> Result<()> {
        let ca = Some(CA_CERT);

        let addr = start_server(None).await.unwrap();

        let connector = TlsClient::new("kv.test.com", None, ca)?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");

        Ok(())
    }

    #[tokio::test]
    async fn tls_with_client_cert_should_work() -> Result<()> {
        let client_identity = Some((CLIENT_CERT, CLIENT_KEY));
        let ca = Some(CA_CERT);

        let addr = start_server(ca.clone()).await.unwrap();

        let connector = TlsClient::new("kv.test.com", client_identity, ca)?;
        let stream = TcpStream::connect(addr).await?;
        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");

        Ok(())
    }

    #[tokio::test]
    async fn tls_with_bad_domain_should_not_work() -> Result<()> {
        let addr = start_server(None).await.unwrap();

        let connector = TlsClient::new("kv.wrong.com", None, Some(CA_CERT))?;
        let stream = TcpStream::connect(addr).await?;
        let result = connector.connect(stream).await;

        assert!(result.is_err());

        Ok(())
    }

    async fn start_server(ca: Option<&str>) -> Result<SocketAddr> {
        let acceptor = TlsServer::new(SERVER_CERT, SERVER_KEY, ca)?;

        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = echo.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = echo.accept().await.unwrap();
            let mut stream = acceptor.accept(stream).await.unwrap();
            let mut buf = [0; 12];
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        });

        Ok(addr)
    }
}
