use crate::network::{Acceptor, Connector};
use anyhow::Result;
use snow::{params::NoiseParams, Builder};
use snowstorm::{NoiseStream, SnowstormError};
use std::future::Future;
use tokio::io::{AsyncRead, AsyncWrite};

pub struct NoiseServer<'a> {
    secret: &'a [u8],
}

pub struct NoiseClient<'a> {
    secret: &'a [u8],
}

impl<'a> NoiseServer<'a> {
    pub fn new(secret: &'a [u8]) -> Self {
        Self { secret }
    }

    pub async fn accept<S>(&self, stream: S) -> Result<NoiseStream<S>, SnowstormError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let params: NoiseParams = "Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s".parse()?;
        // initialize our responder using a builder
        let builder: Builder<'_> = Builder::new(params);
        let static_key = builder.generate_keypair()?.private;
        let noise = builder
            .local_private_key(&static_key)
            .psk(3, self.secret)
            .build_responder()?;
        // Start handshaking
        NoiseStream::handshake(stream, noise).await
    }
}

impl<S> Acceptor<S> for NoiseServer<'_>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = NoiseStream<S>;
    type Error = SnowstormError;
    fn accept(&self, input: S) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send {
        async move { NoiseServer::accept(&self, input).await }
    }
}

impl<'a> NoiseClient<'a> {
    pub fn new(secret: &'a [u8]) -> Self {
        Self { secret }
    }

    pub async fn connect<S>(&self, stream: S) -> Result<NoiseStream<S>, SnowstormError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let params: NoiseParams = "Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s".parse()?;
        let builder: Builder<'_> = Builder::new(params);
        let static_key = builder.generate_keypair()?.private;
        let noise = builder
            .local_private_key(&static_key)
            .psk(3, self.secret)
            .build_initiator()?;
        // Start handshaking
        NoiseStream::handshake(stream, noise).await
    }
}

impl<S> Connector<S> for NoiseClient<'_>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = NoiseStream<S>;
    type Error = SnowstormError;
    fn connect(&self, input: S) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send {
        async move { NoiseClient::connect(self, input).await }
    }
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

    #[tokio::test]
    async fn noise_should_work() -> Result<()> {
        let addr = start_server().await?;

        let connector = NoiseClient::new(b"keykeykeykeykeykeykeykeykeykeyke");
        let stream = TcpStream::connect(addr).await?;
        let mut stream = connector.connect(stream).await?;
        stream.write_all(b"hello world!").await?;
        let mut buf = [0; 12];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"hello world!");

        Ok(())
    }

    async fn start_server() -> Result<SocketAddr> {
        let acceptor = NoiseServer::new(b"keykeykeykeykeykeykeykeykeykeyke");

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
