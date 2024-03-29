mod frame;
mod multiplex;
mod noise;
mod stream;
mod stream_result;
mod tls;
mod tokio_codec;

use self::{frame::read_frame, stream_result::StreamResult, tokio_codec::CompressionCodec};
use crate::{CommandRequest, CommandResponse, KvError, Service, Storage};
use bytes::BytesMut;
pub use frame::FrameCodec;
use futures::{SinkExt, StreamExt};
pub use multiplex::*;
use std::fmt::Debug;
use std::future::Future;
use std::marker;
pub use tls::*;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::Framed;
use tracing::info;

trait Acceptor<Input> {
    type Output: AsyncRead + AsyncWrite + Send + Unpin;
    type Error: Debug;
    fn accept(
        &self,
        input: Input,
    ) -> impl Future<Output = anyhow::Result<Self::Output, Self::Error>> + Send;
}

trait Connector<Input> {
    type Output: AsyncRead + AsyncWrite + Send + Unpin + 'static;
    type Error: Debug;
    fn connect(
        &self,
        input: Input,
    ) -> impl Future<Output = anyhow::Result<Self::Output, Self::Error>> + Send;
}

#[deprecated]
pub struct FrameIO<S, F, T> {
    inner: S,
    _f: marker::PhantomData<F>,
    _t: marker::PhantomData<T>,
}

impl<S, F, T> FrameIO<S, F, T>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: FrameCodec,
    T: FrameCodec,
{
    fn new(s: S) -> FrameIO<S, F, T> {
        FrameIO {
            inner: s,
            _f: marker::PhantomData,
            _t: marker::PhantomData,
        }
    }
    async fn send(&mut self, msg: T) -> Result<(), KvError> {
        let mut buf = BytesMut::new();
        msg.encode_frame(&mut buf)?;
        let encoded = buf.freeze();
        self.inner.write_all(&encoded[..]).await?;
        Ok(())
    }
    async fn recv(&mut self) -> Result<F, KvError> {
        let mut buf = BytesMut::new();
        let stream = &mut self.inner;
        read_frame(stream, &mut buf).await?;
        F::decode_frame(&mut buf)
    }
}

pub struct ServerStream<S: AsyncRead + AsyncWrite, Store> {
    service: Service<Store>,
    inner: Framed<S, CompressionCodec<CommandResponse, CommandRequest>>,
}

pub struct ClientStream<S> {
    inner: Framed<S, CompressionCodec<CommandRequest, CommandResponse>>,
}

impl<S, Store> ServerStream<S, Store>
where
    Store: Storage,
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S, service: Service<Store>) -> Self {
        Self {
            inner: Framed::new(stream, CompressionCodec::new()),
            service,
        }
    }

    pub async fn process(mut self) -> Result<(), KvError> {
        while let Some(Ok(cmd)) = self.inner.next().await {
            info!("process command: {:?}", cmd);
            let mut res = self.service.execute(cmd);
            while let Some(data) = res.next().await {
                self.inner.send(data.into()).await?;
            }
        }
        info!("process ok, client disconnect");
        Ok(())
    }
}

impl<S> ClientStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(stream: S) -> Self {
        Self {
            inner: Framed::new(stream, CompressionCodec::new()),
        }
    }

    pub async fn execute(&mut self, cmd: &CommandRequest) -> Result<CommandResponse, KvError> {
        self.inner.send(cmd.clone()).await?;
        self.inner
            .next()
            .await
            .unwrap_or_else(|| Err(KvError::Internal("no response".into())))
    }

    pub async fn execute_streaming(self, cmd: &CommandRequest) -> Result<StreamResult, KvError> {
        let mut stream = self.inner;

        stream.send(cmd.clone()).await?;
        // stream.close().await?;

        StreamResult::new(stream).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{assert_res_ok, MemTable, Value};
    use anyhow::Result;
    use bytes::Bytes;
    use std::net::SocketAddr;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn client_server_basic_communication_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ClientStream::new(stream);

        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let res = client.execute(&cmd).await.unwrap();

        assert_res_ok(res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = client.execute(&cmd).await?;

        assert_res_ok(res, &["v1".into()], &[]);

        Ok(())
    }

    #[tokio::test]
    async fn client_server_compression_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;

        let stream = TcpStream::connect(addr).await?;
        let mut client = ClientStream::new(stream);

        let v: Value = Bytes::from(vec![0u8; 1437]).into();
        let cmd = CommandRequest::new_hset("t2", "k2", v.clone().into());
        let res = client.execute(&cmd).await?;

        assert_res_ok(res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hget("t2", "k2");
        let res = client.execute(&cmd).await?;

        assert_res_ok(res, &[v.into()], &[]);

        Ok(())
    }

    async fn start_server() -> Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let service: Service = Service::new(MemTable::new());
                let server = ServerStream::new(stream, service);
                tokio::spawn(server.process());
            }
        });

        Ok(addr)
    }
}

#[cfg(test)]
pub mod utils {
    use bytes::{BufMut, BytesMut};
    use tokio::io::{AsyncRead, AsyncWrite};

    pub struct DummyStream {
        pub buf: BytesMut,
    }

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let len = buf.capacity();
            let data = self.get_mut().buf.split_to(len);
            buf.put_slice(&data);

            std::task::Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for DummyStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            self.get_mut().buf.put_slice(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
}
