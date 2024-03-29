use crate::{ClientStream, KvError, MultiplexStream};
use futures::{future, Future, StreamExt, TryStreamExt};
use std::marker::PhantomData;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, ConnectionError, Mode};

pub struct YamuxCtrl<S> {
    sender: mpsc::Sender<ControlMessage>,
    _conn: PhantomData<S>,
}

enum ControlMessage {
    OpenStream(oneshot::Sender<yamux::Stream>),
}

impl<S> YamuxCtrl<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new_server<F, Fut>(stream: S, config: Option<Config>, f: F)
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        let config = config.unwrap_or_default();

        let mut conn = Connection::new(stream.compat(), config, Mode::Server);

        tokio::spawn(
            futures::stream::poll_fn(move |cx| conn.poll_next_inbound(cx))
                .try_for_each_concurrent(None, f),
        );
    }

    pub fn new_client(stream: S, config: Option<Config>) -> Self {
        let config = config.unwrap_or_default();

        let mut conn = Connection::new(stream.compat(), config, Mode::Client);

        let (sender, mut receiver) = mpsc::channel(32);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Process control messages (opening new streams)
                    Some(message) = receiver.recv() => {
                       match message {
                            ControlMessage::OpenStream(resp_sender) => {
                                let stream = future::poll_fn(|cx| conn.poll_new_outbound(cx))
                                    .await
                                    .unwrap();

                                resp_sender.send(stream).unwrap()
                            }
                        }
                    }
                    // Drive the connection by repeatedly calling poll_next_inbound
                    _ = noop_server(
                        futures::stream::poll_fn(|cx| {
                            conn.poll_next_inbound(cx)
                        })
                    ) => {
                        unreachable!()
                    }
                }
            }
        });

        Self {
            sender,
            _conn: Default::default(),
        }
    }
}

/// For each incoming stream, do nothing.
pub async fn noop_server(
    c: impl futures::Stream<Item = Result<yamux::Stream, yamux::ConnectionError>>,
) {
    c.for_each_concurrent(None, |maybe_stream| {
        drop(maybe_stream);
        future::ready(())
    })
    .await;
}

impl<S> MultiplexStream for YamuxCtrl<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type InnerStream = Compat<yamux::Stream>;
    async fn open_stream(&mut self) -> Result<ClientStream<Self::InnerStream>, KvError> {
        let (resp_sender, resp_receiver) = oneshot::channel();
        self.sender
            .send(ControlMessage::OpenStream(resp_sender))
            .await
            .unwrap();
        let stream = resp_receiver.await.unwrap();

        Ok(ClientStream::new(stream.compat()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{Acceptor, Connector};
    use crate::{
        assert_res_ok,
        network::noise::{NoiseClient, NoiseServer},
        CommandRequest, MemTable, ServerStream, Service, ServiceInner, Storage, TlsClient,
        TlsServer,
    };
    use anyhow::Result;
    use std::net::SocketAddr;
    use tokio::net::{TcpListener, TcpStream};
    use tracing::warn;

    #[tokio::test]
    async fn yamux_ctrl_client_server_should_work() -> Result<()> {
        let acceptor = NoiseServer::new(b"keykeykeykeykeykeykeykeykeykeyke");
        let addr = start_yamux_server("127.0.0.1:6666", acceptor, MemTable::new()).await?;

        let connector = NoiseClient::new(b"keykeykeykeykeykeykeykeykeykeyke");
        let stream = TcpStream::connect(addr).await.unwrap();
        let stream = connector.connect(stream).await.unwrap();

        // yamux client with noise
        let ctrl = YamuxCtrl::new_client(stream, None);

        run_test(ctrl).await?;

        Ok(())
    }

    #[tokio::test]
    async fn yamux_ctrl_with_tls_client_server_should_work() -> Result<()> {
        const CA_CERT: &str = include_str!("../../../certs/ca.crt");
        const CLIENT_CERT: &str = include_str!("../../../certs/client.crt");
        const CLIENT_KEY: &str = include_str!("../../../certs/client.key");
        const SERVER_CERT: &str = include_str!("../../../certs/server.crt");
        const SERVER_KEY: &str = include_str!("../../../certs/server.key");

        let acceptor = TlsServer::new(SERVER_CERT, SERVER_KEY, None)?;
        let addr = start_yamux_server("127.0.0.1:8888", acceptor, MemTable::new()).await?;

        let connector = TlsClient::new("kv.test.com", None, Some(CA_CERT))?;
        let stream = TcpStream::connect(addr).await?;

        let stream = connector.connect(stream).await.unwrap();

        // yamux client with tls
        let ctrl = YamuxCtrl::new_client(stream, None);

        run_test(ctrl).await?;

        Ok(())
    }

    async fn run_test<T>(mut ctrl: T) -> Result<()>
    where
        T: MultiplexStream + Send + 'static,
        <T as MultiplexStream>::InnerStream: Send + Unpin + AsyncWrite + AsyncRead + 'static,
    {
        // open new yamux stream
        let mut stream1 = ctrl.open_stream().await?;

        tokio::spawn(async move {
            let mut stream = ctrl.open_stream().await.unwrap();

            let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
            stream.execute(&cmd).await.unwrap();

            let cmd = CommandRequest::new_hget("t1", "k1");
            let res = stream.execute(&cmd).await.unwrap();
            assert_res_ok(res, &["v1".into()], &[]);
        });

        stream1
            .execute(&CommandRequest::new_hset("t1", "k", "v".into()))
            .await?;
        let res = stream1
            .execute(&CommandRequest::new_hget("t1", "k1"))
            .await?;
        assert_res_ok(res, &["v1".into()], &[]);
        let res = stream1
            .execute(&CommandRequest::new_hget("t1", "k"))
            .await?;
        assert_res_ok(res, &["v".into()], &[]);

        Ok(())
    }

    async fn start_yamux_server<S>(
        addr: &str,
        acceptor: impl Acceptor<TcpStream> + Send + 'static,
        store: S,
    ) -> Result<SocketAddr>
    where
        S: Storage + Send + Sync + 'static,
    {
        let addr: SocketAddr = addr.parse().unwrap();
        let listener = TcpListener::bind(addr).await.unwrap();

        let service: Service<S> = ServiceInner::new(store).into();
        let f = |stream, service: Service<S>| {
            YamuxCtrl::new_server(stream, None, move |s| {
                let svc = service.clone();
                async move {
                    let stream = ServerStream::new(s.compat(), svc);
                    stream.process().await.unwrap();
                    Ok(())
                }
            });
        };

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _addr)) => match acceptor.accept(stream).await {
                        // new server stream
                        Ok(stream) => f(stream, service.clone()),
                        Err(e) => warn!("failed to process noise handshake: {:?}", e),
                    },
                    Err(e) => warn!("failed to process TCP: {:?}", e),
                }
            }
        });

        Ok(addr)
    }
}
