use crate::{ClientStream, KvError, MultiplexStream};
use async_trait::async_trait;
use futures::{future, AsyncRead as AR, AsyncWrite as AW, Future, TryStreamExt};
use s2n_quic::{stream::BidirectionalStream, Connection as QuicConn};
use std::marker::{self, PhantomData};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use yamux::{Config, Connection, ConnectionError, Control, Mode, WindowUpdateMode};

pub struct YamuxCtrl<S> {
    ctrl: Control,
    _conn: PhantomData<S>,
}

impl<S> YamuxCtrl<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new_client(stream: S, config: Option<Config>) -> Self {
        Self::new(stream, config, true, |_stream| future::ready(Ok(())))
    }

    pub fn new_server<F, Fut>(stream: S, config: Option<Config>, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        Self::new(stream, config, false, f)
    }

    fn new<F, Fut>(stream: S, config: Option<Config>, is_client: bool, f: F) -> Self
    where
        F: FnMut(yamux::Stream) -> Fut,
        F: Send + 'static,
        Fut: Future<Output = Result<(), ConnectionError>> + Send + 'static,
    {
        let mode = if is_client {
            Mode::Client
        } else {
            Mode::Server
        };

        let mut config = config.unwrap_or_default();
        config.set_window_update_mode(WindowUpdateMode::OnRead);

        let conn = Connection::new(stream.compat(), config, mode);

        let ctrl = conn.control();

        tokio::spawn(into_stream(conn).try_for_each_concurrent(None, f));

        Self {
            ctrl,
            _conn: marker::PhantomData,
        }
    }
}

impl<S> MultiplexStream for YamuxCtrl<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type InnerStream = Compat<yamux::Stream>;
    async fn open_stream(&mut self) -> Result<ClientStream<Self::InnerStream>, KvError> {
        let stream = self.ctrl.open_stream().await?;
        Ok(ClientStream::new(stream.compat()))
    }
}

type YamulResult<T> = std::result::Result<T, yamux::ConnectionError>;

/// Convert a Yamux connection into a futures::Stream
fn into_stream<T>(
    c: yamux::Connection<T>,
) -> impl futures::stream::Stream<Item = YamulResult<yamux::Stream>>
where
    T: AR + AW + Unpin,
{
    futures::stream::unfold(c, |mut c| async {
        match c.next_stream().await {
            Ok(None) => None,
            Ok(Some(stream)) => Some((Ok(stream), c)),
            Err(_) => None,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        assert_res_ok,
        network::noise::{NoiseClient, NoiseServer},
        ClientStream, CommandRequest, MemTable, ServerStream, Service, ServiceInner, Storage,
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
        let mut ctrl = YamuxCtrl::new_client(stream, None);

        // open new yamux stream
        let mut stream = ctrl.open_stream().await?;
        // let mut client = ClientStream::new(stream);

        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        stream.execute(&cmd).await.unwrap();

        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = stream.execute(&cmd).await.unwrap();
        assert_res_ok(res, &["v1".into()], &[]);

        Ok(())
    }

    async fn start_yamux_server<S: 'static>(
        addr: &str,
        acceptor: NoiseServer<'static>,
        store: S,
    ) -> Result<SocketAddr>
    where
        S: Storage + Send + Sync,
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
