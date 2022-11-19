use crate::{ClientStream, KvError, MultiplexStream};
use async_trait::async_trait;
use s2n_quic::{stream::BidirectionalStream, Connection as QuicConn};

pub struct QuicCtrl {
    ctrl: QuicConn,
}

impl QuicCtrl {
    pub fn new(conn: QuicConn) -> Self {
        Self { ctrl: conn }
    }
}

#[async_trait]
impl MultiplexStream for QuicCtrl {
    type InnerStream = BidirectionalStream;
    async fn open_stream(&mut self) -> Result<ClientStream<Self::InnerStream>, KvError> {
        let stream = self.ctrl.open_bidirectional_stream().await?;
        Ok(ClientStream::new(stream))
    }
}
