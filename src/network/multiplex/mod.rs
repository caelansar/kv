mod quic_multiplex;
mod yamux_multiplex;

use crate::{ClientStream, KvError};
use async_trait::async_trait;
pub use quic_multiplex::*;
pub use yamux_multiplex::*;

#[async_trait]
pub trait MultiplexStream {
    type InnerStream;
    async fn open_stream(&mut self) -> Result<ClientStream<Self::InnerStream>, KvError>;
}
