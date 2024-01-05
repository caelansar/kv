mod quic_multiplex;
mod yamux_multiplex;

use crate::{ClientStream, KvError};
pub use quic_multiplex::*;
pub use yamux_multiplex::*;

pub trait MultiplexStream {
    type InnerStream;
    async fn open_stream(&mut self) -> Result<ClientStream<Self::InnerStream>, KvError>;
}
