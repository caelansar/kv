mod quic_multiplex;
mod yamux_multiplex;

use crate::{ClientStream, KvError};
pub use quic_multiplex::*;
use std::future::Future;
pub use yamux_multiplex::*;

pub trait MultiplexStream {
    type InnerStream;
    fn open_stream(
        &mut self,
    ) -> impl Future<Output = Result<ClientStream<Self::InnerStream>, KvError>> + Send;
}
