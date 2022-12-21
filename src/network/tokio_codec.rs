use crate::KvError;
use bytes::{Buf, BufMut};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use prost::Message;
use std::{
    io::{Read, Write},
    marker,
};
use tokio_util::codec::{Decoder, Encoder};
use tracing::debug;

const LENGTH: usize = 4;
const MAX_FRAME: usize = 2u32.pow(31) as usize;
// 1500(mtu) - 20(ip header) - 20(tcp header) - 20(others) - 4(length)
const COMPRESSION_LIMIT: usize = 1436;
const COMPRESSION_BIT: usize = 1 << 31;

pub struct CompressionCodec<T: Message + Sized + Default, U: Message + Sized + Default> {
    _t: marker::PhantomData<T>,
    _u: marker::PhantomData<U>,
}

impl<T: Message + Sized + Default, U: Message + Sized + Default> CompressionCodec<T, U> {
    pub fn new() -> Self {
        CompressionCodec {
            _t: marker::PhantomData,
            _u: marker::PhantomData,
        }
    }
}

impl<T: Message + Sized + Default, U: Message + Sized + Default> Encoder<T>
    for CompressionCodec<T, U>
{
    type Error = KvError;

    fn encode(&mut self, item: T, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let size = item.encoded_len();

        debug!("max frame is: {}", MAX_FRAME);
        if size > MAX_FRAME {
            return Err(KvError::FrameError("length exceed".to_string()));
        }

        dst.put_u32(size as u32);

        if size > COMPRESSION_LIMIT {
            debug!("encode compression");
            let mut buf1 = Vec::with_capacity(size);
            item.encode(&mut buf1)?;

            let msg = dst.split_off(LENGTH);
            dst.clear();
            debug!("buf after clear: {:?}", dst);

            let mut encoder = GzEncoder::new(msg.writer(), Compression::default());
            encoder.write_all(&buf1[..])?;

            let msg = encoder.finish()?.into_inner();
            // compression flag & length
            dst.put_u32((msg.len() | COMPRESSION_BIT) as u32);
            // msg paylod
            dst.unsplit(msg);
            Ok(())
        } else {
            item.encode(dst)?;
            Ok(())
        }
    }
}

impl<T: Message + Sized + Default, U: Message + Sized + Default> Decoder
    for CompressionCodec<T, U>
{
    type Item = U;

    type Error = KvError;

    fn decode(&mut self, buf: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() < LENGTH {
            // NOTE: Not enough data
            return Ok(None);
        }
        let header = buf.get_u32() as usize;
        let len = header & !COMPRESSION_BIT;
        let compressed = header & COMPRESSION_BIT == COMPRESSION_BIT;

        if compressed {
            debug!("decode compression");
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut buf1 = Vec::with_capacity(len * 2);
            decoder.read_to_end(&mut buf1)?;

            let msg = Self::Item::decode(&buf1[..buf1.len()])?;
            buf.advance(len);
            Ok(Some(msg))
        } else {
            let msg = Self::Item::decode(&buf[..len])?;
            buf.advance(len);
            Ok(Some(msg))
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    use crate::CommandRequest;

    use super::CompressionCodec;

    #[test]
    fn codec_should_work() {
        let mut codec: CompressionCodec<CommandRequest, CommandRequest> = CompressionCodec::new();
        let req = CommandRequest::new_hget("a", "b");
        let req_clone = req.clone();

        let mut output = BytesMut::new();
        codec.encode(req, &mut output).unwrap();

        let req1 = codec.decode(&mut output).unwrap().unwrap();

        assert_eq!(req1, req_clone);
        assert_eq!(0, output.len());
    }
}
