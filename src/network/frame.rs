use bytes::{Buf, BufMut, BytesMut};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use prost::Message;
use std::io::{Read, Write};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

use crate::{CommandRequest, CommandResponse, KvError};

const LENGTH: usize = 4;
const MAX_FRAME: usize = 2u32.pow(31) as usize;
// 1500(mtu) - 20(ip header) - 20(tcp header) - 20(others) - 4(length)
const COMPRESSION_LIMIT: usize = 1436;
const COMPRESSION_BIT: usize = 1 << 31;

pub trait FrameCodec
where
    Self: Message + Sized + Default,
{
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        let size = self.encoded_len();

        debug!("max frame is: {}", MAX_FRAME);
        if size > MAX_FRAME {
            return Err(KvError::FrameError("length exceed".to_string()));
        }

        buf.put_u32(size as u32);

        if size > COMPRESSION_LIMIT {
            debug!("encode compression");
            let mut buf1 = Vec::with_capacity(size);
            self.encode(&mut buf1)?;

            let msg = buf.split_off(LENGTH);
            buf.clear();
            debug!("buf after clear: {:?}", buf);

            let mut encoder = GzEncoder::new(msg.writer(), Compression::default());
            encoder.write_all(&buf1[..])?;

            let msg = encoder.finish()?.into_inner();
            // compression flag & length
            buf.put_u32((msg.len() | COMPRESSION_BIT) as u32);
            // msg paylod
            buf.unsplit(msg);
            Ok(())
        } else {
            self.encode(buf)?;
            Ok(())
        }
    }
    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        let header = buf.get_u32() as usize;
        let len = header & !COMPRESSION_BIT;
        let compressed = header & COMPRESSION_BIT == COMPRESSION_BIT;

        if compressed {
            debug!("decode compression");
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut buf1 = Vec::with_capacity(len * 2);
            decoder.read_to_end(&mut buf1)?;

            let msg = Self::decode(&buf1[..buf1.len()])?;
            buf.advance(len);
            Ok(msg)
        } else {
            let msg = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(msg)
        }
    }
}

impl FrameCodec for CommandRequest {}
impl FrameCodec for CommandResponse {}

pub async fn read_frame<S>(stream: &mut S, buf: &mut BytesMut) -> Result<(), KvError>
where
    S: AsyncRead + Unpin + Send,
{
    let header = stream.read_u32().await? as usize;
    let len = header & !COMPRESSION_BIT;

    buf.reserve(LENGTH + len);
    buf.put_u32(header as _);

    unsafe { buf.advance_mut(len) };
    stream.read_exact(&mut buf[LENGTH..]).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{utils::DummyStream, Value};

    use super::*;
    use bytes::Bytes;
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

    #[test]
    fn command_request_encode_decode_should_work() {
        tracing_subscriber::registry().with(fmt::layer()).init();

        let mut buf = BytesMut::new();

        let cmd = CommandRequest::new_hget("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();

        assert_eq!(is_compressed(&buf), false);

        let cmd1 = CommandRequest::decode_frame(&mut buf).unwrap();
        assert_eq!(cmd, cmd1);

        // make sure buf is exhausted
        assert_eq!(0, buf.len());
    }

    #[test]
    fn command_response_encode_decode_should_work() {
        let mut buf = BytesMut::new();
        let values: Vec<Value> = vec![1.into(), "hello".into(), "world".into()];
        let res: CommandResponse = values.into();

        res.encode_frame(&mut buf).unwrap();
        assert_eq!(is_compressed(&buf), false);

        let res1 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(res, res1);
    }

    #[test]
    fn command_response_compressed_encode_decode_should_work() {
        let mut buf = BytesMut::new();
        let value: Value = Bytes::from(vec![0u8; COMPRESSION_LIMIT + 1]).into();
        let res: CommandResponse = value.into();

        res.encode_frame(&mut buf).unwrap();
        assert_eq!(is_compressed(&buf), true);

        let res1 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(res, res1);
    }

    #[tokio::test]
    async fn read_frame_should_work() {
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hget("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();

        let mut stream = DummyStream { buf };
        let mut data = BytesMut::new();

        read_frame(&mut stream, &mut data).await.unwrap();
        let cmd_c = CommandRequest::decode_frame(&mut data).unwrap();
        assert_eq!(cmd, cmd_c);
    }

    fn is_compressed(data: &[u8]) -> bool {
        if let &[v] = &data[..1] {
            v >> 7 == 1
        } else {
            false
        }
    }
}
