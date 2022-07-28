use bytes::BytesMut;
use prost::Message;

use crate::{CommandRequest, CommandResponse, KvError};

pub trait FrameCodec
where
    Self: Message + Sized + Default,
{
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError>;
    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError>;
}

impl FrameCodec for CommandRequest {
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        todo!()
    }

    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        todo!()
    }
}
impl FrameCodec for CommandResponse {
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        todo!()
    }

    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_request_encode_decode_should_work() {
        let mut buf = BytesMut::new();

        let cmd = CommandRequest::new_hget("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();

        assert_eq!(is_compressed(&buf), false);

        let cmd1 = CommandRequest::decode_frame(&mut buf).unwrap();
        assert_eq!(cmd, cmd1);
    }

    fn is_compressed(data: &[u8]) -> bool {
        if let &[v] = &data[..1] {
            v >> 7 == 1
        } else {
            false
        }
    }
}
