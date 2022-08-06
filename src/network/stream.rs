use crate::{FrameCodec, KvError};
use bytes::BytesMut;
use futures::{ready, FutureExt, Sink, Stream};
use std::{marker, pin::Pin, task::Poll};
use tokio::io::{AsyncRead, AsyncWrite};

use super::frame::read_frame;

pub struct FrameStream<S, F, T> {
    inner: S,
    wbuf: BytesMut,
    rbuf: BytesMut,
    written: usize,
    _f: marker::PhantomData<F>,
    _t: marker::PhantomData<T>,
}

impl<S, F, T> Unpin for FrameStream<S, F, T> {}

impl<S, F, T> FrameStream<S, F, T>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(s: S) -> FrameStream<S, F, T> {
        FrameStream {
            inner: s,
            written: 0,
            rbuf: BytesMut::new(),
            wbuf: BytesMut::new(),
            _f: marker::PhantomData,
            _t: marker::PhantomData,
        }
    }
}

impl<S, F, T> Stream for FrameStream<S, F, T>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    F: Unpin + Send + FrameCodec,
    T: Unpin + Send,
{
    type Item = Result<F, KvError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut rest = self.rbuf.split_off(0);

        let fut = read_frame(&mut self.inner, &mut rest);
        ready!(Box::pin(fut).poll_unpin(cx))?;

        self.rbuf.unsplit(rest);
        Poll::Ready(Some(F::decode_frame(&mut self.rbuf)))
    }
}
impl<S, F, T> Sink<T> for FrameStream<S, F, T>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    T: Unpin + Send + FrameCodec,
    F: Unpin + Send,
{
    type Error = KvError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: std::pin::Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
        item.encode_frame(&mut self.wbuf)?;
        Ok(())
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        while self.written != self.wbuf.len() {
            let rest = self.wbuf.split_off(0);
            let written = self.written;
            let n = ready!(Pin::new(&mut self.inner).poll_write(cx, &rest[written..]))?;
            self.wbuf.unsplit(rest);
            self.written += n;
        }
        self.wbuf.clear();
        self.written = 0;

        ready!(Pin::new(&mut self.inner).poll_flush(cx)?);
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx)?);

        ready!(Pin::new(&mut self.inner).poll_shutdown(cx)?);
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use futures::{SinkExt, StreamExt};

    use crate::{utils::DummyStream, CommandRequest, KvError};

    use super::FrameStream;

    #[tokio::test]
    async fn stream_should_work() -> Result<(), KvError> {
        let buf = BytesMut::new();
        let stream = DummyStream { buf };
        let mut stream = FrameStream::<_, CommandRequest, CommandRequest>::new(stream);
        let cmd = CommandRequest::new_hget("t1", "k1");
        stream.send(cmd.clone()).await?;

        if let Some(Ok(s)) = stream.next().await {
            assert_eq!(cmd, s);
        } else {
            assert!(false);
        }
        Ok(())
    }
}
