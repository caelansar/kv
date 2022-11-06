use std::{
    convert::TryInto,
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, StreamExt};
use tracing::info;

use crate::{CommandResponse, KvError};

/// get id during initialization, it also impl
/// Deref & DerefMut trait so that it allows
/// access to the inner Stream through a reference
pub struct StreamResult {
    pub id: u32,
    inner: Pin<Box<dyn Stream<Item = Result<CommandResponse, KvError>> + Send>>,
}

impl StreamResult {
    pub async fn new<T>(mut stream: T) -> Result<Self, KvError>
    where
        T: Stream<Item = Result<CommandResponse, KvError>> + Send + Unpin + 'static,
    {
        let id = match stream.next().await {
            Some(Ok(CommandResponse {
                status: 200,
                values: v,
                ..
            })) => {
                if v.is_empty() {
                    return Err(KvError::Internal("Invalid stream".into()));
                }
                let id: i64 = (&v[0]).try_into().unwrap();
                Ok(id as u32)
            }
            _ => Err(KvError::Internal("Invalid stream".into())),
        };

        Ok(StreamResult {
            inner: Box::pin(stream),
            id: id?,
        })
    }
}

impl Stream for StreamResult {
    type Item = Result<CommandResponse, KvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        info!("STREAMRESULT POLL NEXT");
        let v = Pin::new(&mut self.inner).poll_next(cx);
        match &v {
            Poll::Ready(Some(x)) => {
                if x.as_ref().map_or(false, |resp| resp.status == 0) {
                    Poll::Ready(None)
                } else {
                    v
                }
            }
            _ => v,
        }
    }
}

impl Deref for StreamResult {
    type Target = Pin<Box<dyn Stream<Item = Result<CommandResponse, KvError>> + Send>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for StreamResult {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
