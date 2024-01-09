pub mod abi;

use super::service::topic::Topic;
use crate::{command_request::RequestData, *};
use abi::*;
use bytes::Bytes;
use futures::stream;
use http::StatusCode;
use prost::Message;
use std::sync::Arc;

impl CommandRequest {
    pub fn dispatch(self, store: &impl Storage) -> CommandResponse {
        match self.request_data {
            Some(request_data) => {
                if request_data.is_streaming() {
                    KvError::InvalidCommand("Not command".into()).into()
                } else {
                    service::CommandService::execute(request_data, store)
                }
            }
            None => KvError::InvalidCommand("Request has no data".into()).into(),
        }
    }

    pub fn dispatch_streaming(self, topic: impl Topic) -> StreamingResponse {
        match self.request_data {
            Some(request_data) => {
                if !request_data.is_streaming() {
                    Box::pin(stream::once(async {
                        Arc::new(KvError::InvalidCommand("Not streaming command".into()).into())
                    }))
                } else {
                    service::TopicService::execute(request_data, topic)
                }
            }
            None => Box::pin(stream::once(async {
                Arc::new(KvError::InvalidCommand("Request has no data".into()).into())
            })),
        }
    }

    pub fn new_hget(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hget(Hget {
                table: table.into(),
                key: key.into(),
            })),
        }
    }

    pub fn new_hset(table: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self {
            request_data: Some(RequestData::Hset(Hset {
                table: table.into(),
                pair: Some(Kvpair::new(key, value)),
            })),
        }
    }

    pub fn new_hdel(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hdel(Hdel {
                table: table.into(),
                key: key.into(),
            })),
        }
    }

    pub fn new_hexist(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hexist(Hexist {
                table: table.into(),
                key: key.into(),
            })),
        }
    }

    pub fn new_hmget(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hmget(Hmget {
                table: table.into(),
                keys,
            })),
        }
    }

    pub fn new_hgetall(table: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hgetall(Hgetall {
                table: table.into(),
            })),
        }
    }

    pub fn new_hmset(table: impl Into<String>, pairs: Vec<Kvpair>) -> Self {
        Self {
            request_data: Some(RequestData::Hmset(Hmset {
                table: table.into(),
                pairs,
            })),
        }
    }

    pub fn new_hmdel(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hmdel(Hmdel {
                table: table.into(),
                keys,
            })),
        }
    }

    pub fn new_hmexist(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hmexist(Hmexist {
                table: table.into(),
                keys,
            })),
        }
    }

    pub fn new_subscribe(topic: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Subscribe(Subscribe {
                topic: topic.into(),
            })),
        }
    }

    pub fn new_publish(topic: impl Into<String>, data: Vec<Value>) -> Self {
        Self {
            request_data: Some(RequestData::Publish(Publish {
                topic: topic.into(),
                data,
            })),
        }
    }

    pub fn new_unsubscribe(topic: impl Into<String>, id: u32) -> Self {
        Self {
            request_data: Some(RequestData::Unsubscribe(Unsubscribe {
                topic: topic.into(),
                id,
            })),
        }
    }
}

impl Kvpair {
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self {
            key: key.into(),
            value: Some(value),
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self {
            value: Some(value::Value::String(s)),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self {
            value: Some(value::Value::String(s.into())),
        }
    }
}

impl TryFrom<&[u8]> for Value {
    type Error = KvError;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        let msg = Value::decode(data)?;
        Ok(msg)
    }
}

impl From<i64> for Value {
    fn from(u: i64) -> Self {
        Self {
            value: Some(value::Value::Integer(u)),
        }
    }
}

impl From<Bytes> for Value {
    fn from(buf: Bytes) -> Self {
        Self {
            value: Some(value::Value::Binary(buf)),
        }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self {
            value: Some(value::Value::Bool(b)),
        }
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Self {
            value: Some(value::Value::Float(f)),
        }
    }
}

impl From<(String, Value)> for Kvpair {
    fn from(t: (String, Value)) -> Self {
        Self::new(t.0, t.1)
    }
}

impl From<Value> for CommandResponse {
    fn from(v: Value) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as u32,
            values: vec![v],
            ..Default::default()
        }
    }
}

impl From<Vec<Value>> for CommandResponse {
    fn from(v: Vec<Value>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as u32,
            values: v,
            ..Default::default()
        }
    }
}

impl From<Arc<CommandResponse>> for CommandResponse {
    fn from(a: Arc<CommandResponse>) -> Self {
        Self {
            status: a.status,
            message: a.message.clone(),
            values: a.values.clone(),
            pairs: a.pairs.clone(),
        }
    }
}

impl TryFrom<Value> for Vec<u8> {
    type Error = KvError;
    fn try_from(v: Value) -> Result<Self, Self::Error> {
        let mut buf = Vec::with_capacity(v.encoded_len());
        v.encode(&mut buf)?;
        Ok(buf)
    }
}

impl TryFrom<Value> for i64 {
    type Error = KvError;

    fn try_from(v: Value) -> Result<Self, Self::Error> {
        match v.value {
            Some(value::Value::Integer(i)) => Ok(i),
            _ => Err(KvError::ConvertError(v, "Integer")),
        }
    }
}

impl TryFrom<&Value> for i64 {
    type Error = KvError;

    fn try_from(v: &Value) -> Result<Self, Self::Error> {
        match v.value {
            Some(value::Value::Integer(i)) => Ok(i),
            _ => Err(KvError::ConvertError(v.to_owned(), "Integer")),
        }
    }
}

impl TryFrom<&CommandResponse> for i64 {
    type Error = KvError;

    fn try_from(value: &CommandResponse) -> Result<Self, Self::Error> {
        if value.status != StatusCode::OK.as_u16() as u32 {
            return Err(KvError::Internal("CommandResponse".into()));
        }
        match value.values.first() {
            Some(v) => v.try_into(),
            None => Err(KvError::Internal("CommandResponse".into())),
        }
    }
}

impl From<Vec<Kvpair>> for CommandResponse {
    fn from(v: Vec<Kvpair>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as u32,
            pairs: v,
            ..Default::default()
        }
    }
}

impl From<KvError> for CommandResponse {
    fn from(e: KvError) -> Self {
        let mut result = Self {
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16() as u32,
            message: e.to_string(),
            ..Default::default()
        };
        match e {
            KvError::NotFound(_, _) => result.status = StatusCode::NOT_FOUND.as_u16() as u32,
            KvError::InvalidCommand(_) => result.status = StatusCode::BAD_REQUEST.as_u16() as u32,
            _ => {}
        }
        result
    }
}

impl CommandResponse {
    pub fn ok() -> Self {
        let mut result = CommandResponse::default();
        result.status = StatusCode::OK.as_u16() as u32;
        result
    }

    pub fn unsubscribe_ack() -> Self {
        CommandResponse::default()
    }
}

impl From<KvError> for Value {
    fn from(_: KvError) -> Self {
        Self::default()
    }
}
