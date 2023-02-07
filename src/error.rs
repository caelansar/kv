use std::io;

use crate::Value;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum KvError {
    #[error("Not found for table: {0}, key: {1}")]
    NotFound(String, String),
    #[error("Cannot parse command: `{0}`")]
    InvalidCommand(String),
    #[error("Cannot convert value {:0} to {1}")]
    ConvertError(Value, &'static str),
    #[error("Cannot process command {0} with table: {1}, key: {2}. Error: {3}")]
    StorageError(&'static str, String, String, String),
    #[error("Failed to access sled db")]
    SledError(#[from] sled::Error),
    #[error("Failed to encode protobuf message")]
    EncodeError(#[from] prost::EncodeError),
    #[error("Failed to decode protobuf message")]
    DecodeError(#[from] prost::DecodeError),
    #[error("Frame error: {0}")]
    FrameError(String),
    #[error("Failed to parse certificate: {0}-{1}")]
    CertifcateParseError(&'static str, &'static str),
    #[error("IO error: {0}")]
    IOError(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Yamux Connection error")]
    YamuxConnectionError(String),
    #[error("Quic connection error")]
    QuicConnectionError(#[from] s2n_quic::connection::Error),
}

impl From<io::Error> for KvError {
    fn from(i: io::Error) -> Self {
        Self::IOError(i.to_string())
    }
}

impl From<yamux::ConnectionError> for KvError {
    fn from(e: yamux::ConnectionError) -> Self {
        Self::YamuxConnectionError(e.to_string())
    }
}
