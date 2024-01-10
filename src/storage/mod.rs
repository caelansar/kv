mod db;
pub mod memory;

use crate::{KvError, Kvpair, Value};

pub trait Storage {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError>;
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError>;
    fn get_iter(&self, table: &str) -> Result<impl Iterator<Item = Kvpair>, KvError>;
}

pub struct StorageIter<T> {
    data: T,
}

impl<T> StorageIter<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }
}

impl<T> Iterator for StorageIter<T>
where
    T: Iterator,
    T::Item: Into<Kvpair>,
{
    type Item = Kvpair;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next().map(|v| v.into())
    }
}
