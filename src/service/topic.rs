use crate::{CommandResponse, KvError, Value};
use dashmap::{DashMap, DashSet};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

const CAPACITY: usize = 128;

static NEXT_ID: AtomicU32 = AtomicU32::new(1);

/// generate next unique id in u32 format
fn get_next_subscription_id() -> u32 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

pub trait Topic: Send + Sync + 'static {
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>>;
    fn unsubscribe(self, name: String, id: u32) -> Result<u32, KvError>;
    fn publish(self, name: String, value: Arc<CommandResponse>);
}

#[derive(Default)]
pub struct PubSub {
    topics: DashMap<String, DashSet<u32>>,
    subscriptions: DashMap<u32, mpsc::Sender<Arc<CommandResponse>>>,
}

impl PubSub {
    pub fn remove_subscription(&self, name: &String, id: u32) -> Option<u32> {
        if let Some(v) = self.topics.get_mut(name) {
            v.remove(&id);

            if v.is_empty() {
                info!("Topic: {:?} is deleted", name);
                drop(v);
                self.topics.remove(name);
            }
        }

        debug!("Subscription {} is removed!", id);
        self.subscriptions.remove(&id).map(|(id, _)| id)
    }
}

impl Topic for Arc<PubSub> {
    fn subscribe(self, name: String) -> mpsc::Receiver<Arc<CommandResponse>> {
        let id = {
            let entry = self.topics.entry(name.clone()).or_default();
            let id = get_next_subscription_id();
            entry.insert(id);
            id
        };

        let (tx, rx) = mpsc::channel(CAPACITY);
        let txc = tx.clone();
        tokio::spawn(async move {
            let val: Value = (id as i64).into();
            if let Err(err) = tx.send(Arc::new(val.into())).await {
                warn!("failed to send: {}", err);
            };
        });
        debug!("add subscription with id {} name {}", id, name);
        self.subscriptions.insert(id, txc);
        rx
    }

    fn unsubscribe(self, name: String, id: u32) -> Result<u32, KvError> {
        if let Some(v) = self.topics.get_mut(&name) {
            v.remove(&id);

            if v.is_empty() {
                info!("Topic: {:?} is deleted", name);
                drop(v);
                self.topics.remove(&name);
            }
        }

        info!("Subscription {} is removed!", id);
        let s = self.subscriptions.remove(&id);

        match s {
            Some(sender) => {
                debug!("send cancel msg");
                tokio::spawn(async move {
                    let resp = CommandResponse::unsubscribe_ack();
                    sender.1.send(Arc::new(resp)).await.unwrap();
                });
                Ok(id)
            }
            None => Err(KvError::NotFound(name, format!("subscription {}", id))),
        }
    }

    fn publish(self, name: String, value: Arc<CommandResponse>) {
        tokio::spawn(async move {
            let mut ids = vec![];
            if let Some(topic) = self.topics.get(&name) {
                let subscriptions = topic.value().clone();
                drop(topic);

                for id in subscriptions.into_iter() {
                    if let Some(tx) = self.subscriptions.get(&id) {
                        if let Err(e) = tx.send(value.clone()).await {
                            warn!("Publish to {} failed! error: {:?}", id, e);
                            ids.push(id);
                        }
                    }
                }
            }

            for id in ids {
                self.remove_subscription(&name, id);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{assert_res_error, assert_res_ok};
    use std::convert::TryInto;

    #[tokio::test]
    async fn pub_sub_should_work() {
        let b = Arc::new(PubSub::default());
        let cae = "cae".to_string();

        // subscribe
        let mut stream1 = b.clone().subscribe(cae.clone());
        let mut stream2 = b.clone().subscribe(cae.clone());

        // publish
        let v: Value = "hello".into();
        b.clone().publish(cae.clone(), Arc::new(v.clone().into()));

        // get id first
        let id1: i64 = stream1.recv().await.unwrap().as_ref().try_into().unwrap();
        let id2: i64 = stream2.recv().await.unwrap().as_ref().try_into().unwrap();

        assert!(id1 != id2);

        let res1 = stream1.recv().await.unwrap();
        let res2 = stream2.recv().await.unwrap();

        assert_eq!(res1, res2);
        assert_res_ok(Arc::clone(&res1).as_ref().to_owned(), &[v.clone()], &[]);

        // stream1 unsubscribe
        b.clone().unsubscribe(cae.clone(), id1 as _).unwrap();

        // publish
        let v: Value = "world".into();
        b.clone().publish(cae.clone(), Arc::new(v.clone().into()));

        let cancel = stream1.recv().await.unwrap();
        assert_res_error(Arc::clone(&cancel).as_ref().to_owned(), 0, "");

        assert!(stream1.recv().await.is_none());

        let res2 = stream2.recv().await.unwrap();
        assert_res_ok(Arc::clone(&res2).as_ref().to_owned(), &[v.clone()], &[]);
    }
}
