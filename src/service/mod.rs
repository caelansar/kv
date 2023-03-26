use self::topic::PubSub;
use crate::{CommandRequest, CommandResponse, MemTable, Storage};
#[cfg(test)]
use crate::{Kvpair, Value};
use futures::{stream, Stream};
use std::{pin::Pin, sync::Arc};
use tracing::{debug, instrument};

mod command_service;
pub mod topic;
mod topic_service;

pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

pub type StreamingResponse = Pin<Box<dyn Stream<Item = Arc<CommandResponse>> + Send>>;

impl From<CommandResponse> for StreamingResponse {
    fn from(cmd: CommandResponse) -> Self {
        Box::pin(stream::once(async { Arc::new(cmd) }))
    }
}

pub trait TopicService {
    fn execute(self, chan: impl topic::Topic) -> StreamingResponse;
}

pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
    broadcaster: Arc<PubSub>,
}

impl<Store> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            broadcaster: Arc::clone(&self.broadcaster),
        }
    }
}

pub trait Hook<Arg> {
    fn hook(&self, arg: &Arg);
}

pub trait HookMut<Arg> {
    fn hook(&self, arg: &mut Arg);
}

impl<T> Hook<T> for Vec<fn(&T)> {
    fn hook(&self, arg: &T) {
        for f in self {
            f(arg)
        }
    }
}

impl<T> HookMut<T> for Vec<fn(&mut T)> {
    fn hook(&self, arg: &mut T) {
        for f in self {
            f(arg)
        }
    }
}

pub struct ServiceInner<Store> {
    store: Store,
    process: Processor<CommandRequest, CommandResponse>,
}

impl<Store: Storage> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        Self {
            inner: Arc::new(inner),
            broadcaster: Default::default(),
        }
    }
}

impl<Store> ServiceInner<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            process: Processor::new(),
        }
    }
    fn received_callback(mut self, c: impl Fn(&CommandRequest) + Send + Sync + 'static) -> Self {
        self.process.set_callback(c);
        self
    }
    fn before_send_callback(
        mut self,
        c: impl Fn(&mut CommandResponse) + Send + Sync + 'static,
    ) -> Self {
        self.process.set_mut_callback(c);
        self
    }
}

impl<Store: Storage> Service<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            inner: Arc::new(ServiceInner::new(store)),
            broadcaster: Default::default(),
        }
    }

    #[instrument(name = "service_execute", skip_all)]
    pub fn execute(&self, cmd: CommandRequest) -> StreamingResponse {
        debug!("Got request: {:?}", cmd);
        self.inner.process.process_events(&cmd);

        if let Some(true) = cmd.request_data.as_ref().map(|x| x.is_streaming()) {
            cmd.dispatch_steaming(Arc::clone(&self.broadcaster))
        } else {
            let mut res = cmd.dispatch(&self.inner.store);
            self.inner.process.process_events_mut(&mut res);
            debug!("Executed response: {:?}", res);

            res.into()
        }
    }
}

struct Processor<T, U> {
    callbacks: Vec<Box<dyn Fn(&T) + Send + Sync>>,
    mut_callbacks: Vec<Box<dyn Fn(&mut U) + Send + Sync>>,
}

impl<T, U> Processor<T, U> {
    fn new() -> Self {
        Self {
            callbacks: Vec::new(),
            mut_callbacks: Vec::new(),
        }
    }
    fn set_callback(&mut self, c: impl Fn(&T) + Send + Sync + 'static) {
        self.callbacks.push(Box::new(c));
    }
    fn set_mut_callback(&mut self, c: impl Fn(&mut U) + Send + Sync + 'static) {
        self.mut_callbacks.push(Box::new(c));
    }
    fn process_events(&self, data: &T) {
        for cb in self.callbacks.iter() {
            cb(data)
        }
    }
    fn process_events_mut(&self, data: &mut U) {
        for cb in self.mut_callbacks.iter() {
            cb(data)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::*;
    use futures::StreamExt;
    use http::StatusCode;
    use tracing::info;

    #[tokio::test]
    async fn service_should_work() {
        let service = Service::new(MemTable::default());
        let service_c = service.clone();
        let handle = tokio::spawn(async move {
            let mut res = service_c.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            let res = res.next().await.unwrap();
            assert_res_ref_ok(&res, &[Value::default()], &[]);
        })
        .await
        .unwrap();

        let mut res = service.execute(CommandRequest::new_hget("t1", "k1"));
        let res = res.next().await.unwrap();
        assert_res_ref_ok(&res, &["v1".into()], &[]);
    }
    #[tokio::test]
    async fn hook_should_work() {
        fn on_received(cmd: &CommandRequest) {
            info!("Received {:?}", cmd);
        }
        fn before_send(res: &mut CommandResponse) {
            info!("Before send {:?}", res);
            res.status = StatusCode::CREATED.as_u16() as _;
        }
        let name = "cae";
        let service: Service = ServiceInner::new(MemTable::default())
            .received_callback(|_: &CommandRequest| {})
            .received_callback(on_received)
            .before_send_callback(before_send)
            .received_callback(move |_| info!("HOLA {}", name))
            .into();
        let mut res = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        let res = res.next().await.unwrap();
        assert_eq!(res.status, StatusCode::CREATED.as_u16() as u32);
        assert_eq!(res.message, "");
        assert_eq!(res.values, vec![Value::default()]);
    }
}

#[cfg(test)]
pub fn assert_res_ref_ok(res: &CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    let mut sorted_pairs = res.pairs.clone();
    sorted_pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

#[cfg(test)]
pub fn assert_res_ref_error(res: &CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}

#[cfg(test)]
pub fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

#[cfg(test)]
pub fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}
