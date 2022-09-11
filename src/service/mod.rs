use crate::{CommandRequest, CommandResponse, MemTable, Storage};
#[cfg(test)]
use crate::{Kvpair, Value};
use std::sync::Arc;
use tracing::debug;

mod command_service;
mod topic;

pub trait CommandService {
    fn execute(self, store: &impl Storage) -> CommandResponse;
}

pub struct Service<Store = MemTable> {
    inner: Arc<ServiceInner<Store>>,
}

impl<Store> Clone for Service<Store> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
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
    on_received: Vec<fn(&CommandRequest)>,
    before_send: Vec<fn(&mut CommandResponse)>,
}

impl<Store: Storage> From<ServiceInner<Store>> for Service<Store> {
    fn from(inner: ServiceInner<Store>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<Store> ServiceInner<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            on_received: Vec::new(),
            before_send: Vec::new(),
        }
    }
    pub fn fn_received(mut self, f: fn(&CommandRequest)) -> Self {
        self.on_received.push(f);
        self
    }
    pub fn fn_before_send(mut self, f: fn(&mut CommandResponse)) -> Self {
        self.before_send.push(f);
        self
    }
}

impl<Store: Storage> Service<Store> {
    pub fn new(store: Store) -> Self {
        Self {
            inner: Arc::new(ServiceInner::new(store)),
        }
    }

    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("Got request: {:?}", cmd);
        self.inner.on_received.hook(&cmd);
        let mut res = cmd.dispatch(&self.inner.store);
        self.inner.before_send.hook(&mut res);
        debug!("Executed response: {:?}", res);

        res
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::*;
    use http::StatusCode;
    use std::thread;
    use tracing::info;

    #[test]
    fn service_should_work() {
        let service = Service::new(MemTable::default());
        let service_c = service.clone();
        let handle = thread::spawn(move || {
            let res = service_c.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            assert_res_ok(res, &[Value::default()], &[]);
        });
        handle.join().unwrap();
        let res = service.execute(CommandRequest::new_hget("t1", "k1"));
        assert_res_ok(res, &["v1".into()], &[]);
    }
    #[test]
    fn hook_should_work() {
        fn on_received(cmd: &CommandRequest) {
            info!("Received {:?}", cmd);
        }
        fn before_send(res: &mut CommandResponse) {
            info!("Before send {:?}", res);
            res.status = StatusCode::CREATED.as_u16() as _;
        }
        let service: Service = ServiceInner::new(MemTable::default())
            .fn_received(|_: &CommandRequest| {})
            .fn_received(on_received)
            .fn_before_send(before_send)
            .into();
        let res = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        assert_eq!(res.status, StatusCode::CREATED.as_u16() as u32);
        assert_eq!(res.message, "");
        assert_eq!(res.values, vec![Value::default()]);
    }
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
