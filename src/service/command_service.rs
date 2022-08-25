use crate::{command_request::RequestData, *};

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for RequestData {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self {
            RequestData::Hget(param) => param.execute(store),
            RequestData::Hset(param) => param.execute(store),
            RequestData::Hdel(param) => param.execute(store),
            RequestData::Hexist(param) => param.execute(store),
            RequestData::Hmget(param) => param.execute(store),
            RequestData::Hmdel(param) => param.execute(store),
            RequestData::Hmset(param) => param.execute(store),
            RequestData::Hgetall(param) => param.execute(store),
            RequestData::Hmexist(param) => param.execute(store),
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => Value::default().into(),
        }
    }
}

impl CommandService for Hdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => Value::default().into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.contains(&self.table, &self.key) {
            Ok(v) => Value::from(v).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.pairs
            .into_iter()
            .map(
                |pair| match store.set(&self.table, pair.key, pair.value.unwrap_or_default()) {
                    Ok(Some(v)) => v.into(),
                    _ => Value::default(),
                },
            )
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hmget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.get(&self.table, key) {
                Ok(Some(v)) => v.into(),
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}
impl CommandService for Hmexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.contains(&self.table, key) {
                Ok(v) => Value::from(v).into(),
                Err(e) => e.into(),
            })
            .collect::<Vec<Value>>()
            .into()
    }
}

impl CommandService for Hmdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .iter()
            .map(|key| match store.del(&self.table, key) {
                Ok(Some(v)) => v,
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = cmd.clone().dispatch(&store);
        assert_res_ok(res, &[Value::default()], &[]);

        let res = cmd.dispatch(&store);
        assert_res_ok(res, &["world".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        cmd.dispatch(&store);
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = cmd.dispatch(&store);
        assert_res_ok(res, &[10.into()], &[]);
    }

    #[test]
    fn hdel_should_work() {
        let store = MemTable::new();
        set_key_pairs("score", vec![("u1", 10)], &store);

        let cmd = CommandRequest::new_hget("score", "u1");
        let res = cmd.dispatch(&store);
        assert_res_ok(res, &[10.into()], &[]);

        let cmd = CommandRequest::new_hdel("score", "u1");
        cmd.dispatch(&store);

        let cmd = CommandRequest::new_hget("score", "u1");
        let res = cmd.dispatch(&store);
        assert_res_error(res, 404, "");
    }

    #[test]
    fn hexist_should_work() {
        let store = MemTable::new();
        set_key_pairs("score", vec![("u1", 10)], &store);

        let cmd = CommandRequest::new_hexist("score", "u1");
        let res = cmd.dispatch(&store);
        assert_res_ok(res, &[true.into()], &[]);

        let cmd = CommandRequest::new_hdel("score", "u1");
        cmd.dispatch(&store);

        let cmd = CommandRequest::new_hexist("score", "u1");
        let res = cmd.dispatch(&store);
        assert_res_ok(res, &[false.into()], &[]);
    }

    #[test]
    fn hmget_should_work() {
        let store = MemTable::new();

        set_key_pairs(
            "score",
            vec![("u1", 10), ("u2", 8), ("u3", 22), ("u4", 6)],
            &store,
        );

        let cmd = CommandRequest::new_hmget("score", vec!["u1".into(), "u2".into(), "u3".into()]);
        let res = cmd.dispatch(&store);
        let values = &[10.into(), 8.into(), 22.into()];
        assert_res_ok(res, values, &[]);
    }

    #[test]
    fn hgetall_should_work() {
        let store = MemTable::new();

        set_key_pairs(
            "score",
            vec![("u1", 10), ("u2", 8), ("u3", 11), ("u1", 6)],
            &store,
        );

        let cmd = CommandRequest::new_hgetall("score");
        let res = cmd.dispatch(&store);
        let pairs = &[
            Kvpair::new("u1", 6.into()),
            Kvpair::new("u2", 8.into()),
            Kvpair::new("u3", 11.into()),
        ];
        assert_res_ok(res, &[], pairs);
    }

    #[test]
    fn hmset_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("u1", "world")], &store);
        let pairs = vec![
            Kvpair::new("u1", 10.1.into()),
            Kvpair::new("u2", 8.1.into()),
        ];
        let cmd = CommandRequest::new_hmset("t1", pairs);
        let res = cmd.dispatch(&store);
        assert_res_ok(res, &["world".into(), Value::default()], &[]);
    }

    #[test]
    fn hmexist_should_work() {
        let store = MemTable::new();
        set_key_pairs("t1", vec![("u1", "v1"), ("u2", "v2")], &store);

        let cmd = CommandRequest::new_hmexist("t1", vec!["u1".into(), "u3".into()]);
        let res = cmd.dispatch(&store);
        assert_res_ok(res, &[true.into(), false.into()], &[]);
    }

    fn set_key_pairs<T: Into<Value>>(table: &str, pairs: Vec<(&str, T)>, store: &impl Storage) {
        pairs
            .into_iter()
            .map(|(k, v)| CommandRequest::new_hset(table, k, v.into()))
            .for_each(|cmd| {
                cmd.dispatch(store);
            });
    }
}
