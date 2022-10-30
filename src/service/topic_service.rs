use std::sync::Arc;

use tokio_stream::wrappers::ReceiverStream;

use crate::{CommandResponse, Publish, Subscribe, TopicService, Unsubscribe};

impl TopicService for Subscribe {
    fn execute(self, chan: impl super::topic::Topic) -> crate::StreamingResponse {
        let rx = chan.subscribe(self.topic);
        Box::pin(ReceiverStream::new(rx))
    }
}

impl TopicService for Unsubscribe {
    fn execute(self, chan: impl super::topic::Topic) -> crate::StreamingResponse {
        let res = match chan.unsubscribe(self.topic, self.id) {
            Ok(_) => CommandResponse::ok(),
            Err(e) => e.into(),
        };
        res.into()
    }
}

impl TopicService for Publish {
    fn execute(self, chan: impl super::topic::Topic) -> crate::StreamingResponse {
        chan.publish(self.topic, Arc::new(self.data.into()));
        CommandResponse::ok().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        assert_res_ref_error, assert_res_ref_ok,
        topic::{PubSub, Topic},
        CommandRequest, StreamingResponse,
    };
    use futures::StreamExt;
    use std::{convert::TryInto, time::Duration};
    use tokio::time;

    #[tokio::test]
    async fn dispatch_publish_should_work() {
        let topic = Arc::new(PubSub::default());
        let cmd = CommandRequest::new_publish("cae", vec!["hello".into()]);
        let mut res = cmd.dispatch_steaming(topic);
        let data = res.next().await.unwrap();
        assert_res_ref_ok(&data, &[], &[]);
    }

    #[tokio::test]
    async fn dispatch_subscribe_should_work() {
        let topic = Arc::new(PubSub::default());
        let cmd = CommandRequest::new_subscribe("cae");
        let mut res = cmd.dispatch_steaming(topic);
        let id = get_id(&mut res).await;
        assert!(id > 0);
    }

    #[tokio::test]
    async fn dispatch_subscribe_abnormal_quit_should_be_removed_on_next_publish() {
        let topic = Arc::new(PubSub::default());
        let id = {
            let cmd = CommandRequest::new_subscribe("cae");
            let mut res = cmd.dispatch_steaming(topic.clone());
            let id = get_id(&mut res).await;
            drop(res);
            id as u32
        };

        // this subscription shoud be deletd since it is invalid
        let cmd = CommandRequest::new_publish("cae", vec!["hello".into()]);
        cmd.dispatch_steaming(topic.clone());
        time::sleep(Duration::from_millis(10)).await;

        // try to delete again, should return KvError
        let result = topic.unsubscribe("cae".into(), id);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn dispatch_unsubscribe_should_work() {
        let topic = Arc::new(PubSub::default());
        let cmd = CommandRequest::new_subscribe("cae");
        let mut res = cmd.dispatch_steaming(topic.clone());
        let id = get_id(&mut res).await;

        let cmd = CommandRequest::new_unsubscribe("cae", id as _);
        let mut res = cmd.dispatch_steaming(topic);
        let data = res.next().await.unwrap();

        assert_res_ref_ok(&data, &[], &[]);
    }

    #[tokio::test]
    async fn dispatch_unsubscribe_non_existed_id_should_error() {
        let topic = Arc::new(PubSub::default());

        let cmd = CommandRequest::new_unsubscribe("cae", 114514);
        let mut res = cmd.dispatch_steaming(topic);
        let data = res.next().await.unwrap();

        assert_res_ref_error(&data, 404, "subscription 114514");
    }

    pub async fn get_id(res: &mut StreamingResponse) -> u32 {
        let id: i64 = res.next().await.unwrap().as_ref().try_into().unwrap();
        id as u32
    }
}
