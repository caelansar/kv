use anyhow::Result;
use kv::{ClientStream, CommandRequest, KvError, TlsClient, YamuxCtrl};
use std::time::Duration;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time,
};
use tokio_stream::StreamExt;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:5000";
    let stream = TcpStream::connect(addr).await?;

    let ca_cert = include_str!("../certs/ca.crt");
    let client_cert = include_str!("../certs/client.crt");
    let client_key = include_str!("../certs/client.key");

    let connector = TlsClient::new(
        "kv.test.com",
        Some((client_cert, client_key)),
        Some(ca_cert),
    )?;
    let stream = connector.connect(stream).await?;

    let mut ctrl = YamuxCtrl::new_client(stream, None);
    let stream = ctrl.open_stream().await?;
    let mut client = ClientStream::new(stream);

    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());
    info!("client send cmd {:?}", cmd);
    let resp = client.execute(&cmd).await?;
    info!("client get resp {:?}", resp);

    let cmd = CommandRequest::new_hget("table1", "hello");
    info!("client send cmd {:?}", cmd);
    let resp = client.execute(&cmd).await?;
    info!("client get resp {:?}", resp);

    let cmd = CommandRequest::new_hget("table1", "hello1");
    info!("client send cmd {:?}", cmd);
    let resp = client.execute(&cmd).await?;
    info!("client get resp {:?}", resp);

    let cmd = CommandRequest::new_subscribe("async");
    let mut stream = client.execute_streaming(&cmd).await?;

    start_publish(ClientStream::new(ctrl.open_stream().await?), "async")?;

    let id = stream.id;
    start_unsubscribe(ClientStream::new(ctrl.open_stream().await?), "async", id)?;

    while let Some(Ok(data)) = stream.next().await {
        println!("Got published data: {:?}", data);
    }

    Ok(())
}

fn start_publish<S>(mut stream: ClientStream<S>, name: &str) -> Result<(), KvError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let cmd = CommandRequest::new_publish(name, vec!["hello".into(), "world".into()]);
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(1000)).await;
        let res = stream.execute(&cmd).await.unwrap();
        println!("Finished publishing: {:?}", res);
    });

    Ok(())
}

fn start_unsubscribe<S>(mut stream: ClientStream<S>, name: &str, id: u32) -> Result<(), KvError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let cmd = CommandRequest::new_unsubscribe(name, id);
    tokio::spawn(async move {
        time::sleep(Duration::from_millis(2000)).await;
        let res = stream.execute(&cmd).await.unwrap();
        println!("Finished unsubscribing: {:?}", res);
    });

    Ok(())
}
