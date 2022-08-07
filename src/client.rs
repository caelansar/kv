use anyhow::Result;
use kv::{ClientStream, CommandRequest, YamuxCtrl};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:5000";
    let stream = TcpStream::connect(addr).await?;

    let mut ctrl = YamuxCtrl::new_client(stream, None);
    let stream = ctrl.open_stream().await?;
    let mut client = ClientStream::new(stream);

    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());
    info!("client send cmd {:?}", cmd);
    let resp = client.execute(cmd).await?;
    info!("client get resp {:?}", resp);

    let cmd = CommandRequest::new_hget("table1", "hello");
    info!("client send cmd {:?}", cmd);
    let resp = client.execute(cmd).await?;
    info!("client get resp {:?}", resp);

    let cmd = CommandRequest::new_hget("table1", "hello1");
    info!("client send cmd {:?}", cmd);
    let resp = client.execute(cmd).await?;
    info!("client get resp {:?}", resp);

    Ok(())
}
