use anyhow::Result;
use kv::{ClientStream, CommandRequest};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:5000";
    let stream = TcpStream::connect(addr).await?;

    let mut client = ClientStream::new(stream);

    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());
    info!("client send cmd {:?}", cmd);
    client.execute(cmd).await?;

    let cmd = CommandRequest::new_hget("table1", "hello");
    info!("client send cmd {:?}", cmd);
    client.execute(cmd).await?;

    let cmd = CommandRequest::new_hget("table1", "hello1");
    info!("client send cmd {:?}", cmd);
    client.execute(cmd).await?;

    Ok(())
}
