use anyhow::Result;
use kv::{KvError, MemTable, ServerStream, Service};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), KvError> {
    tracing_subscriber::fmt::init();
    let service = Service::new(MemTable::new());
    let addr = "127.0.0.1:5000";
    let listener = TcpListener::bind(addr).await?;
    info!("start listening on {}", addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        info!("client {:?} connected", addr);
        let server = ServerStream::new(stream, service.clone());
        tokio::spawn(async move {
            server.process().await?;
            info!("client {:?} disconnected", addr);
            Result::<(), KvError>::Ok(())
        });
    }
}
