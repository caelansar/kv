use anyhow::Result;
use kv::{KvError, MemTable, ServerStream, Service, YamuxCtrl};
use tokio::net::TcpListener;
use tokio_util::compat::FuturesAsyncReadCompatExt;
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

        let svc = service.clone();
        tokio::spawn(async move {
            YamuxCtrl::new_server(stream, None, move |stream| {
                let svc1 = svc.clone();
                async move {
                    let server = ServerStream::new(stream.compat(), svc1.clone());
                    server.process().await.unwrap();
                    info!("client {:?} disconnected", addr);
                    Ok(())
                }
            });
        });
    }
}
