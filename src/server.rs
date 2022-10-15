use anyhow::Result;
use kv::{KvError, MemTable, ServerStream, Service, TlsServer, YamuxCtrl};
use std::future::Future;
use tokio::{net::TcpListener, signal};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    run(signal::ctrl_c()).await
}

async fn run_server() -> Result<(), KvError> {
    let service = Service::new(MemTable::new());
    let addr = "127.0.0.1:5000";

    let server_cert = include_str!("../certs/server.crt");
    let server_key = include_str!("../certs/server.key");
    let client_ca = include_str!("../certs/ca.crt");
    let acceptor = TlsServer::new(server_cert, server_key, Some(client_ca))?;

    let listener = TcpListener::bind(addr).await?;
    info!("start listening on {}", addr);
    loop {
        let (stream, addr) = listener.accept().await?;
        info!("client {:?} connected", addr);

        let tls = acceptor.clone();
        let svc = service.clone();
        tokio::spawn(async move {
            let stream = tls.accept(stream).await.unwrap();
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

async fn run(shutdown: impl Future) {
    tokio::select! {
        res = run_server() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }
}
