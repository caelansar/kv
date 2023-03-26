use anyhow::{Error, Result};
use kv::{MemTable, ServerStream, Service, TlsServer, YamuxCtrl};
use s2n_quic::Server;
use s2n_quic_rustls::server::Builder;
use std::{future::Future, str::FromStr};
use tokio::{net::TcpListener, signal};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::{error, info, span};
use tracing_subscriber::{prelude::*, EnvFilter};

#[tokio::main]
async fn main() {
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("kv-server")
        .install_simple()
        .unwrap();

    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    let root = span!(tracing::Level::INFO, "server_start");
    let _ = root.enter();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().compact())
        .with(opentelemetry.with_filter(EnvFilter::from_str("debug").unwrap()))
        .init();

    run(signal::ctrl_c()).await
}

async fn run_quic_server() -> Result<(), Error> {
    let service = Service::new(MemTable::new());
    let addr = "127.0.0.1:5000";

    let server_cert = include_str!("../certs/server.crt");
    let server_key = include_str!("../certs/server.key");

    let config = Builder::new()
        .with_certificate(server_cert, server_key)?
        .build()?;

    let mut listener = Server::builder()
        .with_tls(config)?
        .with_io(addr)?
        .start()
        .unwrap();

    info!("start listening on {}", addr);
    loop {
        if let Some(mut conn) = listener.accept().await {
            let remote = conn.remote_addr();
            let svc = service.clone();

            tokio::spawn(async move {
                while let Ok(Some(stream)) = conn.accept_bidirectional_stream().await {
                    info!("client {:?} connected", addr);
                    let svc1 = svc.clone();
                    tokio::spawn(async move {
                        let stream = ServerStream::new(stream, svc1.clone());
                        let _ = stream.process().await;
                        info!("client {:?} disconnected", remote);
                    });
                }
                Ok::<(), Error>(())
            });
        }
    }
}

async fn run_tcp_server() -> Result<(), Error> {
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
        res = run_quic_server() => {
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }
}
