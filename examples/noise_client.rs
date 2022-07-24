use anyhow::Result;
use snow::{params::NoiseParams, Builder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "127.0.0.1:5000";

    let params: NoiseParams = "Noise_XX_25519_ChaChaPoly_BLAKE2s".parse()?;
    let builder: Builder<'_> = Builder::new(params);
    let static_key = builder.generate_keypair()?.private;
    let mut noise = builder.local_private_key(&static_key).build_initiator()?;

    let mut stream = TcpStream::connect(addr).await?;
    println!("connected {}", addr);

    let mut buf = vec![0u8; 65535];

    // -> e
    let len = noise.write_message(&[], &mut buf)?;
    send(&mut stream, &buf[..len]).await?;

    // <- e, ee, s, es
    let rec = recv(&mut stream);
    noise.read_message(&rec.await?, &mut buf)?;

    // -> s, se
    let len = noise.write_message(&[], &mut buf)?;
    send(&mut stream, &buf[..len]).await?;

    let mut noise = noise.into_transport_mode()?;
    println!("session established...");

    for _ in 0..10 {
        let len = noise.write_message(b"Hello World", &mut buf)?;
        send(&mut stream, &buf[..len]).await?;
    }

    Ok(())
}

async fn recv(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut msg_len_buf = [0u8; 2];
    stream.read_exact(&mut msg_len_buf).await?;
    let msg_len = ((msg_len_buf[0] as usize) << 8) + (msg_len_buf[1] as usize);
    let mut msg = vec![0u8; msg_len];
    stream.read_exact(&mut msg[..]).await?;
    Ok(msg)
}

async fn send(stream: &mut TcpStream, buf: &[u8]) -> Result<()> {
    let msg_len_buf = [(buf.len() >> 8) as u8, (buf.len() & 0xff) as u8];
    stream.write_all(&msg_len_buf).await?;
    stream.write_all(buf).await?;
    Ok(())
}
