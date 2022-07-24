use anyhow::Result;
use snow::{params::NoiseParams, Builder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

static SECRET: &[u8] = b"keykeykeykeykeykeykeykeykeykeyke";

#[tokio::main]
async fn main() -> Result<()> {
    let mut buf = vec![0u8; 65535];

    let params: NoiseParams = "Noise_XX_25519_ChaChaPoly_BLAKE2s".parse()?;
    // initialize our responder using a builder
    let builder: Builder<'_> = Builder::new(params);
    let static_key = builder.generate_keypair()?.private;
    let mut noise = builder
        .local_private_key(&static_key)
        .psk(3, SECRET)
        .build_responder()?;

    // wait on client's arrival
    println!("listening on 127.0.0.1:5000");
    let (mut stream, _) = TcpListener::bind("127.0.0.1:5000").await?.accept().await?;

    // <- e
    noise.read_message(&recv(&mut stream).await?, &mut buf)?;

    // -> e, ee, s, es
    let len = noise.write_message(&[0u8; 0], &mut buf)?;
    send(&mut stream, &buf[..len]).await?;

    // <- s, se
    noise.read_message(&recv(&mut stream).await?, &mut buf)?;

    let mut noise = noise.into_transport_mode()?;

    while let Ok(msg) = recv(&mut stream).await {
        let len = noise.read_message(&msg, &mut buf)?;
        println!("client said: {}", String::from_utf8_lossy(&buf[..len]));
    }
    println!("connection closed.");
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
