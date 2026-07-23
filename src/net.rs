use std::{net::SocketAddr, time::Duration};

use tokio::{io, net, time};

pub async fn connect_peer(peer: &SocketAddr) -> Result<net::TcpStream, io::Error> {
    loop {
        match net::TcpStream::connect(peer).await {
            Ok(s) => break Ok(s),
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                // TODO: Proper backoff
                time::sleep(Duration::from_millis(250)).await
            }
            Err(e) => return Err(e),
        }
    }
}
