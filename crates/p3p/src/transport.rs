use tokio::net::UdpSocket;
use std::{io, net::SocketAddr, sync::Arc};
use std::io::Result;
use async_trait::async_trait;

pub struct BindingPortRange {
    pub start: u16,
    pub end: u16
}

#[async_trait]
pub trait Transport {
    async fn send(&self, msg: TransportMessage) -> Result<usize>;
    async fn receive(&self, buf: &mut [u8; 1024]) -> Result<TransportMessage>;
}

pub struct UdpTransport {
    pub socket: Arc<UdpSocket>, // We listen and send from the same socket.
}

pub struct TransportMessage { 
    pub msg: String,
    pub addr: SocketAddr, // it's from/to address in case we receive/send
}

impl UdpTransport {
    pub async fn bind(port_range: BindingPortRange) -> Result<UdpTransport> {
        for port in port_range.start..=port_range.end {
            let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
            match UdpSocket::bind(bind_addr).await {
                Ok(socket) => {
                    return Ok(UdpTransport { socket: Arc::new(socket) });
                },
                Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
                Err(e) => return Err(e),
            }
        }

        Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "[Listener] No available address"))
    }
}

#[async_trait]
impl Transport for UdpTransport {
    async fn send(&self, msg: TransportMessage) -> Result<usize> {
        self.socket.send_to(msg.msg.as_bytes(), msg.addr).await
    }

    async fn receive(&self, buf: &mut [u8; 1024]) -> Result<TransportMessage> {
        let incoming = self.socket.recv_from(buf).await;
        match incoming {
            Ok((len, addr))  => {
                let msg = String::from_utf8_lossy(&buf[..len]);
                Ok(TransportMessage { msg: msg.to_string(), addr })
            },
            Err(e) => Err(e)
        }
    }
}