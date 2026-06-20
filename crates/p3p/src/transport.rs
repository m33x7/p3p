use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use std::{io, net::SocketAddr, sync::Arc};
use std::io::Result;

struct BindingPortRange {
    start: u16,
    end: u16
}

struct UdpTransport {
    socket: Arc<UdpSocket>,
    incoming_rx: mpsc::Receiver<TransportMessage>,
    listening_task: JoinHandle<()> // No need to have Result here.
}

struct TransportMessage { 
    msg: String,
    addr: SocketAddr, // it's from/to address in case we receive/send
}

impl UdpTransport {
    pub async fn spawn(port_range: BindingPortRange) -> Result<UdpTransport> {
        for port in port_range.start..=port_range.end {
            let (incoming_tx, incoming_rx) = mpsc::channel::<TransportMessage>(1_000);
            
            let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
            let socket= UdpSocket::bind(bind_addr).await;
            match socket {
                Ok(socket) => {
                    let socket = Arc::new(socket);

                    // spawn listener task
                    let listening_socket = socket.clone();
                    let listening_task = tokio::spawn(async move {
                        let mut buf = [0; 1024];
                        loop {
                            let incoming = listening_socket.recv_from(&mut buf).await;
                            match incoming {
                                Ok((len, addr)) => {
                                    let msg = String::from_utf8_lossy(&buf[..len]);
                                    incoming_tx.send(TransportMessage { msg: msg.to_string(), addr }).await.unwrap(); // panic if the channel is broken.
                                }
                                Err(e) => eprintln!("Error on incoming : {e}")
                            }
                        };
                    });

                    return Ok(UdpTransport { socket, incoming_rx, listening_task });
                },
                Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
                Err(e) => return Err(e),
            }
        }

        Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "[Listener] No available address"))
    }

    pub async fn send(&self, msg: TransportMessage) -> Result<usize> {
        self.socket.send_to(msg.msg.as_bytes(), msg.addr).await
    }
}
