use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::thread::JoinHandle;
use std::{io, thread};
use std::net::{SocketAddr, TcpListener, UdpSocket};

use crate::transport::{TransportMessage};
use crate::transport::connectionpool::ConnectionPool;

pub struct BindingPortRange { 
    pub start: u16,
    pub end: u16,
}

pub struct Listener {
    pub bind_addr: SocketAddr,
    pub listener_thread: JoinHandle<()>
}

// TODO - try to unify it.
impl Listener {
    // Binds to a port.
    pub fn tcp_listen(port_range: BindingPortRange, pool: Arc<ConnectionPool>) -> io::Result<Self> {
        for port in port_range.start..=port_range.end {
            let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
            match TcpListener::bind(bind_addr) {
                Ok(tcp_listener) => {
                    println!("[Listener] Binded to {bind_addr}");

                    let listener_thread = thread::spawn(move || 
                        match tcp_listener
                            .accept()
                            .and_then(|(stream,addr)| pool.replace(addr, stream)) {
                                Ok(()) => println!("[Listener] Incoming connection succeeded."),
                                Err(e) => eprintln!("[Listener] Incoming connection failed: {:?}", e)
                        }
                    );

                    return Ok(Listener { bind_addr, listener_thread });
                },
                Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
                Err(e) => return Err(e),
            }
        }

        Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "[Listener] No available address"))
    }

    pub fn udp_listen(port_range: BindingPortRange, incoming_tx: Sender<TransportMessage>) -> io::Result<(Self, UdpSocket)> {
        for port in port_range.start..=port_range.end {
            let bind_addr = SocketAddr::from(([0, 0, 0, 0], port));
            match UdpSocket::bind(bind_addr) {
                Ok(socket) => {
                    println!("[Listener] Binded to {bind_addr}");

                    // Spawn a thread to receive messages
                    let recv_socket = socket.try_clone()?;
                    let listener_thread = std::thread::spawn(move || {
                        let mut buf = [0u8; 1500];
                        loop {
                            match recv_socket.recv_from(&mut buf) {
                                Ok((len, addr)) => {
                                    let msg = String::from_utf8_lossy(&buf[..len]);
                                    incoming_tx.send(TransportMessage { msg: msg.to_string(), addr }).unwrap();
                                }
                                Err(e) => eprintln!("Recv error: {}", e),
                            }
                        }
                    });

                    return Ok((Listener { bind_addr, listener_thread }, socket));
                },
                Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
                Err(e) => return Err(e),
            }
        }

        Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "[Listener] No available address"))
    }
}