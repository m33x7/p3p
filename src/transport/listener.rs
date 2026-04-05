use std::sync::Arc;
use std::thread::JoinHandle;
use std::{io, thread};
use std::net::{SocketAddr, TcpListener};

use crate::transport::connectionpool::ConnectionPool;

pub struct PortRange { 
    pub start: u16,
    pub end: u16,
}

pub struct Listener {
    pub bind_addr: SocketAddr,
    pub listener_thread: JoinHandle<()>
}

impl Listener {
    // Binds to a port.
    pub fn listen(port_range: PortRange, pool: Arc<ConnectionPool>) -> io::Result<Self> {
        for port in port_range.start..=port_range.end {
            let bind_addr = SocketAddr::from(([127, 0, 0, 1], port));
            match TcpListener::bind(bind_addr) {
                Ok(tcp_listener) => {
                    println!("[Listener] Binded to {bind_addr}");

                    let listener_thread = thread::spawn(move || 
                        match tcp_listener.accept() {
                            Ok((stream, addr)) => {
                                pool.replace(addr, stream);
                                println!("[Listener] Incoming connection succeeded: {:?}", addr);
                            }
                            Err(e) => {
                                eprintln!("[Listener] Incoming connection failed: {:?}", e);
                            }
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
}