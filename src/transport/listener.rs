use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};

pub struct PortRange { 
    pub start: u16,
    pub end: u16,
}

pub struct Listener {
    tcp_listener: TcpListener,
}

impl Listener {
    // Binds to a port.
    pub fn bind(port_range: PortRange) -> io::Result<(Self, SocketAddr)> {
        for port in port_range.start..=port_range.end {
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            match TcpListener::bind(addr) {
                Ok(tcp_listener) => {
                    println!("Listener: binded to {addr}");
                    return Ok((Listener {tcp_listener}, addr ))
                },
                Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
                Err(e) => return Err(e),
            }
        }

        Err(io::Error::new(io::ErrorKind::AddrNotAvailable, "No available address"))
    }

    pub fn incoming(&self) -> Incoming {
        Incoming { listener: self }
    }
}

pub struct Incoming<'a> {
    listener: &'a Listener
}

impl Iterator for Incoming<'_> {
    type Item = io::Result<(TcpStream, SocketAddr)>;

    fn next(&mut self) -> Option<Self::Item>{
        match self.listener.tcp_listener.accept() {
            Ok(v) => return Some(Ok(v)),
            Err(e) => {
                eprintln!("incoming connection failed: {:?}", e);
                return Some(Err(e));
            }
        };
    }
}