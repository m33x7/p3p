use std::{io, sync::Arc};
use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc::{self, Receiver};
use enum_dispatch::enum_dispatch;
use crate::transport::listener::BindingPortRange;

mod listener;
mod connectionpool;
mod connection;
mod framing;

pub struct TransportMessage {
    pub msg: String,
    pub addr: SocketAddr
}

#[enum_dispatch(TransportTrait)]
pub enum Transport {
    TcpTransport(TcpTransport),
    UdpTransport(UdpTransport),
}

#[enum_dispatch]
pub trait TransportTrait {
    fn send(&self, msg: TransportMessage) -> io::Result<()>;
    fn get_binding_port(&self) -> u16;
}

impl Transport {
    pub fn spawn_tcp() -> io::Result<(Transport, Receiver<TransportMessage>)> {
        let (incoming_tx, incoming_rx) = mpsc::channel();
        
        let connection_factory = connection::ConnectionFactory { dispatcher_tx: incoming_tx };
        let connection_pool = connectionpool::ConnectionPool::new(connection_factory);

        let listener = listener::Listener::tcp_listen(BindingPortRange { start: 8080, end: 8090 }, connection_pool.clone())?;

        Ok((Transport::TcpTransport(TcpTransport { listener, connection_pool }), incoming_rx))
    }

    pub fn spawn_udp() -> io::Result<(Transport, Receiver<TransportMessage>)> {
        let (incoming_tx, incoming_rx) = mpsc::channel();

        let (listener, socket) = listener::Listener::udp_listen(BindingPortRange { start: 4000, end: 4080 }, incoming_tx)?;

        Ok((Transport::UdpTransport(UdpTransport { listener, socket }), incoming_rx))
    }
}

pub struct UdpTransport {
    listener: listener::Listener,
    socket: UdpSocket
}

impl TransportTrait for UdpTransport {
    fn send(&self, msg: TransportMessage) -> io::Result<()> {
        self.socket.send_to(msg.msg.as_bytes(), msg.addr)?;
        Ok(())
    }

    fn get_binding_port(&self) -> u16 {
        self.listener.bind_addr.port()
    }
}

// Transport layer should be able to work with ports/addresses
pub struct TcpTransport {
    connection_pool: Arc<connectionpool::ConnectionPool>,
    listener: listener::Listener
}

impl TransportTrait for TcpTransport {
    fn send(&self, msg: TransportMessage) -> io::Result<()> {
        println!("[TcpTransport] Sending message {:?}", msg.addr);
        let mut pooled_connection = self.connection_pool.get(msg.addr)?;
        pooled_connection.send(&msg.msg)
    }

    fn get_binding_port(&self) -> u16 {
        self.listener.bind_addr.port()
    }
}