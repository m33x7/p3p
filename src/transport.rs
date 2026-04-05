
use std::hash::Hash;
use std::io;
use std::net::{TcpListener, SocketAddr, TcpStream};
use std::collections::{HashMap};
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::mpsc::{self, TryRecvError};
use std::sync::mpsc::{Sender, Receiver};
use std::thread::{self, JoinHandle};

mod listener;
use listener::{Listener, PortRange};
    
mod connection;
use connection::{TcpConnection};

mod framing;
use framing::{LengthPrefixFraming};

mod connectionpool;
use connectionpool::{ConnectionPool};

use crate::transport::connection::ConnectionFactory;

pub struct Transport {
    pub connection_pool: Arc<ConnectionPool>,
    pub listener: Listener
}

pub struct TransportMessage {
    pub msg: String,
    pub addr: SocketAddr
}

impl Transport {

    // TODO - change it to receive trait Framing instead of stream.
    pub fn spawn() -> io::Result<(Self, Receiver<TransportMessage>)> {
        let (incoming_tx, incoming_rx) = mpsc::channel();
        
        let connection_factory = ConnectionFactory { dispatcher_tx: incoming_tx };
        let connection_pool = ConnectionPool::new(connection_factory);

        let listener = Listener::listen(PortRange { start: 8080, end: 8090 }, connection_pool.clone())?;

        Ok((Transport { listener, connection_pool }, incoming_rx))
    }

    pub fn send(&self, msg: TransportMessage) -> io::Result<()> {

        println!("[Transport] Sending message {:?}", msg.addr);
        let mut pooled_connection = self.connection_pool.get(msg.addr)?;
        pooled_connection.send(&msg.msg);

        Ok(())
    }
}
