use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Sender};
use std::thread::{self, JoinHandle};
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use crate::transport::TransportMessage;
use crate::transport::connectionpool::ConnectionPool;
use crate::transport::framing::{LengthPrefixFraming, Framing};

pub struct TcpConnection {
    pub addr: SocketAddr,
    working_thread: JoinHandle<()>,
    cancelled: Arc<AtomicBool>,
    send_framing: LengthPrefixFraming
}

impl TcpConnection {
    fn spawn(framing: LengthPrefixFraming, incoming_messages: Sender<TransportMessage>, addr: SocketAddr, pool: Arc<ConnectionPool>) -> io::Result<TcpConnection> {
        // Used to cancel the connection
        let cancelled = Arc::new(AtomicBool::new(false));

        // We separate read and write operations over the same TcpStream
        let (listen_framing, send_framing) = framing.into_read_write()?;

        // Start working thread
        let working_thread = thread::spawn({
            let cancelled = cancelled.clone();
            move || {
                let e = Self::listen_loop(addr, incoming_messages, listen_framing, cancelled);
                eprintln!("{:?}", e);
                pool.remove(&addr);
            }
        });

        Ok(TcpConnection { addr, working_thread, cancelled, send_framing })
    }

    pub fn cancel(self) {
        // TODO - add TcpStream shutdown.
        println!("Connection cancellation requested.");
        self.cancelled.store(true, Ordering::Relaxed);
        self.working_thread.join(); // TODO - use the result.
    }

    pub fn send(&mut self, msg: &str) -> io::Result<()> {
        // TODO - if message write failed - remove the connection from the Pool.
        self.send_framing.write_msg(msg)
    }

    fn listen_loop(addr: SocketAddr, incoming_messages: Sender<TransportMessage>, mut incoming_framing: LengthPrefixFraming, cancelled: Arc<AtomicBool>) -> io::Result<()>{
        while !cancelled.load(Ordering::Relaxed) {

            // Listen to incoming messages
            match incoming_framing.read_msg() {
                Ok(Some(msg)) => incoming_messages.send(TransportMessage { msg, addr }).unwrap(),
                Ok(None) => {},
                Err(e) => return Err(e), // Error when reading message to TcpStream
            };

            thread::sleep(Duration::from_millis(250));
        }

        Ok(())
    }
}


// TODO - connection settings can be stored here. For example, framing selection, connection timeout.
pub struct ConnectionFactory {
    pub dispatcher_tx: Sender<TransportMessage>
}

impl ConnectionFactory {
    pub fn new_connection_with_stream(&self, stream: TcpStream, pool: Arc<ConnectionPool>, addr: SocketAddr) -> io::Result<TcpConnection>{
        let framing = LengthPrefixFraming { stream };
        TcpConnection::spawn(framing, self.dispatcher_tx.clone(), addr, pool)
    }
}

impl Clone for ConnectionFactory {
    fn clone(&self) -> Self {
        Self { dispatcher_tx: self.dispatcher_tx.clone() }
    }
}