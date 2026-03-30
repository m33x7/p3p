
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

pub struct Transport {
    connections: Arc<Mutex<HashMap<SocketAddr, (Sender<String>, Receiver<String>)>>>,
    bind_addr: Option<SocketAddr>
}

pub struct TransportMessage {
    pub msg: String,
    pub addr: SocketAddr
}

impl Transport {
    pub fn new() -> Transport {
        let connections : Arc<Mutex<HashMap<SocketAddr, (Sender<String>, Receiver<String>)>>> = Arc::new(Mutex::new(HashMap::new()));
        Transport { connections, bind_addr: None }
    }

    // TODO - change it to receive trait Framing instead of stream.
    pub fn spawn(&mut self) -> io::Result<(Sender<TransportMessage>, Receiver<TransportMessage>)> {
        let (listener, bind_addr) = Listener::bind(PortRange { start: 8080, end: 8090 })?;
        self.bind_addr = Some(bind_addr);

        let connections = self.connections.clone();
        thread::spawn(move || // TODO - threads should join
        {
            // TODO - change to ".and_then" :
            for stream in listener.incoming() {

                println!("Incoming connection");

                match stream {
                    Ok((stream, addr)) => match connection::TcpConnection::new(stream){ 
                        Ok(connection) => {
                            let connection = connection.spawn();
                            let mut connections = connections.lock().unwrap();
                            connections.insert(addr, connection);
                        }
                        Err(e) => eprintln!("Creating a connection failed {e}"),
                    },
                    Err(e) => eprintln!("Creating a connection failed {e}"),
                }
            }
        });

        let (incoming_tx, incoming_rx): (Sender<TransportMessage>, Receiver<TransportMessage>) = mpsc::channel();
        let (outgoing_tx, outgoing_rx): (Sender<TransportMessage>, Receiver<TransportMessage>) = mpsc::channel();

        let connections = self.connections.clone();
        thread::spawn(move || { // TODO - threads should join
            loop {
                let connections = connections.lock().unwrap();
                for(&addr, (tx, rx)) in connections.iter(){
                    match rx.try_recv() {
                        Ok(msg) => { incoming_tx.send(TransportMessage { addr, msg }).unwrap() }, // TODO - handle error here on on top level.
                        Err(TryRecvError::Empty) => {},
                        Err(TryRecvError::Disconnected) => {} // TODO - handle error here - remove the connection from the pool
                    };
                }

                match outgoing_rx.try_recv() {
                    Ok(msg) => {
                        Self::send(connections, msg).unwrap(); // TODO - handle error here - remove  the connection from the pool
                    },
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {}
                }
            }
        });

        Ok((outgoing_tx, incoming_rx))
    }

    fn send(mut connections: MutexGuard<'_, HashMap<SocketAddr, (Sender<String>, Receiver<String>)>>, msg: TransportMessage) -> io::Result<()> {
        let TransportMessage { msg, addr } = msg;

        println!("Transport: sending message. {:?}", msg);

        if !connections.contains_key(&addr){
            let stream = TcpStream::connect(&addr)?;
            let connection = TcpConnection::new(stream)?;
            connections.insert(addr, connection.spawn());
        }

        let (tx, _rx) = connections.get(&addr).unwrap();
        tx.send(msg).unwrap(); // TODO - handle error here

        Ok(())
    }
}
