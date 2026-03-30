use std::collections::{HashMap};
use std::f32::consts::E;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, TryRecvError};
use std::sync::mpsc::{Sender, Receiver};
use std::thread::{self, JoinHandle};
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use crate::transport::framing::{Framing, LengthPrefixFraming};

pub struct TcpConnection {
    framing: LengthPrefixFraming
}

// TODO - add ability for the connection to signal that it needs to be removed from the connection pool.
// TODO - add ability to close the connection - via "Drop" trait.
// TODO - handle panic inside a thread.
impl TcpConnection {
    pub fn new(stream: TcpStream) -> io::Result<TcpConnection> {
        let framing = LengthPrefixFraming::new(stream)?;
        Ok(TcpConnection { framing })
    }

    pub fn spawn(mut self) -> (Sender<String>, Receiver<String>) {
        let (incoming_tx, incoming_rx): (Sender<String>, Receiver<String>) = mpsc::channel();
        let (outgoing_tx, outgoing_rx): (Sender<String>, Receiver<String>) = mpsc::channel();

        // Start listening to incoming messages.
        thread::spawn(move || {
            loop {
                match self.framing.read_msg() {
                    Ok(Some(msg)) => incoming_tx.send(msg).unwrap(),
                    Ok(None) => {},
                    Err(e) => self.handle_error(e),
                };

                match outgoing_rx.try_recv() {
                    Ok(msg) => {
                        match self.framing.write_msg(&msg) {
                            Ok(()) => {},
                            Err(e) => self.handle_error(e),
                        }
                    },
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => {}
                };

                thread::sleep(Duration::from_millis(250));
            }
        });

        (outgoing_tx, incoming_rx)
    }

    fn handle_error(&self, e: io::Error) -> (){
        eprintln!("{:?}", e);
    }
}