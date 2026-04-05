use std::net::SocketAddr;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::TryRecvError;
use std::thread;
use std::io;
use std::time::Duration;

mod discovery;
mod transport;

use crate::transport::{Transport, TransportMessage};

fn main() -> std::io::Result<()> {
    let (transport, incoming_messages) = Transport::spawn()?;
    
    let read_thread = thread::spawn(move || output_message(incoming_messages));
    let write_thread = thread::spawn(move || write_message(transport));

    read_thread.join().expect("reader thread panicked")?;
    write_thread.join().expect("writer thread panicked")?;
    
    Ok(())
}

fn write_message(transport: Transport) -> io::Result<()>{
    loop {
        let stdin = io::stdin();
        for line in stdin.lines() {
            let line = line.unwrap();
            if let Some((addr_str, msg)) = line.split_once(' ') {
                if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                    transport.send(TransportMessage { msg: msg.to_string(), addr }); // TODO - handle error
                } else {
                    eprintln!("invalid address format: {}", addr_str);
                }
            } else {
                eprintln!("invalid format, use '127.0.0.1:8080'");
            }
        }

        thread::sleep(Duration::from_millis(300));
    }
}

fn output_message(rx: Receiver<TransportMessage>) -> io::Result<()>{
    loop {
        match rx.try_recv() {
            Ok(msg) => println!(">>> {} {}", msg.addr, msg.msg),
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {}
        }
        
        thread::sleep(Duration::from_millis(250));
    }
}
