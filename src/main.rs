use std::net::SocketAddr;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::mpsc::TryRecvError;
use std::thread;
use std::io;
use std::time::Duration;

mod discovery;
mod transport;

use crate::transport::{Transport, TransportMessage};

fn main() -> std::io::Result<()> {
    let mut transport = Transport::new();
    let (tx, rx) = transport.spawn()?;
    
    let read_thread = thread::spawn(move || output_message(rx));
    let write_thread = thread::spawn(move || write_message(tx));

    read_thread.join().expect("reader thread panicked")?;
    write_thread.join().expect("writer thread panicked")?;
    
    Ok(())
}

fn write_message(tx: Sender<TransportMessage>) -> io::Result<()>{
    loop {
        let stdin = io::stdin();
        for line in stdin.lines() {
            let line = line.unwrap();
            if let Some((addr_str, msg)) = line.split_once(' ') {
                if let Ok(addr) = addr_str.parse::<SocketAddr>() {
                    tx.send(TransportMessage { msg: msg.to_string(), addr }); // TODO - handle error
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
