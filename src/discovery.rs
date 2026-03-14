use std::io;
use std::net::{TcpListener, TcpStream};

pub fn establish_connection() -> Result<TcpStream, io::Error> {
    let listener = TcpListener::bind("127.0.0.1:8080");

    match listener {
        Ok(listener) => {
            println!("Server mode. Waiting for incoming connections.");
            for s in listener.incoming() {
                // Return first connected
                println!("Incoming connection.");
                return s;
            }
            return Err(io::Error::new(io::ErrorKind::Other, "Shouldn't happen"));
        }
        Err(error) => {
            println!("Error when binding to 8080 socket. {error:?}. Will try to connect to server instead.");
            return TcpStream::connect("127.0.0.1:8080")
        }
    }
}
